import { describe, expect, it } from "vitest";
import {
    Client,
    Server,
    createClientSession,
    createServerSession,
    encodeFrame,
    FrameFlag,
    FrameParser,
    FrameType,
    YAMUX_VERSION,
} from "../../src";
import { onceEvent, createDuplexPair } from "../helpers/memory-duplex";

describe("session", () => {
    it("opens a stream and sends data", async () => {
        const { a, b } = createDuplexPair();
        const client = createClientSession(a);
        const server = createServerSession(b);

        const inboundPromise = onceEvent<any>(server, "stream");
        const outbound = client.openStream();
        outbound.write(Buffer.from("hello yamux"));

        const inbound = await inboundPromise;
        const data = await onceEvent<Buffer>(inbound, "data");
        expect(data.toString()).toBe("hello yamux");

        outbound.end();
        await onceEvent(inbound, "end");

        client.close();
        server.close();
    });

    it("supports ping roundtrip", async () => {
        const { a, b } = createDuplexPair();
        const client = createClientSession(a);
        const server = createServerSession(b);

        const result = await client.ping();
        expect(result.nonce).toBeGreaterThan(0);
        expect(result.rttMs).toBeGreaterThanOrEqual(0);

        client.close();
        server.close();
    });

    it("blocks new stream after goaway", async () => {
        const { a, b } = createDuplexPair();
        const client = createClientSession(a);
        const server = createServerSession(b);

        server.goAway();
        await onceEvent(server, "close", 50).catch(() => undefined);

        await new Promise((resolve) => setTimeout(resolve, 10));
        expect(() => client.openStream()).toThrow();

        client.close();
        server.close();
    });

    it("tolerates a frame for an unknown stream and replies RST instead of tearing down the session", async () => {
        // Regression: a late/duplicate frame for an already-closed (unknown) stream must NOT
        // throw a fatal protocol error that destroys the whole session. This previously broke
        // any yamux-server (e.g. one stream per inbound connection) on the 2nd connection.
        const { a, b } = createDuplexPair();
        const server = createServerSession(b);

        let fatal: Error | undefined;
        let closed = false;
        server.on("error", (error) => {
            fatal = error;
        });
        server.on("close", () => {
            closed = true;
        });

        // Capture frames the server writes back.
        const parser = new FrameParser();
        const received: ReturnType<FrameParser["feed"]> = [];
        a.on("data", (chunk: Buffer) => {
            received.push(...parser.feed(chunk));
        });

        // Inject a WindowUpdate (no SYN) for a stream the server has never seen.
        const unknownStreamId = 7;
        a.write(
            encodeFrame({
                header: {
                    version: YAMUX_VERSION,
                    type: FrameType.WindowUpdate,
                    flags: 0,
                    streamId: unknownStreamId,
                    length: 0,
                },
                payload: Buffer.alloc(0),
            }),
        );

        await new Promise((resolve) => setTimeout(resolve, 20));

        expect(fatal).toBeUndefined();
        expect(closed).toBe(false);

        const rst = received.find(
            (frame) =>
                frame.header.streamId === unknownStreamId &&
                (frame.header.flags & FrameFlag.RST) !== 0,
        );
        expect(rst, "server should reply RST for the unknown stream").toBeDefined();

        // Session is still usable afterwards.
        const inboundPromise = onceEvent<any>(server, "stream");
        const client = createClientSession(a);
        const outbound = client.openStream();
        outbound.write(Buffer.from("still alive"));
        const inbound = await inboundPromise;
        const data = await onceEvent<Buffer>(inbound, "data");
        expect(data.toString()).toBe("still alive");

        client.close();
        server.close();
    });

    it("silently ignores a FIN/RST for an unknown stream (no RST storm, no teardown)", async () => {
        const { a, b } = createDuplexPair();
        const server = createServerSession(b);

        let closed = false;
        server.on("error", () => undefined);
        server.on("close", () => {
            closed = true;
        });

        const parser = new FrameParser();
        const received: ReturnType<FrameParser["feed"]> = [];
        a.on("data", (chunk: Buffer) => {
            received.push(...parser.feed(chunk));
        });

        a.write(
            encodeFrame({
                header: {
                    version: YAMUX_VERSION,
                    type: FrameType.Data,
                    flags: FrameFlag.FIN,
                    streamId: 9,
                    length: 0,
                },
                payload: Buffer.alloc(0),
            }),
        );

        await new Promise((resolve) => setTimeout(resolve, 20));

        expect(closed).toBe(false);
        // Must NOT reply RST to a teardown frame (avoids an RST loop between peers).
        expect(received.find((frame) => (frame.header.flags & FrameFlag.RST) !== 0)).toBeUndefined();

        server.close();
    });

    it("supports Go-style Client/Server constructors", async () => {
        const { a, b } = createDuplexPair();
        const client = Client(a);
        const server = Server(b);

        const inboundPromise = onceEvent<any>(server, "stream");
        const outbound = client.openStream();
        outbound.write(Buffer.from("go-style"));

        const inbound = await inboundPromise;
        const data = await onceEvent<Buffer>(inbound, "data");
        expect(data.toString()).toBe("go-style");

        outbound.end();
        await onceEvent(inbound, "end");

        client.close();
        server.close();
    });
});
