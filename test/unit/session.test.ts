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

    it("silently ignores frames for an unknown/closed stream without tearing down the session", async () => {
        // Regression: a late/crossing frame for an already-closed (unknown) stream must NOT
        // throw a fatal protocol error that destroys the whole session. This previously broke
        // any yamux-server (e.g. one stream per inbound connection) on the 2nd connection.
        //
        // It must also NOT reply RST: after a clean both-FIN close the peer's read side may still
        // be draining, and an RST would reset that still-valid half-open stream.
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

        // Inject various non-SYN frames for streams the server has never seen.
        const unknownFrames = [
            { type: FrameType.WindowUpdate, flags: 0, streamId: 7 },
            { type: FrameType.Data, flags: FrameFlag.FIN, streamId: 9 },
            { type: FrameType.WindowUpdate, flags: FrameFlag.RST, streamId: 11 },
        ];
        for (const f of unknownFrames) {
            a.write(
                encodeFrame({
                    header: {
                        version: YAMUX_VERSION,
                        type: f.type,
                        flags: f.flags,
                        streamId: f.streamId,
                        length: 0,
                    },
                    payload: Buffer.alloc(0),
                }),
            );
        }

        await new Promise((resolve) => setTimeout(resolve, 20));

        expect(fatal).toBeUndefined();
        expect(closed).toBe(false);
        // No RST (or any frame) should be emitted in response to frames for unknown streams.
        expect(received).toHaveLength(0);

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

    it("resets only the offending stream on a duplicate SYN, keeping the session alive", async () => {
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

        // Open a real inbound stream (SYN), then send a duplicate SYN for the same stream id.
        const streamId = 1;
        const synFrame = encodeFrame({
            header: {
                version: YAMUX_VERSION,
                type: FrameType.Data,
                flags: FrameFlag.SYN,
                streamId,
                length: 0,
            },
            payload: Buffer.alloc(0),
        });
        const inboundPromise = onceEvent<any>(server, "stream");
        a.write(synFrame);
        const inbound = await inboundPromise;
        const resetPromise = onceEvent(inbound, "error").catch(() => undefined);

        a.write(synFrame); // duplicate SYN

        await new Promise((resolve) => setTimeout(resolve, 20));

        expect(closed, "session must stay open").toBe(false);
        await resetPromise; // the offending stream is reset
        expect(
            received.find((frame) => frame.header.streamId === streamId && (frame.header.flags & FrameFlag.RST) !== 0),
            "peer should be told the stream was reset",
        ).toBeDefined();

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
