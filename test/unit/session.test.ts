import { describe, expect, it } from "vitest";
import { createClientSession, createServerSession } from "../../src";
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
});
