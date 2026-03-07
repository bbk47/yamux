import { describe, expect, it } from "vitest";
import {
    FrameFlag,
    FrameType,
    HEADER_SIZE,
    YAMUX_VERSION,
    decodeFrame,
    decodeHeader,
    encodeFrame,
} from "../../src";
import { FrameParser } from "../../src/parser";

describe("frame", () => {
    it("encodes and decodes a data frame", () => {
        const payload = Buffer.from("hello");
        const encoded = encodeFrame({
            header: {
                version: YAMUX_VERSION,
                type: FrameType.Data,
                flags: FrameFlag.SYN,
                streamId: 1,
                length: payload.length,
            },
            payload,
        });

        expect(encoded.length).toBe(HEADER_SIZE + payload.length);
        const decoded = decodeFrame(encoded);
        expect(decoded.header.streamId).toBe(1);
        expect(decoded.header.flags).toBe(FrameFlag.SYN);
        expect(decoded.payload.toString()).toBe("hello");
    });

    it("rejects invalid stream id for ping", () => {
        const buffer = Buffer.alloc(HEADER_SIZE);
        buffer.writeUInt8(YAMUX_VERSION, 0);
        buffer.writeUInt8(FrameType.Ping, 1);
        buffer.writeUInt16BE(FrameFlag.SYN, 2);
        buffer.writeUInt32BE(1, 4);
        buffer.writeUInt32BE(42, 8);

        expect(() => decodeHeader(buffer)).not.toThrow();
        expect(() => decodeFrame(buffer)).toThrow(/stream ID 0/);
    });

    it("parses fragmented frames", () => {
        const payload = Buffer.from("abcd");
        const encoded = encodeFrame({
            header: {
                version: YAMUX_VERSION,
                type: FrameType.Data,
                flags: 0,
                streamId: 3,
                length: payload.length,
            },
            payload,
        });

        const parser = new FrameParser();
        const first = parser.feed(encoded.subarray(0, 5));
        const second = parser.feed(encoded.subarray(5));

        expect(first).toHaveLength(0);
        expect(second).toHaveLength(1);
        expect(second[0]?.header.streamId).toBe(3);
        expect(second[0]?.payload.toString()).toBe("abcd");
    });
});
