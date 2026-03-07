import {
    FrameFlag,
    FrameType,
    GoAwayCode,
    HEADER_SIZE,
    YAMUX_VERSION,
} from "./constants";
import { YamuxProtocolError } from "./errors";
import type { Frame, FrameHeader, FramePredicate } from "./types";

export function encodeFrame(frame: Frame): Buffer {
    validateHeader(frame.header);

    const payloadLength = wirePayloadLength(frame.header);
    if (frame.payload.length !== payloadLength) {
        throw new YamuxProtocolError("Frame payload length mismatch");
    }

    const buffer = Buffer.allocUnsafe(HEADER_SIZE + payloadLength);
    writeHeader(frame.header, buffer, 0);
    if (payloadLength > 0) {
        frame.payload.copy(buffer, HEADER_SIZE);
    }
    return buffer;
}

export function decodeHeader(buffer: Buffer, offset = 0): FrameHeader {
    if (buffer.length - offset < HEADER_SIZE) {
        throw new YamuxProtocolError("Insufficient bytes for Yamux header");
    }

    const version = buffer.readUInt8(offset);
    const type = buffer.readUInt8(offset + 1);
    const flags = buffer.readUInt16BE(offset + 2);
    const streamId = buffer.readUInt32BE(offset + 4);
    const length = buffer.readUInt32BE(offset + 8);

    return {
        version,
        type,
        flags,
        streamId,
        length,
    } as FrameHeader;
}

export function writeHeader(header: FrameHeader, target: Buffer, offset = 0): void {
    if (target.length - offset < HEADER_SIZE) {
        throw new YamuxProtocolError("Target buffer too small for Yamux header");
    }

    validateHeader(header);

    target.writeUInt8(header.version, offset);
    target.writeUInt8(header.type, offset + 1);
    target.writeUInt16BE(header.flags, offset + 2);
    target.writeUInt32BE(header.streamId, offset + 4);
    target.writeUInt32BE(header.length, offset + 8);
}

export function decodeFrame(buffer: Buffer): Frame {
    const header = decodeHeader(buffer);
    const payloadLength = wirePayloadLength(header);

    if (buffer.length !== HEADER_SIZE + payloadLength) {
        throw new YamuxProtocolError("Invalid frame size");
    }

    const payload = payloadLength > 0 ? buffer.subarray(HEADER_SIZE) : Buffer.alloc(0);
    validateHeader(header);
    validateFrameSemantics(header, payload);
    return { header, payload };
}

export function validateHeader(header: FrameHeader): void {
    if (header.version !== YAMUX_VERSION) {
        throw new YamuxProtocolError(`Unsupported Yamux version ${header.version}`);
    }

    if (!isValidFrameType(header.type)) {
        throw new YamuxProtocolError(`Unknown Yamux frame type ${header.type}`);
    }

    if (header.streamId === 0 && (header.type === FrameType.Data || header.type === FrameType.WindowUpdate)) {
        throw new YamuxProtocolError("Data/WindowUpdate frame must use non-zero stream ID");
    }

    if (header.streamId !== 0 && (header.type === FrameType.Ping || header.type === FrameType.GoAway)) {
        throw new YamuxProtocolError("Ping/GoAway frame must use stream ID 0");
    }

    if (header.flags & ~allKnownFlags()) {
        throw new YamuxProtocolError("Frame contains unknown flags");
    }
}

export function validateFrameSemantics(header: FrameHeader, payload: Buffer): void {
    switch (header.type) {
        case FrameType.Data:
            if (payload.length !== header.length) {
                throw new YamuxProtocolError("Data frame payload length mismatch");
            }
            return;
        case FrameType.WindowUpdate:
            if (payload.length !== 0) {
                throw new YamuxProtocolError("WindowUpdate frame must not contain payload");
            }
            return;
        case FrameType.Ping:
            if (payload.length !== 0) {
                throw new YamuxProtocolError("Ping frame must not contain payload");
            }
            return;
        case FrameType.GoAway:
            if (payload.length !== 0) {
                throw new YamuxProtocolError("GoAway frame must not contain payload");
            }
            if (!isValidGoAwayCode(header.length)) {
                throw new YamuxProtocolError(`Unknown GoAway code ${header.length}`);
            }
            return;
        default:
            throw new YamuxProtocolError(`Unsupported frame type ${header.type}`);
    }
}

export function assertFrame(frame: Frame, predicate: FramePredicate): void {
    if (predicate.type !== undefined && frame.header.type !== predicate.type) {
        throw new YamuxProtocolError(
            `Unexpected frame type ${frame.header.type}, expected ${predicate.type}`,
        );
    }

    if (predicate.streamIdMustBeZero && frame.header.streamId !== 0) {
        throw new YamuxProtocolError("Expected stream ID 0");
    }

    if (predicate.requiredFlags) {
        for (const flag of predicate.requiredFlags) {
            if ((frame.header.flags & flag) === 0) {
                throw new YamuxProtocolError(`Expected frame flag ${flag}`);
            }
        }
    }

    if (predicate.forbiddenFlags) {
        for (const flag of predicate.forbiddenFlags) {
            if ((frame.header.flags & flag) !== 0) {
                throw new YamuxProtocolError(`Unexpected frame flag ${flag}`);
            }
        }
    }
}

export function makeControlFrame(
    type: FrameType.WindowUpdate | FrameType.Ping | FrameType.GoAway,
    streamId: number,
    flags: number,
    length: number,
): Frame {
    return {
        header: {
            version: YAMUX_VERSION,
            type,
            flags,
            streamId,
            length,
        },
        payload: Buffer.alloc(0),
    };
}

export function makeDataFrame(streamId: number, flags: number, payload: Buffer): Frame {
    return {
        header: {
            version: YAMUX_VERSION,
            type: FrameType.Data,
            flags,
            streamId,
            length: payload.length,
        },
        payload,
    };
}

function allKnownFlags(): number {
    return FrameFlag.SYN | FrameFlag.ACK | FrameFlag.FIN | FrameFlag.RST;
}

function isValidFrameType(type: number): type is FrameType {
    return (
        type === FrameType.Data ||
        type === FrameType.WindowUpdate ||
        type === FrameType.Ping ||
        type === FrameType.GoAway
    );
}

function isValidGoAwayCode(code: number): code is GoAwayCode {
    return code === GoAwayCode.Normal || code === GoAwayCode.ProtocolError || code === GoAwayCode.InternalError;
}

export function wirePayloadLength(header: FrameHeader): number {
    return header.type === FrameType.Data ? header.length : 0;
}
