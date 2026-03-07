import type { FrameFlag, FrameType, GoAwayCode } from "./constants";

export interface FrameHeader {
    version: number;
    type: FrameType;
    flags: number;
    streamId: number;
    length: number;
}

export interface Frame {
    header: FrameHeader;
    payload: Buffer;
}

export type SessionRole = "client" | "server";

export interface YamuxSessionOptions {
    role: SessionRole;
    initialStreamWindow?: number;
    maxFrameSize?: number;
}

export interface OpenStreamOptions {
    signal?: AbortSignal;
}

export interface PingResult {
    nonce: number;
    rttMs: number;
}

export interface StreamOpenEvent {
    streamId: number;
}

export interface StreamClosedEvent {
    streamId: number;
    hadError: boolean;
}

export interface YamuxStreamState {
    localOpened: boolean;
    remoteOpened: boolean;
    localClosed: boolean;
    remoteClosed: boolean;
    reset: boolean;
}

export interface OutboundChunk {
    chunk: Buffer;
    offset: number;
    callback: (error?: Error | null) => void;
}

export type FramePredicate = {
    type?: FrameType;
    requiredFlags?: FrameFlag[];
    forbiddenFlags?: FrameFlag[];
    streamIdMustBeZero?: boolean;
};

export interface PendingPing {
    startedAt: number;
    resolve: (result: PingResult) => void;
    reject: (error: Error) => void;
    timer?: NodeJS.Timeout;
}

export interface SendDataResult {
    bytesSent: number;
    frameFlags: number;
}

export type GoAwayReason = GoAwayCode;
