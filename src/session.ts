import { EventEmitter } from "node:events";
import type { Duplex } from "node:stream";
import {
    DEFAULT_INITIAL_WINDOW,
    DEFAULT_MAX_FRAME_SIZE,
    FrameFlag,
    FrameType,
    GoAwayCode,
    YAMUX_VERSION,
} from "./constants";
import { YamuxCodec } from "./codec";
import { YamuxClosedError, YamuxProtocolError } from "./errors";
import type {
    Frame,
    OpenStreamOptions,
    PendingPing,
    PingResult,
    SessionRole,
    YamuxConfig,
    YamuxSessionOptions,
} from "./types";
import { YamuxStream } from "./stream";

export interface YamuxSessionEvents {
    stream: (stream: YamuxStream) => void;
    goaway: (code: GoAwayCode) => void;
    close: () => void;
    error: (error: Error) => void;
}

export class YamuxSession extends EventEmitter {
    private readonly role: SessionRole;
    private readonly transport: Duplex;
    private readonly codec = new YamuxCodec();
    private readonly streams = new Map<number, YamuxStream>();
    private readonly initialWindow: number;
    private readonly maxFrameSize: number;

    private nextStreamId: number;
    private goAwaySent = false;
    private goAwayReceived = false;
    private closed = false;

    private nextPingNonce = 1;
    private readonly pendingPings = new Map<number, PendingPing>();

    public constructor(transport: Duplex, options: YamuxSessionOptions) {
        super();

        this.transport = transport;
        this.role = options.role;
        this.initialWindow = options.initialStreamWindow ?? DEFAULT_INITIAL_WINDOW;
        this.maxFrameSize = options.maxFrameSize ?? DEFAULT_MAX_FRAME_SIZE;
        this.nextStreamId = this.role === "client" ? 1 : 2;

        this.transport.on("data", (chunk: Buffer) => this.onTransportData(chunk));
        this.transport.on("error", (error: Error) => this.onTransportError(error));
        this.transport.on("close", () => this.onTransportClose());
        this.transport.on("end", () => this.onTransportClose());
    }

    public override on<K extends keyof YamuxSessionEvents>(event: K, listener: YamuxSessionEvents[K]): this {
        return super.on(event, listener);
    }

    public openStream(_options?: OpenStreamOptions): YamuxStream {
        if (this.closed || this.goAwaySent || this.goAwayReceived) {
            throw new YamuxClosedError("Cannot open stream on closed or goaway session");
        }

        const streamId = this.nextStreamId;
        this.nextStreamId += 2;
        const stream = this.createStream(streamId, true);
        stream.open();
        return stream;
    }

    public async ping(timeoutMs = 5_000): Promise<PingResult> {
        if (this.closed) {
            throw new YamuxClosedError();
        }

        const nonce = this.nextPingNonce++ >>> 0;

        return new Promise<PingResult>((resolve, reject) => {
            const timer =
                timeoutMs > 0
                    ? setTimeout(() => {
                        this.pendingPings.delete(nonce);
                        reject(new YamuxClosedError("Ping timed out"));
                    }, timeoutMs)
                    : undefined;

            const pending: PendingPing = {
                startedAt: Date.now(),
                resolve,
                reject,
            };

            if (timer) {
                pending.timer = timer;
            }

            this.pendingPings.set(nonce, pending);

            this.sendFrame({
                header: {
                    version: YAMUX_VERSION,
                    type: FrameType.Ping,
                    flags: FrameFlag.SYN,
                    streamId: 0,
                    length: nonce,
                },
                payload: Buffer.alloc(0),
            });
        });
    }

    public goAway(code: GoAwayCode = GoAwayCode.Normal): void {
        if (this.goAwaySent) {
            return;
        }

        this.goAwaySent = true;
        this.sendFrame({
            header: {
                version: YAMUX_VERSION,
                type: FrameType.GoAway,
                flags: 0,
                streamId: 0,
                length: code,
            },
            payload: Buffer.alloc(0),
        });
    }

    public close(): void {
        if (this.closed) {
            return;
        }

        this.closed = true;

        for (const stream of this.streams.values()) {
            stream.destroy();
        }
        this.streams.clear();

        for (const [nonce, pending] of this.pendingPings.entries()) {
            if (pending.timer) {
                clearTimeout(pending.timer);
            }
            pending.reject(new YamuxClosedError("Session closed"));
            this.pendingPings.delete(nonce);
        }

        this.transport.destroy();
        this.emit("close");
    }

    private onTransportData(chunk: Buffer): void {
        if (this.closed) {
            return;
        }

        try {
            const frames = this.codec.decode(chunk);
            for (const frame of frames) {
                this.handleFrame(frame);
            }
        } catch (error) {
            this.handleProtocolViolation(error as Error);
        }
    }

    private handleFrame(frame: Frame): void {
        if (frame.header.type === FrameType.Ping) {
            this.handlePing(frame);
            return;
        }

        if (frame.header.type === FrameType.GoAway) {
            this.goAwayReceived = true;
            this.emit("goaway", frame.header.length as GoAwayCode);
            return;
        }

        const streamId = frame.header.streamId;
        if (streamId === 0) {
            throw new YamuxProtocolError("Stream control frame has stream ID 0");
        }

        let stream = this.streams.get(streamId);
        const hasSyn = (frame.header.flags & FrameFlag.SYN) !== 0;

        if (!stream) {
            if (!hasSyn) {
                // Frame for an unknown/already-closed stream. Crossing frames are inherent to
                // multiplexing (e.g. a trailing WindowUpdate the peer emitted while still draining
                // our earlier data, or a FIN that crosses ours). Silently ignore and keep the
                // session alive, matching hashicorp/yamux which discards frames for missing streams.
                //
                // We deliberately do NOT reply RST here: after a clean both-FIN close we may have
                // removed the stream while the peer's read side is still draining, and an RST would
                // reset that still-valid half-open stream (possible truncation).
                return;
            }

            stream = this.createStream(streamId, false);
            this.emit("stream", stream);
            this.sendAck(streamId);
        } else if (hasSyn) {
            // A duplicate SYN for an existing stream means stream-ID reuse / peer desync. Reset
            // just that one stream (and tell the peer with RST) rather than tearing down the whole
            // session, keeping every other multiplexed stream alive.
            stream.onRemoteReset();
            this.streams.delete(streamId);
            this.sendStreamReset(streamId);
            return;
        }

        if ((frame.header.flags & FrameFlag.ACK) !== 0) {
            stream.onAck();
        }

        if ((frame.header.flags & FrameFlag.RST) !== 0) {
            stream.onRemoteReset();
            this.streams.delete(streamId);
            return;
        }

        if (frame.header.type === FrameType.WindowUpdate) {
            stream.onWindowUpdate(frame.header.length);
        }

        if (frame.header.type === FrameType.Data && frame.payload.length > 0) {
            stream.onRemoteData(frame.payload);
        }

        if ((frame.header.flags & FrameFlag.FIN) !== 0) {
            stream.onRemoteFin();
            if (stream.isClosed()) {
                this.streams.delete(streamId);
            }
        }
    }

    private handlePing(frame: Frame): void {
        if (frame.header.streamId !== 0) {
            throw new YamuxProtocolError("Ping stream ID must be 0");
        }

        const hasSyn = (frame.header.flags & FrameFlag.SYN) !== 0;
        const hasAck = (frame.header.flags & FrameFlag.ACK) !== 0;

        if (hasSyn && hasAck) {
            throw new YamuxProtocolError("Ping frame cannot contain SYN and ACK simultaneously");
        }

        if (hasSyn) {
            this.sendFrame({
                header: {
                    version: YAMUX_VERSION,
                    type: FrameType.Ping,
                    flags: FrameFlag.ACK,
                    streamId: 0,
                    length: frame.header.length,
                },
                payload: Buffer.alloc(0),
            });
            return;
        }

        if (!hasAck) {
            throw new YamuxProtocolError("Ping frame must contain SYN or ACK");
        }

        const pending = this.pendingPings.get(frame.header.length);
        if (!pending) {
            return;
        }

        if (pending.timer) {
            clearTimeout(pending.timer);
        }

        this.pendingPings.delete(frame.header.length);
        pending.resolve({
            nonce: frame.header.length,
            rttMs: Date.now() - pending.startedAt,
        });
    }

    private sendStreamReset(streamId: number): void {
        try {
            this.sendFrame({
                header: {
                    version: YAMUX_VERSION,
                    type: FrameType.WindowUpdate,
                    flags: FrameFlag.RST,
                    streamId,
                    length: 0,
                },
                payload: Buffer.alloc(0),
            });
        } catch {
            // Ignore transport-side failures while signalling a reset for an unknown stream.
        }
    }

    private sendAck(streamId: number): void {
        this.sendFrame({
            header: {
                version: YAMUX_VERSION,
                type: FrameType.Data,
                flags: FrameFlag.ACK,
                streamId,
                length: 0,
            },
            payload: Buffer.alloc(0),
        });

        const delta = this.initialWindow - DEFAULT_INITIAL_WINDOW;
        if (delta > 0) {
            this.sendFrame({
                header: {
                    version: YAMUX_VERSION,
                    type: FrameType.WindowUpdate,
                    flags: FrameFlag.ACK,
                    streamId,
                    length: delta,
                },
                payload: Buffer.alloc(0),
            });
        }
    }

    private createStream(streamId: number, openedByLocal: boolean): YamuxStream {
        this.validateStreamId(streamId, openedByLocal);

        const stream = new YamuxStream({
            streamId,
            openedByLocal,
            initialWindow: this.initialWindow,
            maxFrameSize: this.maxFrameSize,
            hooks: {
                sendFrame: (frame) => this.sendFrame(frame),
                onStreamFullyClosed: (id) => {
                    this.streams.delete(id);
                },
            },
        });

        stream.once("close", () => {
            if (stream.isClosed()) {
                this.streams.delete(streamId);
            }
        });

        stream.once("error", (error) => {
            this.emit("error", error);
        });

        this.streams.set(streamId, stream);
        return stream;
    }

    private validateStreamId(streamId: number, openedByLocal: boolean): void {
        const isOdd = (streamId & 1) === 1;
        const shouldBeOdd = this.role === "client";

        if (openedByLocal && isOdd !== shouldBeOdd) {
            throw new YamuxProtocolError(`Local stream ID parity mismatch: ${streamId}`);
        }

        if (!openedByLocal && isOdd === shouldBeOdd) {
            throw new YamuxProtocolError(`Remote stream ID parity mismatch: ${streamId}`);
        }
    }

    private sendFrame(frame: Frame): void {
        if (this.closed) {
            throw new YamuxClosedError();
        }

        const encoded = this.codec.encode(frame);
        const ok = this.transport.write(encoded);
        if (!ok) {
            // Keep semantics simple for now; transport backpressure is still enforced at socket level.
        }
    }

    private onTransportError(error: Error): void {
        this.emit("error", error);
        this.close();
    }

    private onTransportClose(): void {
        if (this.closed) {
            return;
        }

        this.close();
    }

    private handleProtocolViolation(error: Error): void {
        this.emit("error", error);
        this.goAway(GoAwayCode.ProtocolError);
        this.close();
    }
}

export function createClientSession(
    transport: Duplex,
    options: Omit<YamuxSessionOptions, "role"> = {},
): YamuxSession {
    return new YamuxSession(transport, {
        ...options,
        role: "client",
    });
}

export function createServerSession(
    transport: Duplex,
    options: Omit<YamuxSessionOptions, "role"> = {},
): YamuxSession {
    return new YamuxSession(transport, {
        ...options,
        role: "server",
    });
}

// Go-style compatibility entrypoint.
export function Client(transport: Duplex, config: YamuxConfig = {}): YamuxSession {
    return createClientSession(transport, config);
}

// Go-style compatibility entrypoint.
export function Server(transport: Duplex, config: YamuxConfig = {}): YamuxSession {
    return createServerSession(transport, config);
}
