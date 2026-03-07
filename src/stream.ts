import { Duplex } from "node:stream";
import {
    DEFAULT_MAX_FRAME_SIZE,
    FrameFlag,
    FrameType,
    YAMUX_VERSION,
} from "./constants";
import { YamuxProtocolError, YamuxStreamResetError } from "./errors";
import { FlowControlWindow } from "./flow-control";
import type { Frame, OutboundChunk, SendDataResult } from "./types";

export interface YamuxStreamHooks {
    sendFrame: (frame: Frame) => void;
    onStreamFullyClosed: (streamId: number) => void;
}

export interface YamuxStreamOptions {
    streamId: number;
    hooks: YamuxStreamHooks;
    openedByLocal: boolean;
    initialWindow: number;
    maxFrameSize?: number;
}

export class YamuxStream extends Duplex {
    public readonly streamId: number;

    private readonly hooks: YamuxStreamHooks;
    private readonly flowControl: FlowControlWindow;
    private readonly maxFrameSize: number;

    private readonly outboundQueue: OutboundChunk[] = [];
    private synPending: boolean;
    private ackReceived: boolean;
    private remoteClosed = false;
    private localClosed = false;
    private reset = false;
    private remoteReset = false;

    public constructor(options: YamuxStreamOptions) {
        super();

        this.streamId = options.streamId;
        this.hooks = options.hooks;
        this.flowControl = new FlowControlWindow(options.initialWindow);
        this.maxFrameSize = options.maxFrameSize ?? DEFAULT_MAX_FRAME_SIZE;

        this.synPending = options.openedByLocal;
        this.ackReceived = !options.openedByLocal;
    }

    public open(): void {
        if (!this.synPending) {
            return;
        }

        this.sendFrame(Buffer.alloc(0), FrameFlag.SYN);
        this.synPending = false;
    }

    public onAck(): void {
        this.ackReceived = true;
    }

    public onRemoteFin(): void {
        this.remoteClosed = true;
        this.push(null);
        this.maybeFinalizeStream();
    }

    public onRemoteReset(): void {
        this.remoteReset = true;
        this.reset = true;

        while (this.outboundQueue.length > 0) {
            const item = this.outboundQueue.shift();
            item?.callback(new YamuxStreamResetError());
        }

        this.push(null);
        this.destroy(new YamuxStreamResetError());
        this.hooks.onStreamFullyClosed(this.streamId);
    }

    public onWindowUpdate(delta: number): void {
        this.flowControl.onWindowUpdate(delta);
        this.flushOutboundQueue();
    }

    public onRemoteData(payload: Buffer): void {
        const delta = this.flowControl.onDataReceived(payload.length);

        if (payload.length > 0 && !this.push(payload)) {
            // Backpressure is handled by Node readable internals. We still must not block session-level parsing.
        }

        if (delta > 0) {
            this.hooks.sendFrame({
                header: {
                    version: YAMUX_VERSION,
                    type: FrameType.WindowUpdate,
                    flags: 0,
                    streamId: this.streamId,
                    length: delta,
                },
                payload: Buffer.alloc(0),
            });
        }
    }

    public isAcked(): boolean {
        return this.ackReceived;
    }

    public isClosed(): boolean {
        return this.localClosed && this.remoteClosed;
    }

    public sendReset(): void {
        if (this.reset) {
            return;
        }

        this.reset = true;
        this.hooks.sendFrame({
            header: {
                version: YAMUX_VERSION,
                type: FrameType.Data,
                flags: FrameFlag.RST,
                streamId: this.streamId,
                length: 0,
            },
            payload: Buffer.alloc(0),
        });

        this.hooks.onStreamFullyClosed(this.streamId);
    }

    public override _read(_size: number): void {
        // No-op. Data is pushed from the session as frames arrive.
    }

    public override _write(
        chunk: Buffer | string,
        encoding: BufferEncoding,
        callback: (error?: Error | null) => void,
    ): void {
        if (this.localClosed) {
            callback(new YamuxProtocolError("Cannot write after local FIN"));
            return;
        }

        if (this.reset) {
            callback(new YamuxStreamResetError());
            return;
        }

        const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, encoding);
        this.outboundQueue.push({
            chunk: buffer,
            offset: 0,
            callback,
        });

        this.flushOutboundQueue();
    }

    public override _final(callback: (error?: Error | null) => void): void {
        if (this.localClosed) {
            callback();
            return;
        }

        try {
            const flags = FrameFlag.FIN | (this.synPending ? FrameFlag.SYN : 0);
            this.sendFrame(Buffer.alloc(0), flags);
            this.synPending = false;
            this.localClosed = true;
            this.maybeFinalizeStream();
            callback();
        } catch (error) {
            callback(error as Error);
        }
    }

    public override _destroy(error: Error | null, callback: (error?: Error | null) => void): void {
        if (!this.remoteReset && !this.reset && !this.isClosed()) {
            try {
                this.sendReset();
            } catch {
                // Ignore transport-side failures during stream teardown.
            }
        }

        callback(error);
    }

    private flushOutboundQueue(): void {
        if (this.reset) {
            while (this.outboundQueue.length > 0) {
                this.outboundQueue.shift()?.callback(new YamuxStreamResetError());
            }
            return;
        }

        while (this.outboundQueue.length > 0) {
            const item = this.outboundQueue[0];
            if (!item) {
                return;
            }

            const remaining = item.chunk.length - item.offset;
            const reservable = this.flowControl.reserveSendCapacity(
                Math.min(remaining, this.maxFrameSize),
            );

            if (reservable === 0) {
                return;
            }

            const payload = item.chunk.subarray(item.offset, item.offset + reservable);
            const flags = this.synPending ? FrameFlag.SYN : 0;
            this.sendFrame(payload, flags);
            this.synPending = false;
            item.offset += reservable;

            if (item.offset === item.chunk.length) {
                this.outboundQueue.shift();
                item.callback();
            }
        }
    }

    private sendFrame(payload: Buffer, flags: number): SendDataResult {
        this.hooks.sendFrame({
            header: {
                version: YAMUX_VERSION,
                type: FrameType.Data,
                flags,
                streamId: this.streamId,
                length: payload.length,
            },
            payload,
        });

        return {
            bytesSent: payload.length,
            frameFlags: flags,
        };
    }

    private maybeFinalizeStream(): void {
        if (this.isClosed() || this.reset) {
            this.hooks.onStreamFullyClosed(this.streamId);
        }
    }
}
