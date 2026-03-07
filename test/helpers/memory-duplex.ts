import { Duplex } from "node:stream";

class MemoryDuplex extends Duplex {
    public peer?: MemoryDuplex;

    public override _read(_size: number): void {
        // No-op: data is pushed by peer writes.
    }

    public override _write(
        chunk: Buffer | string,
        encoding: BufferEncoding,
        callback: (error?: Error | null) => void,
    ): void {
        const payload = Buffer.isBuffer(chunk) ? Buffer.from(chunk) : Buffer.from(chunk, encoding);
        this.peer?.push(payload);
        callback();
    }

    public override _final(callback: (error?: Error | null) => void): void {
        this.peer?.push(null);
        callback();
    }
}

export function createDuplexPair(): { a: Duplex; b: Duplex } {
    const a = new MemoryDuplex();
    const b = new MemoryDuplex();
    a.peer = b;
    b.peer = a;
    return { a, b };
}

export function onceEvent<T>(
    emitter: NodeJS.EventEmitter,
    eventName: string,
    timeoutMs = 1_000,
): Promise<T> {
    return new Promise<T>((resolve, reject) => {
        const timer = setTimeout(() => {
            cleanup();
            reject(new Error(`Timed out waiting for event: ${eventName}`));
        }, timeoutMs);

        const onEvent = (arg: T) => {
            cleanup();
            resolve(arg);
        };

        const onError = (error: Error) => {
            cleanup();
            reject(error);
        };

        const cleanup = () => {
            clearTimeout(timer);
            emitter.removeListener(eventName, onEvent as (...args: unknown[]) => void);
            emitter.removeListener("error", onError);
        };

        emitter.once(eventName, onEvent as (...args: unknown[]) => void);
        emitter.once("error", onError);
    });
}
