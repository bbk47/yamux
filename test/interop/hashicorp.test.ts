import { once } from "node:events";
import { spawn, spawnSync, type ChildProcessByStdio } from "node:child_process";
import net from "node:net";
import path from "node:path";
import type { Readable } from "node:stream";
import { afterEach, describe, expect, it } from "vitest";
import { createClientSession } from "../../src";
import type { YamuxStream } from "../../src/stream";

const hasGo = spawnSync("go", ["version"], { stdio: "ignore" }).status === 0;
const describeInterop = hasGo ? describe : describe.skip;

describeInterop("hashicorp/yamux interop", () => {
    const resources: Array<() => void> = [];

    afterEach(() => {
        for (const cleanup of resources.splice(0, resources.length).reverse()) {
            cleanup();
        }
    });

    it("opens stream and exchanges payload", async () => {
        const server = await startGoServer(resources);
        const socket = net.connect(server.port, "127.0.0.1");
        resources.push(() => socket.destroy());
        await once(socket, "connect");

        const session = createClientSession(socket);
        resources.push(() => session.close());
        session.on("error", () => {
            // Errors are asserted through failed operations in this test.
        });

        const stream = session.openStream();
        const echoed = await writeAndCollect(stream, Buffer.from("interop-hello"));
        expect(echoed.toString()).toBe("interop-hello");
    }, 20_000);

    it("supports concurrent streams against go server", async () => {
        const server = await startGoServer(resources);
        const socket = net.connect(server.port, "127.0.0.1");
        resources.push(() => socket.destroy());
        await once(socket, "connect");

        const session = createClientSession(socket);
        resources.push(() => session.close());
        session.on("error", () => {
            // Errors are asserted through failed operations in this test.
        });

        const payloads = ["s1", "stream-two", "payload-3", "four", "five"];
        const results = await Promise.all(
            payloads.map((text) => writeAndCollect(session.openStream(), Buffer.from(text))),
        );

        expect(results.map((item) => item.toString())).toEqual(payloads);
    }, 20_000);

    it("handles ping roundtrip with go peer", async () => {
        const server = await startGoServer(resources);
        const socket = net.connect(server.port, "127.0.0.1");
        resources.push(() => socket.destroy());
        await once(socket, "connect");

        const session = createClientSession(socket);
        resources.push(() => session.close());

        const result = await session.ping(5_000);
        expect(result.nonce).toBeGreaterThan(0);
        expect(result.rttMs).toBeGreaterThanOrEqual(0);
    }, 20_000);
});

async function startGoServer(
    cleanups: Array<() => void>,
): Promise<{ proc: ChildProcessByStdio<null, Readable, Readable>; port: number }> {
    const cwd = path.resolve(process.cwd(), "test/interop/go");
    const proc = spawn("go", ["run", "."], {
        cwd,
        stdio: ["ignore", "pipe", "pipe"],
    });

    cleanups.push(() => {
        if (!proc.killed) {
            proc.kill("SIGTERM");
        }
    });

    const stderrChunks: Buffer[] = [];
    proc.stderr.on("data", (chunk: Buffer) => {
        stderrChunks.push(Buffer.from(chunk));
    });

    const port = await new Promise<number>((resolve, reject) => {
        const timeout = setTimeout(() => {
            reject(new Error(`Timed out waiting go server startup. stderr=${Buffer.concat(stderrChunks).toString("utf8")}`));
        }, 15_000);

        const onData = (chunk: Buffer) => {
            const text = chunk.toString("utf8").trim();
            const match = /^READY\s+(\d+)$/.exec(text);
            if (!match) {
                return;
            }

            clearTimeout(timeout);
            proc.stdout.off("data", onData);
            resolve(Number(match[1]));
        };

        proc.stdout.on("data", onData);
        proc.once("exit", (code) => {
            clearTimeout(timeout);
            reject(new Error(`go server exited early with code ${code}. stderr=${Buffer.concat(stderrChunks).toString("utf8")}`));
        });
    });

    return { proc, port };
}

async function writeAndCollect(stream: YamuxStream, payload: Buffer): Promise<Buffer> {
    const chunks: Buffer[] = [];
    stream.on("data", (chunk: Buffer) => {
        chunks.push(Buffer.from(chunk));
    });

    await writeStream(stream, payload);
    stream.end();
    await once(stream, "end");
    return Buffer.concat(chunks);
}

async function writeStream(stream: YamuxStream, payload: Buffer): Promise<void> {
    await new Promise<void>((resolve, reject) => {
        stream.write(payload, (error) => {
            if (error) {
                reject(error);
                return;
            }
            resolve();
        });
    });
}
