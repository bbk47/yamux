# yamux-ts

TypeScript implementation of Yamux for Node.js.

This library multiplexes many logical `Duplex` streams over a single underlying transport (typically a TCP socket), following the Yamux framing and stream lifecycle rules.

## Features

- Yamux frame codec (12-byte header, big-endian)
- `SYN`/`ACK`/`FIN`/`RST` stream lifecycle
- Per-stream flow control (default 256 KB)
- Session-level `Ping` and `GoAway`
- Node.js `Duplex` API for each logical stream
- Interop tests with `hashicorp/yamux` (Go)

## Install

If published to npm:

```bash
pnpm add yamux-ts
```

If used locally in this repository:

```bash
pnpm install
pnpm build
```

## Quick Start

### Client side

```ts
import net from "node:net";
import { createClientSession } from "yamux-ts";

const socket = net.connect(9000, "127.0.0.1");

socket.once("connect", async () => {
  const session = createClientSession(socket);

  session.on("error", (err) => {
    console.error("session error", err);
  });

  const stream = session.openStream();
  stream.write("hello over yamux\n");
  stream.end();

  for await (const chunk of stream) {
    process.stdout.write(chunk);
  }

  const ping = await session.ping();
  console.log("rtt(ms)", ping.rttMs);

  session.goAway();
  session.close();
});
```

### Server side

```ts
import net from "node:net";
import { createServerSession } from "yamux-ts";

const server = net.createServer((socket) => {
  const session = createServerSession(socket);

  session.on("stream", (stream) => {
    stream.on("data", (chunk) => {
      // Echo back.
      stream.write(chunk);
    });

    stream.on("end", () => {
      stream.end();
    });
  });

  session.on("goaway", (code) => {
    console.log("peer sent goaway", code);
  });

  session.on("error", (err) => {
    console.error("session error", err);
    session.close();
  });
});

server.listen(9000, "127.0.0.1");
```

## API

### `createClientSession(transport, options?)`

Create a Yamux session in client mode (outbound stream IDs are odd: `1, 3, 5...`).

### `createServerSession(transport, options?)`

Create a Yamux session in server mode (outbound stream IDs are even: `2, 4, 6...`).

### `new YamuxSession(transport, options)`

`options`:

- `role: "client" | "server"` (required)
- `initialStreamWindow?: number` default `256 * 1024`
- `maxFrameSize?: number` default `64 * 1024`

Methods:

- `openStream(): YamuxStream`
- `ping(timeoutMs?: number): Promise<{ nonce: number; rttMs: number }>`
- `goAway(code?: GoAwayCode): void`
- `close(): void`

Events:

- `stream` incoming `YamuxStream`
- `goaway` peer session termination code
- `error` session/protocol error
- `close` session closed

### `YamuxStream` (extends `Duplex`)

Use it as a normal Node stream:

- write with `stream.write()` / `stream.end()`
- read with `stream.on("data")` / async iteration
- remote half-close maps to `end`
- reset maps to stream error (`YamuxStreamResetError`)

## Constants and Types

Exported protocol constants:

- `YAMUX_VERSION`
- `HEADER_SIZE`
- `FrameType`
- `FrameFlag`
- `GoAwayCode`
- `DEFAULT_INITIAL_WINDOW`
- `DEFAULT_MAX_FRAME_SIZE`

Exported low-level helpers:

- `encodeFrame`, `decodeFrame`, `decodeHeader`, `writeHeader`
- `FrameParser`, `YamuxCodec`

## Error Semantics

- `YamuxProtocolError`: invalid frame or protocol violation
- `YamuxClosedError`: operation on closed/goaway session
- `YamuxStreamResetError`: stream reset (RST)

On protocol violation, session sends `GoAway(ProtocolError)` and closes.

## Development

```bash
pnpm typecheck
pnpm test
pnpm test:interop
pnpm build
```

## Interop Testing

`pnpm test:interop` starts a Go process in `test/interop/go` using `hashicorp/yamux` and validates:

- stream open + payload echo
- concurrent streams
- ping roundtrip

## LLM Integration Guide

For code generation agents, see:

- `docs/LLM_GUIDE.md`
