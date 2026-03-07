# LLM Guide for yamux-ts

This file is for code-generation agents that need to call `yamux-ts` correctly.

## Goal

Use a single transport (usually a `net.Socket`) and multiplex multiple logical streams with Yamux.

## Canonical API

Imports:

```ts
import { createClientSession, createServerSession, GoAwayCode } from "yamux-ts";
```

Client session:

```ts
const session = createClientSession(socket, {
  initialStreamWindow: 256 * 1024,
  maxFrameSize: 64 * 1024,
});
```

Server session:

```ts
const session = createServerSession(socket);
```

## Required Usage Pattern

1. Create session from an already-connected transport.
2. Attach `session.on("error", ...)` immediately.
3. For outbound request:
- call `const stream = session.openStream()`
- write payload to stream
- end stream when done
- read response from stream
4. For inbound request:
- handle `session.on("stream", (stream) => { ... })`
- read request data
- write response
- `stream.end()` when complete
5. Call `session.close()` on shutdown.

## Minimal Client Example

```ts
import net from "node:net";
import { createClientSession } from "yamux-ts";

const socket = net.connect(9000, "127.0.0.1");
await new Promise<void>((resolve) => socket.once("connect", () => resolve()));

const session = createClientSession(socket);
session.on("error", console.error);

const stream = session.openStream();
stream.write(Buffer.from("request"));
stream.end();

const chunks: Buffer[] = [];
for await (const chunk of stream) {
  chunks.push(Buffer.from(chunk));
}
const response = Buffer.concat(chunks);

const ping = await session.ping(5000);
console.log(ping.rttMs);

session.goAway();
session.close();
```

## Minimal Server Example

```ts
import net from "node:net";
import { createServerSession } from "yamux-ts";

const server = net.createServer((socket) => {
  const session = createServerSession(socket);
  session.on("error", console.error);

  session.on("stream", (stream) => {
    const chunks: Buffer[] = [];

    stream.on("data", (chunk) => {
      chunks.push(Buffer.from(chunk));
    });

    stream.on("end", () => {
      const req = Buffer.concat(chunks);
      stream.write(req); // echo
      stream.end();
    });
  });
});

server.listen(9000);
```

## Protocol Facts Agents Must Respect

- Stream ID parity:
- Client outbound IDs are odd.
- Server outbound IDs are even.
- Stream ID `0` is reserved for session control (`Ping`, `GoAway`).
- Frame `Length` field:
- For `Data`: payload byte length.
- For `WindowUpdate`: receive-window delta.
- For `Ping`: opaque nonce.
- For `GoAway`: termination code.
- Flow control only counts `Data` bytes.

## Common Mistakes to Avoid

- Do not open streams after `goAway` or `close`.
- Do not ignore stream/session `error` events.
- Do not assume one `data` event equals one message; stream data is chunked.
- Do not manually assign stream IDs; let `openStream()` handle it.

## Lifecycle Recommendations

- Graceful shutdown:
1. `session.goAway(GoAwayCode.Normal)`
2. stop creating new streams
3. close existing streams
4. `session.close()`

- Error shutdown:
1. log context
2. `session.close()`

## Test Commands

```bash
pnpm test
pnpm test:interop
```

`test:interop` requires Go and uses `test/interop/go`.
