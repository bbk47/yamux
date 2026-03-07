import { encodeFrame } from "./frame";
import { FrameParser } from "./parser";
import type { Frame } from "./types";

export class YamuxCodec {
    private readonly parser = new FrameParser();

    public encode(frame: Frame): Buffer {
        return encodeFrame(frame);
    }

    public decode(chunk: Buffer): Frame[] {
        return this.parser.feed(chunk);
    }

    public reset(): void {
        this.parser.reset();
    }
}
