import { HEADER_SIZE } from "./constants";
import {
    decodeHeader,
    validateFrameSemantics,
    validateHeader,
    wirePayloadLength,
} from "./frame";
import type { Frame } from "./types";

export class FrameParser {
    private buffered = Buffer.alloc(0);

    public feed(chunk: Buffer): Frame[] {
        if (chunk.length === 0) {
            return [];
        }

        this.buffered =
            this.buffered.length === 0 ? Buffer.from(chunk) : Buffer.concat([this.buffered, chunk]);
        const frames: Frame[] = [];

        while (this.buffered.length >= HEADER_SIZE) {
            const header = decodeHeader(this.buffered);
            validateHeader(header);

            const frameSize = HEADER_SIZE + wirePayloadLength(header);
            if (this.buffered.length < frameSize) {
                break;
            }

            const payload = this.buffered.subarray(HEADER_SIZE, frameSize);
            validateFrameSemantics(header, payload);
            frames.push({ header, payload });
            this.buffered = this.buffered.subarray(frameSize);
        }

        return frames;
    }

    public reset(): void {
        this.buffered = Buffer.alloc(0);
    }
}
