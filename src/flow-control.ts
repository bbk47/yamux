import { DEFAULT_INITIAL_WINDOW } from "./constants";
import { YamuxProtocolError } from "./errors";

export class FlowControlWindow {
    private readonly maxReceiveWindow: number;
    private receiveWindow: number;
    private sendWindow: number;

    public constructor(initialWindow = DEFAULT_INITIAL_WINDOW) {
        if (initialWindow <= 0) {
            throw new YamuxProtocolError("Initial window must be greater than zero");
        }

        this.maxReceiveWindow = initialWindow;
        this.receiveWindow = initialWindow;
        this.sendWindow = DEFAULT_INITIAL_WINDOW;
    }

    public onDataReceived(bytes: number): number {
        if (bytes < 0) {
            throw new YamuxProtocolError("Received data byte count cannot be negative");
        }

        if (bytes > this.receiveWindow) {
            throw new YamuxProtocolError("Received data exceeds receive window");
        }

        this.receiveWindow -= bytes;

        // Refill when half of the local window has been consumed.
        if (this.receiveWindow <= this.maxReceiveWindow / 2) {
            const delta = this.maxReceiveWindow - this.receiveWindow;
            this.receiveWindow += delta;
            return delta;
        }

        return 0;
    }

    public onWindowUpdate(delta: number): void {
        if (delta < 0) {
            throw new YamuxProtocolError("Window update delta cannot be negative");
        }

        this.sendWindow += delta;
    }

    public reserveSendCapacity(maxBytes: number): number {
        if (maxBytes <= 0 || this.sendWindow === 0) {
            return 0;
        }

        const allowed = Math.min(maxBytes, this.sendWindow);
        this.sendWindow -= allowed;
        return allowed;
    }

    public getSendWindow(): number {
        return this.sendWindow;
    }
}
