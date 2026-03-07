export class YamuxError extends Error {
    public readonly code: string;

    public constructor(code: string, message: string) {
        super(message);
        this.name = "YamuxError";
        this.code = code;
    }
}

export class YamuxProtocolError extends YamuxError {
    public constructor(message: string) {
        super("ERR_YAMUX_PROTOCOL", message);
        this.name = "YamuxProtocolError";
    }
}

export class YamuxClosedError extends YamuxError {
    public constructor(message = "Yamux session is closed") {
        super("ERR_YAMUX_CLOSED", message);
        this.name = "YamuxClosedError";
    }
}

export class YamuxStreamResetError extends YamuxError {
    public constructor(message = "Yamux stream was reset") {
        super("ERR_YAMUX_STREAM_RESET", message);
        this.name = "YamuxStreamResetError";
    }
}
