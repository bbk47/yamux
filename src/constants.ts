export const YAMUX_VERSION = 0;
export const HEADER_SIZE = 12;
export const DEFAULT_INITIAL_WINDOW = 256 * 1024;
export const DEFAULT_MAX_FRAME_SIZE = 64 * 1024;

export const enum FrameType {
    Data = 0x0,
    WindowUpdate = 0x1,
    Ping = 0x2,
    GoAway = 0x3,
}

export const enum FrameFlag {
    SYN = 0x1,
    ACK = 0x2,
    FIN = 0x4,
    RST = 0x8,
}

export const enum GoAwayCode {
    Normal = 0x0,
    ProtocolError = 0x1,
    InternalError = 0x2,
}
