package io.latticedb;

/** Error codes mirroring {@code lattice_error} from the native C API. */
public enum ErrorCode {
    OK(0),
    ERROR(-1),
    IO(-2),
    CORRUPTION(-3),
    NOT_FOUND(-4),
    ALREADY_EXISTS(-5),
    INVALID_ARG(-6),
    TXN_ABORTED(-7),
    LOCK_TIMEOUT(-8),
    READ_ONLY(-9),
    FULL(-10),
    VERSION_MISMATCH(-11),
    CHECKSUM(-12),
    OUT_OF_MEMORY(-13),
    UNSUPPORTED(-14),
    VALUE_TOO_LARGE(-15),
    /** Another process holds the database file (native code -16, added in
     * 0.13.0). Distinct from {@link #LOCK_TIMEOUT}, which reports a second
     * writer inside this process. */
    DATABASE_LOCKED(-16),
    /** {@link Transaction#deleteNode(long)} refused a node that still has
     * relationships, and deleted nothing (native code -17).
     * {@link Transaction#detachDeleteNode(long)} deletes them with it. */
    HAS_RELATIONSHIPS(-17),
    /** The open options contradict the vector index the database file
     * records: its dimensions, metric, M or ef_construction (native code
     * -18). */
    CONFIG_MISMATCH(-18),
    /** The database file was written by a newer release that does not let
     * this one write it (native code -19). It opens with
     * {@code readOnly(true)}; a writable open writes nothing. */
    NEEDS_NEWER_WRITER(-19),
    /** Placeholder for native codes this binding does not recognize. The
     * original numeric code is preserved on the exception that carries it
     * (see {@link LatticeException#getNativeCode()}). */
    UNKNOWN(-9999);

    private final int code;

    ErrorCode(int code) {
        this.code = code;
    }

    /** The native {@code lattice_error} value. */
    public int code() {
        return code;
    }

    static ErrorCode fromCode(int code) {
        for (ErrorCode ec : values()) {
            if (ec.code == code && ec != UNKNOWN) {
                return ec;
            }
        }
        // Fall back to a catch-all so the caller still gets as much
        // diagnostic info as possible; the raw numeric code is kept on the
        // exception via LatticeException.getNativeCode().
        return UNKNOWN;
    }
}
