package io.latticedb;

/**
 * Runtime exception carrying a native {@code lattice_error} code.
 *
 * Thrown by all LatticeDB operations that fail at the native layer.
 */
public class LatticeException extends RuntimeException {
    private final ErrorCode errorCode;
    private final int nativeCode;

    public LatticeException(ErrorCode errorCode, String message) {
        this(errorCode, errorCode.code(), message);
    }

    LatticeException(ErrorCode errorCode, int nativeCode, String message) {
        super(message);
        this.errorCode = errorCode;
        this.nativeCode = nativeCode;
    }

    LatticeException(int rawCode, String message) {
        this(ErrorCode.fromCode(rawCode), rawCode,
                ErrorCode.fromCode(rawCode) == ErrorCode.UNKNOWN
                        ? message + " (native error code " + rawCode + ")"
                        : message);
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    /**
     * Runs a call that needs the vector index. On a database created without
     * one it fails with {@link ErrorCode#UNSUPPORTED} ({@code IO} before
     * 0.18.1), and the library's text for that code does not say why.
     */
    static <T> T needingVectorIndex(String action, java.util.function.Supplier<T> call) {
        try {
            return call.get();
        } catch (LatticeException e) {
            if (e.getErrorCode() == ErrorCode.UNSUPPORTED) {
                throw new LatticeException(ErrorCode.UNSUPPORTED, "cannot " + action
                        + ": the database was created without a vector index; open it with "
                        + "enableVectors and vectorDimensions to add one");
            }
            throw e;
        }
    }

    /** The raw numeric {@code lattice_error} value behind this failure.
     * For unrecognized codes this preserves diagnostics that the enum
     * cannot express. */
    public int getNativeCode() {
        return nativeCode;
    }
}
