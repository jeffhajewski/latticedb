package io.latticedb;

/**
 * The changefeed no longer holds the events after the position read from:
 * its retention policy, or a trim, dropped them, so the reader has missed
 * changes. Reading from sequence 0 starts at the oldest event held. Its code
 * is {@link ErrorCode#NOT_FOUND}.
 */
public class ChangesTrimmedException extends LatticeException {
    ChangesTrimmedException(long afterSequence) {
        super(ErrorCode.NOT_FOUND, "the changefeed no longer holds the events after sequence "
                + Long.toUnsignedString(afterSequence) + ": its retention policy or a trim dropped them");
    }
}
