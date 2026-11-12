package io.latticedb;

/**
 * The changefeed's retention policy and the events it holds.
 *
 * @param retention the policy in force, in the form
 *     {@link OpenOptions#changefeedRetention(String)} takes
 * @param oldestSequence the oldest event held, 0 when the changefeed holds none
 * @param lastSequence the last sequence handed to an event, 0 if none ever was
 */
public record ChangefeedStatus(String retention, long oldestSequence, long lastSequence) {
}
