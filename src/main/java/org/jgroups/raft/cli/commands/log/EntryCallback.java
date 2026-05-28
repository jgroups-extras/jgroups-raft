package org.jgroups.raft.cli.commands.log;

/**
 * Receives per-entry data during a sequential scan of the entries file.
 *
 * <p>
 * The entries parsing invokes this callback once for every entry it successfully parses, regardless of checksum outcome.
 * Implementations control what happens with the data.
 * </p>
 *
 * <p>
 * The {@code payload} array is the same instance held by the scanner during iteration. It is safe to read during the
 * callback invocation but must not be retained — the scanner may reuse or discard it on the next iteration.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
@FunctionalInterface
public interface EntryCallback {

    EntryCallback NOOP = (index, term, internal, dataLength, magic, status, payload) -> { };

    /**
     * Called once per parsed entry during a sequential file scan.
     *
     * @param index      the Raft log index of the entry
     * @param term       the Raft term in which the entry was proposed
     * @param internal   {@code true} if the entry carries an internal cluster-management command
     * @param dataLength the length of the entry payload in bytes
     * @param magic      the magic byte identifying the entry format ({@code 0x01} legacy, {@code 0x02} CRC)
     * @param status     the checksum verification result for this entry
     * @param payload    the raw entry payload bytes (not including header or checksum)
     */
    void onEntry(long index, long term, boolean internal, int dataLength, byte magic, CrcStatus status, byte[] payload);

    /**
     * Represents the checksum verification outcome for a single log entry.
     *
     * <p>
     * Each variant carries exactly the data needed by downstream consumers (callbacks, formatters) to render the result.
     * {@link Ok} and {@link Legacy} are singletons; {@link Mismatch} carries the two checksum values so the operator can
     * see what diverged.
     * </p>
     *
     * @since 2.0
     * @author José Bolina
     */
    sealed interface CrcStatus {

        /**
         * The stored checksum matches the computed checksum.
         */
        record Ok() implements CrcStatus {
            @Override
            public String toString() {
                return "CRC OK";
            }
        }

        /**
         * The stored checksum does not match the computed checksum.
         *
         * @param stored   the checksum value read from the file
         * @param computed the checksum value calculated over the entry header and payload
         */
        record Mismatch(int stored, int computed) implements CrcStatus {
            @Override
            public String toString() {
                return "CRC MISMATCH";
            }
        }

        /**
         * The entry uses the legacy format which has no checksum.
         */
        record Legacy() implements CrcStatus {
            @Override
            public String toString() {
                return "CRC n/a (legacy entry)";
            }
        }
    }
}
