package org.jgroups.raft.cli.commands.log;

/**
 * Immutable configuration passed through the validation rule chain.
 *
 * <p>
 * Carries operator-selected options that affect how rules produce output without changing what they validate. Rules that
 * do not consume any option simply ignore it. The {@link #DEFAULT} instance uses {@link EntryCallback#NOOP}.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
public final class LogValidationOptions {
    private static final LogValidationOptions DEFAULT = new LogValidationOptions(EntryCallback.NOOP);

    private final EntryCallback callback;

    private LogValidationOptions(EntryCallback callback) {
        this.callback = callback;
    }

    /**
     * Returns a new instance with the given entry callback replacing the current one.
     *
     * @param callback the callback to invoke for each parsed entry during the entries file scan
     * @return a new {@code ValidationOptions} with the updated callback
     */
    public static LogValidationOptions withCallback(EntryCallback callback) {
        return new LogValidationOptions(callback);
    }

    /**
     * Returns the default validation options.
     *
     * @return Default validation options.
     */
    public static LogValidationOptions simple() {
        return DEFAULT;
    }

    EntryCallback callback() {
        return callback;
    }
}
