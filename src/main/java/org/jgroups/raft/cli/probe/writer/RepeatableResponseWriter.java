package org.jgroups.raft.cli.probe.writer;

import org.jgroups.raft.cli.probe.ProbeResponseWriter;

import java.io.PrintWriter;
import java.time.LocalTime;
import java.util.Collection;

import picocli.CommandLine.Help.Ansi;

/**
 * A Decorator that adds "Watch Mode" capabilities to any existing response writer.
 *
 * <p>
 * This class wraps a concrete writer (like Table or JSON) and handles the visual refresh cycle required for continuous
 * monitoring ({@code -w} flag).
 * </p>
 *
 * <p>
 * <b>Behavior:</b>
 * <ul>
 *   <li><b>Interactive Terminals:</b> Clears the screen using ANSI codes {@code \033[H\033[2J} before printing, creating a static dashboard effect.</li>
 *   <li><b>Non-Interactive (Pipes/Files):</b> Appends a timestamped separator line instead of clearing, preserving the log history.</li>
 * </ul>
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
public final class RepeatableResponseWriter implements ProbeResponseWriter {
    static final String CLEAN_SCREEN = "\033[H\033[2J";
    static final Ansi ANSI = Ansi.AUTO;
    private final ProbeResponseWriter delegate;

    public RepeatableResponseWriter(ProbeResponseWriter delegate) {
        this.delegate = delegate;
    }

    @Override
    public void accept(Collection<ProbeResponse> responses) {
        // If we are in a real terminal (ANSI enabled), clear the screen
        // This approach automatically handle cases where the output is redirected, and we avoid writing the ANSI codes.
        if (ANSI.enabled())
            delegate.out().print(CLEAN_SCREEN);

        delegate.accept(responses);

        // Print a footer to indicate liveliness
        delegate.out().println(ANSI.string("@|faint,italic ────── Updated at " + LocalTime.now() + " ──────|@"));
        delegate.out().flush();
    }

    @Override
    public ProbeResponseWriterFormat format() {
        return delegate.format();
    }

    @Override
    public PrintWriter out() {
        return delegate.out();
    }
}
