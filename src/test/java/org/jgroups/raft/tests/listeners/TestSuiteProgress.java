package org.jgroups.raft.tests.listeners;

import static org.jgroups.raft.tests.listeners.Ansi.CYAN;
import static org.jgroups.raft.tests.listeners.Ansi.GREEN;
import static org.jgroups.raft.tests.listeners.Ansi.RED;
import static org.jgroups.raft.tests.listeners.Ansi.RESET;
import static org.jgroups.raft.tests.listeners.Ansi.YELLOW;

import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

final class TestSuiteProgress {

    private static final Logger LOG = LogManager.getLogger(TestSuiteProgress.class);
    private final AtomicInteger failed = new AtomicInteger(0);
    private final AtomicInteger succeeded = new AtomicInteger(0);
    private final AtomicInteger skipped = new AtomicInteger(0);
    private final PrintStream out;

    public TestSuiteProgress() {
        out = System.out;
    }

    public void started(String name) {
        String message = "Test starting: " + name;
        progress(message);
        LOG.info(message);
    }

    public void succeeded(String name) {
        succeeded.incrementAndGet();
        String message = "Test succeeded: " + name;
        progress(GREEN, message);
        LOG.info(message);
    }

    public void failed(String name, Throwable t) {
        failed.incrementAndGet();
        String message = "Test failed: " + name;
        progress(RED, message, t);
        LOG.error(message, t);
    }

    public void ignored(String name) {
        skipped.incrementAndGet();
        String message = "Test ignored: " + name;
        progress(YELLOW, message);
        LOG.info(message);
    }

    public void configurationStarted(String name) {
        LOG.debug("Test configuration started: {}", name);
    }

    public void configurationFinished(String name) {
        LOG.debug("Test configuration finished: {}", name);
    }

    public void configurationFailed(String name, Throwable t) {
        failed.incrementAndGet();
        String message = "Test configuration failed: " + name;
        progress(RED, message, t);
        LOG.error(message, t);
    }

    private void progress(CharSequence message) {
        progress(null, message);
    }

    private void progress(String color, CharSequence message) {
        progress(color, message, null);
    }

    private void progress(String color, CharSequence message, Throwable t) {
        String actualColor = "";
        String reset = "";
        String okColor = "";
        String koColor = "";
        String skipColor = "";
        if (Ansi.useColor && color != null) {
            actualColor = color;
            reset = RESET;
            if (succeeded.get() > 0) {
                okColor = GREEN;
            }
            if (failed.get() > 0) {
                okColor = RED;
            }
            if (skipped.get() > 0) {
                skipColor = CYAN;
            }
        }
        // Must format explicitly, see SUREFIRE-1814
        out.println(String.format("[%sOK: %5s%s, %sKO: %5s%s, %sSKIP: %5s%s] %s%s%s", okColor, succeeded.get(), reset, koColor, failed.get(), reset, skipColor, skipped.get(), reset, actualColor, message, reset));
        if (t != null) {
            t.printStackTrace(out);
        }
    }
}
