package org.jgroups.perf.counter;

import org.HdrHistogram.AbstractHistogram;
import org.HdrHistogram.AtomicHistogram;
import org.HdrHistogram.Histogram;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

/**
 * Utility methods to create histograms and write the logs to file
 */
public enum HistogramUtil {
    ;

    // highest latency observable
    private static final long HIGHEST_TRACKABLE_VALUE = TimeUnit.MINUTES.toNanos(1);
    // precision between 0 and 5
    private static final int PRECISION = 3;

    public static Histogram create() {
        return new Histogram(HIGHEST_TRACKABLE_VALUE, PRECISION);
    }

    public static AtomicHistogram createAtomic() {
        return new AtomicHistogram(HIGHEST_TRACKABLE_VALUE, PRECISION);
    }

    public static void writeTo(AbstractHistogram histogram, File file) throws IOException {
        try (FileOutputStream out = new FileOutputStream(file)) {
            //change scale from nanos -> micros
            histogram.outputPercentileDistribution(new PrintStream(out), 1000.0);
        }
    }
}
