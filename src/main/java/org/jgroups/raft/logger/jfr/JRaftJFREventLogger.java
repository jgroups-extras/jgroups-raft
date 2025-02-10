package org.jgroups.raft.logger.jfr;

import org.jgroups.raft.logger.JRaftEventLogger;

import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class JRaftJFREventLogger implements JRaftEventLogger {

    @Override
    public void logEvent(ClusterEvent event) {
        // https://www.morling.dev/blog/rest-api-monitoring-with-custom-jdk-flight-recorder-events/
        // https://bell-sw.com/announcements/2021/01/29/JDK-Flight-Recorder-The-Programmatic-Way/
        // https://docs.oracle.com/en/java/javase/17/jfapi/categories.html
    }
}
