package org.jgroups.raft.tests.listeners;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.lookup.StrLookup;

@Plugin(name = "testName", category = StrLookup.CATEGORY)
public class TestNameLookup implements StrLookup {

    @Override
    public String lookup(String key) {
        String testName = TestResourceTracker.getTestName();
        return testName != null ? testName : "general-execution";
    }

    @Override
    public String lookup(LogEvent event, String key) {
        return lookup(key);
    }
}
