package org.jgroups.raft.tests.listeners;

final class TestResourceTracker {

    // InheritableThreadLocal ensures child threads (like JGroups timers)
    // automatically inherit the test name from the main test thread.
    private static final InheritableThreadLocal<String> currentTestName = new InheritableThreadLocal<>();

    public static void setTestName(String name) {
        currentTestName.set(name);
    }

    public static String getTestName() {
        return currentTestName.get();
    }

    public static void clear() {
        currentTestName.remove();
    }
}
