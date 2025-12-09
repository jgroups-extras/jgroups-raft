module org.jgroups.raft {
    requires java.compiler;
    requires java.xml;
    requires jdk.unsupported;

    requires static java.management;
    requires static jcip.annotations;

    // Extra optional dependency to utilize mmap files.
    requires static mashona.logwriting;

    // Optional dependency to collect metrics.
    requires static HdrHistogram;

    // Core clustering part of JGroups Raft.
    requires org.jgroups;
    requires org.infinispan.protostream.core;
    requires org.infinispan.protostream.types;

    exports org.jgroups.raft;
    exports org.jgroups.raft.command;
    exports org.jgroups.raft.configuration;
    exports org.jgroups.raft.exceptions;
    exports org.jgroups.raft.logger;
    exports org.jgroups.raft.metrics;
    exports org.jgroups.raft.blocks;
    exports org.jgroups.raft.testfwk;
    exports org.jgroups.raft.util;
    exports org.jgroups.protocols.raft;

    // Optional dependencies utilized by demonstrations in test folder.
    requires static java.desktop;
}
