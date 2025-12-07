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
    exports org.jgroups.raft.blocks;
    exports org.jgroups.raft.util;
    exports org.jgroups.protocols.raft;
}
