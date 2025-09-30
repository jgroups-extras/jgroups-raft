module org.jgroups.raft {
   requires java.xml;
   requires static jcip.annotations;
   requires mashona.logwriting;
   requires org.jgroups;
   requires jdk.unsupported;

   requires static java.management;

   exports org.jgroups.raft;
   exports org.jgroups.protocols.raft;
}
