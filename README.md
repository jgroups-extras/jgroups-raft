jgroups-raft
============

[![Last Build](https://img.shields.io/github/actions/workflow/status/jgroups-extras/jgroups-raft/maven.yml?style=for-the-badge&logo=github)](https://github.com/jgroups-extras/jgroups-raft/actions/workflows/maven.yml)
[![Maven Central](https://img.shields.io/maven-central/v/org.jgroups/jgroups-raft?style=for-the-badge&logo=apache-maven&color=green)](https://central.sonatype.com/artifact/org.jgroups/jgroups-raft)
[![License](https://img.shields.io/github/license/jgroups-extras/jgroups-raft?style=for-the-badge&logo=apache&color=green)](https://www.apache.org/licenses/LICENSE-2.0)

jgroups-raft is an implementation of the [Raft](https://raft.github.io/) consensus algorithm in [JGroups](http://jgroups.org/).
Users can use jgroups-raft embedded in their applications to build strongly consistent, highly available, fault-tolerant systems.


## Overview
jgroups-raft is a library offering all the guarantees the Raft algorithm provides, with features including:

* Configurable and alternatives for leader election;
* Dynamic membership changes in single-step;
* Configurable log implementations for storage;
* Member deduplication and request redirection;
* Ready-to-use building blocks.

By building on top of JGroups, jgroups-raft takes advantage of additional features with a mature and battle-tested network stack.
jgroups-raft is [verified with Jepsen](https://github.com/jgroups-extras/jepsen-jgroups-raft) to identify linearizability violations in the building blocks.


## Getting Started

To get started developing with jgroups-raft:

* Take a look at the complete [documentation](https://belaban.github.io/jgroups-raft/manual/index.html);
* Details about the implementation are available in the [design documents](https://github.com/jgroups-extras/jgroups-raft/tree/master/doc/design).


## Contributing

* Get in touch through the [discussion group](https://groups.google.com/forum/#!forum/jgroups-raft) or [GitHub discussions](https://github.com/jgroups-extras/jgroups-raft/discussions);
* Open bug reports on [GitHub issues](https://github.com/jgroups-extras/jgroups-raft/issues);
* Feel like coding? Look at the issues page and get in touch with any questions.
