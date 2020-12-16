## Builds an image containing jgroups-raft

## ***************************************************************
## Make sure you have jgroups-raft compiled (ant) before doing so!
## ***************************************************************

## The first stage is used to prepare/update the OS.
## The second stage copies the local files (lib:classes) to the image
# Build: docker build -f Dockerfile -t belaban/jgroups-raft .
# Push: docker push belaban/jgroups-raft


FROM adoptopenjdk/openjdk11:jre as build-stage
RUN apt-get update ; apt-get install -y git ant net-tools netcat iputils-ping

# For the runtime, we only need a JRE (smaller footprint)
FROM adoptopenjdk/openjdk11:jre
LABEL maintainer="Bela Ban (belaban@mailbox.org)"
RUN useradd --uid 1000 --home /opt/jgroups --create-home --shell /bin/bash jgroups
RUN echo root:root | chpasswd ; echo jgroups:jgroups | chpasswd
RUN printf "\njgroups ALL=(ALL) NOPASSWD: ALL\n" >> /etc/sudoers
# EXPOSE 7800-7900:7800-7900 9000-9100:9000-9100
EXPOSE 1965-1975:2065-2075 8787
ENV HOME /opt/jgroups
ENV PATH $PATH:$HOME/jgroups-raft/bin
ENV JGROUPS_RAFT_HOME=$HOME/jgroups-raft
WORKDIR /opt/jgroups

COPY --from=build-stage /bin/ping /bin/netstat /bin/nc /bin/
COPY --from=build-stage /sbin/ifconfig /sbin/
COPY  README.md $JGROUPS_RAFT_HOME/
COPY ./classes $JGROUPS_RAFT_HOME/classes
COPY ./lib $JGROUPS_RAFT_HOME/lib
COPY ./bin $JGROUPS_RAFT_HOME/bin
COPY ./conf $JGROUPS_RAFT_HOME/conf

RUN mkdir /mnt/data ; chown -R jgroups.jgroups /mnt/data $HOME/*

# Run everything below as the jgroups user. Unfortunately, USER is only observed by RUN, *not* by ADD or COPY !!
USER jgroups

RUN chmod u+x $HOME/*
CMD clear && cat $HOME/jgroups-raft/README.md && /bin/bash


