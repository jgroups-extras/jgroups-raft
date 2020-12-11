
## The first stage is used to git-clone and build jgroups-raft; this requires a JDK/javac/git/ant
# Build: docker build --no-cache -f Dockerfile -t belaban/jgroups-raft .
# Push: docker push belaban/jgroups-raft


FROM adoptopenjdk/openjdk11 as build-stage
RUN apt-get update ; apt-get install -y git ant net-tools netcat iputils-ping

## Download and build jgroups-raft src code
RUN git clone https://github.com/belaban/jgroups-raft.git
RUN cd jgroups-raft && ant retrieve ; ant compile

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

COPY --from=build-stage /jgroups-raft /opt/jgroups/jgroups-raft
COPY --from=build-stage /bin/ping /bin/netstat /bin/nc /bin/
COPY --from=build-stage /sbin/ifconfig /sbin/

RUN mkdir /mnt/data ; chown -R jgroups.jgroups /mnt/data $HOME/*

# Run everything below as the jgroups user. Unfortunately, USER is only observed by RUN, *not* by ADD or COPY !!
USER jgroups

RUN chmod u+x $HOME/*
CMD clear && cat $HOME/jgroups-raft/README.md && /bin/bash


