## Builds an image containing jgroups-raft

## ***************************************************************
## Make sure you have jgroups-raft compiled (mvn clean package) before doing so!
## ***************************************************************
##
## Build: docker build -f etc/Dockerfile -t belaban/jgroups-raft .
## Push:  docker push belaban/jgroups-raft

FROM eclipse-temurin:21-jre
LABEL maintainer="Bela Ban (belaban@mailbox.org)"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       net-tools \
       netcat-openbsd \
       iputils-ping \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --home /opt/jgroups --create-home --shell /bin/bash jgroups
RUN echo root:root | chpasswd ; echo jgroups:jgroups | chpasswd
RUN printf "\njgroups ALL=(ALL) NOPASSWD: ALL\n" >> /etc/sudoers

EXPOSE 1965-1975 2065-2075 8787
ENV HOME=/opt/jgroups
ENV TERM=xterm
ENV PATH=$PATH:$HOME/jgroups-raft/bin
ENV JGROUPS_RAFT_HOME=$HOME/jgroups-raft
WORKDIR /opt/jgroups/jgroups-raft

COPY ../README.md                   $JGROUPS_RAFT_HOME/
COPY ../target/classes            $JGROUPS_RAFT_HOME/target/classes
COPY ../target/test-classes       $JGROUPS_RAFT_HOME/target/test-classes
COPY ../target/libs               $JGROUPS_RAFT_HOME/target/libs
COPY ../bin                       $JGROUPS_RAFT_HOME/bin

RUN mkdir /mnt/data \
    && chown -R jgroups:jgroups /mnt/data $HOME

# Run everything below as the jgroups user
USER jgroups

RUN chmod u+x $HOME/jgroups-raft/bin/*.sh $HOME/jgroups-raft/bin/raft
CMD ["bash", "-c", "clear && cat $HOME/jgroups-raft/README.md && /bin/bash"]
