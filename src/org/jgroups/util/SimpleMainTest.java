package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.protocols.raft.LevelDBLog;
import org.jgroups.protocols.raft.LogEntry;


/**
 * Created by ugol on 06/12/14.
 */
public class SimpleMainTest {

    public static void main(String[] args) throws Exception {

        LevelDBLog log = new LevelDBLog();
        Address addr=Util.createRandomAddress("A");
        log.init("foo.log", null);
        log.append(1, true, new LogEntry(1, "UGO LANDINI 1".getBytes()));
        log.append(2, true, new LogEntry(2, "UGO LANDINI 2".getBytes()));
        log.append(3, true, new LogEntry(3, "UGO LANDINI 3".getBytes()));
        log.append(3, true, new LogEntry(3, "UGO LANDINI 3bis".getBytes()));
        log.forEach(null, 0, 3);

        log.printMetadata();
        log.close();
        log.delete();
    }

}