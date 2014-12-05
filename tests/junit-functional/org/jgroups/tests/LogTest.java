package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.protocols.raft.LevelDBLog;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.protocols.raft.MapDBLog;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests all {@link org.jgroups.protocols.raft.Log} implementations for correctness
 * @author Bela Ban
 * @since  0.1
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true,dataProvider="logProvider")
public class LogTest {
    protected Log log;
    protected static final String filename="raft.log";


    @DataProvider static Object[][] logProvider() {
        return new Object[][] {
          {new MapDBLog()},
          {new LevelDBLog()}
        };
    }

    @AfterMethod protected void destroy() {
        if(log != null) {
            log.delete();
            log.close();
        }
        log=null;
    }



    public void testFields(Log log) throws Exception {
        Address addr=Util.createRandomAddress("A");
        this.log=log;
        log.init(filename, null);
        log.currentTerm(22);
        int current_term=log.currentTerm();
        Assert.assertEquals(current_term, 22);

        log.votedFor(addr);
        Address voted_for=log.votedFor();
        Assert.assertEquals(addr, voted_for);

        log.close();
        log.init(filename, null);
        current_term=log.currentTerm();
        Assert.assertEquals(current_term, 22);
        voted_for=log.votedFor();
        Assert.assertEquals(addr, voted_for);

        log.delete();
        log.close();
        log.init(filename, null);
        current_term=log.currentTerm();
        assert current_term == 0;
        voted_for=log.votedFor();
        assert voted_for == null;
    }


    public void testAppendOnLeader(Log log) throws Exception {
        this.log=log;
        log.init(filename, null);
        byte[] buf=new byte[10];
        log.append(1, new LogEntry(5, buf));
        log.append(2, new LogEntry(5, buf));
        assert log.lastApplied() == 2;
        assert log.first() == 1;
        assert log.commitIndex() == 0;
    }

}
