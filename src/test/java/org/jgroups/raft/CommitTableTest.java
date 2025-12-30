package org.jgroups.raft;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.raft.util.CommitTable;
import org.jgroups.util.Util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Test;

/**
 * @author Bela Ban
 * @since  0.1
 */
@Test(groups=Global.FUNCTIONAL)
public class CommitTableTest {
    private static final Logger LOGGER = LogManager.getLogger(CommitTableTest.class);

    protected static final Address a=Util.createRandomAddress("A"), b=Util.createRandomAddress("B"),
      c=Util.createRandomAddress("C"), d=Util.createRandomAddress("D"), e=Util.createRandomAddress("E");


    public void testAddition() {
        CommitTable table=new CommitTable(Arrays.asList(a,b,c), 5);
        LOGGER.info("table = {}", table);
        assert table.keys().size() == 3;

        List<Address> mbrs=Arrays.asList(b, c, d, e);
        table.adjust(mbrs, 5);
        LOGGER.info("table = {}", table);
        assert table.keys().size() == 4;
        Set<Address> keys=table.keys();
        assert keys.equals(new HashSet<>(mbrs));
    }
}
