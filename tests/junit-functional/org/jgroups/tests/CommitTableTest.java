package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.CommitTable;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.Arrays;

/**
 * @author Bela Ban
 * @since  0.1
 */
@Test(groups=Global.FUNCTIONAL)
public class CommitTableTest {

    public void testSnapshotInProgress() {
        Address a=Util.createRandomAddress("A"), b=Util.createRandomAddress("B");
        CommitTable tab=new CommitTable(Arrays.asList(a, b), 5);
        boolean flag=tab.snapshotInProgress(a, true);
        assert flag;

        flag=tab.snapshotInProgress(a,true);
        assert !flag;

        flag=tab.snapshotInProgress(a,false);
        assert flag;

        flag=tab.snapshotInProgress(a,false);
        assert !flag;
    }

}
