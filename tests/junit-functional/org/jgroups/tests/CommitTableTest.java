package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.CommitTable;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Bela Ban
 * @since  0.1
 */
@Test(groups=Global.FUNCTIONAL)
public class CommitTableTest {
    protected static final Address a=Util.createRandomAddress("A"), b=Util.createRandomAddress("B"),
      c=Util.createRandomAddress("C"), d=Util.createRandomAddress("D"), e=Util.createRandomAddress("E");

    public void testSnapshotInProgress() {
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


    public void testAddition() {
        CommitTable table=new CommitTable(Arrays.asList(a,b,c), 5);
        System.out.println("table = " + table);
        assert table.keys().size() == 3;

        List<Address> mbrs=Arrays.asList(b, c, d, e);
        table.adjust(mbrs, 5);
        System.out.println("table = " + table);
        assert table.keys().size() == 4;
        Set<Address> keys=table.keys();
        assert keys.equals(new HashSet<>(mbrs));
    }


    protected List<Address> generate(String ... members) {
        return Arrays.stream(members).map(Util::createRandomAddress).collect(Collectors.toList());
    }

}
