package org.jgroups.raft;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.raft.util.CommitTable;
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


    protected static List<Address> generate(String... members) {
        return Arrays.stream(members).map(Util::createRandomAddress).collect(Collectors.toList());
    }

}
