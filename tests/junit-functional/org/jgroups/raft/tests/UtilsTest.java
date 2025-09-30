package org.jgroups.raft.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.raft.util.Utils;
import org.jgroups.raft.util.Utils.Majority;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import static org.jgroups.raft.util.Utils.Majority.*;

/**
 * @author Bela Ban
 * @since  1.0.6
 */
@Test(groups=Global.FUNCTIONAL)
public class UtilsTest {
    protected static final Address a=Util.createRandomAddress("A"),
      b=Util.createRandomAddress("B"),c=Util.createRandomAddress("C"),
      d=Util.createRandomAddress("D"),e=Util.createRandomAddress("E");
    protected static final int MAJORITY=3;

    public void testMajority() {
        View old=null, new_view=create(a);
        _test(old, new_view, false, false);

        old=new_view;
        new_view=create(a,b);
        _test(old, new_view, false, false);

        old=new_view;
        new_view=create(a,b,c);
        _test(old, new_view, true, false);

        old=new_view;
        new_view=create(a,b,c,d);
        _test(old, new_view, false, false);

        old=new_view;
        new_view=create(a,b,c,d,e);
        _test(old, new_view, false, false);

        old=new_view;
        new_view=create(a,b,c,d);
        _test(old, new_view, false, false);

        old=new_view;
        new_view=create(a,b,c);
        _test(old, new_view, false, false);

        old=new_view;
        new_view=create(a,b);
        _test(old, new_view, false, true);

        old=new_view;
        new_view=create(a);
        _test(old, new_view, false, false);

        old=new_view;
        new_view=create(a,b,c,d);
        _test(old, new_view, true, false);

        old=new_view;
        new_view=create(d);
        _test(old, new_view, false, true);
    }

    public void testComputeMajority() {
        View old=null, new_view=create(a);
        _test(old, new_view, no_change);

        old=new_view;
        new_view=create(a,b);
        _test(old, new_view, no_change);

        old=new_view;
        new_view=create(a,b,c);
        _test(old, new_view, reached);

        old=new_view;
        new_view=create(a,b,c,d);
        _test(old, new_view, no_change);

        old=new_view;
        new_view=create(a,b,c,d,e);
        _test(old, new_view, no_change);

        old=new_view;
        new_view=create(a,b,c,d);
        _test(old, new_view, no_change);

        old=new_view;
        new_view=create(a,b,c);
        _test(old, new_view, no_change);

        old=new_view;
        new_view=create(a,b);
        _test(old, new_view, lost);

        old=new_view;
        new_view=create(a);
        _test(old, new_view, no_change);

        old=new_view;
        new_view=create(a,b,c,d);
        _test(old, new_view, reached);

        old=new_view;
        new_view=create(b,c,d);
        _test(old, new_view, leader_lost);

        old=new_view;
        new_view=create(d);
        _test(old, new_view, lost);

        old=null;
        new_view=create(a,b,c,d);
        _test(old, new_view, reached);
    }

    protected static void _test(View old, View new_view, Majority expected) {
        Majority result=Utils.computeMajority(old, new_view, MAJORITY, old != null? old.getCoord() : null);
        System.out.printf("old: %s, new: %s, result: %s\n", old, new_view, result);
        assert result == expected;
    }

    protected static void _test(View old, View new_view, boolean majority_reached, boolean majority_lost) {
        boolean maj_reached=Utils.majorityReached(old, new_view, MAJORITY);
        boolean maj_lost=Utils.majorityLost(old, new_view, MAJORITY);
        System.out.printf("old view: %s, new view: %s, majority reached: %b, majority lost: %b\n",
                          old, new_view, maj_reached, maj_lost);
        assert maj_reached == majority_reached;
        assert maj_lost == majority_lost;
    }

    protected static View create(Address... mbrs) {
        return View.create(mbrs[0], 1, mbrs);
    }
}
