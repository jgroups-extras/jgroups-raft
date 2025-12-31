package org.jgroups.raft;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.util.Utils.Majority.leader_lost;
import static org.jgroups.raft.util.Utils.Majority.lost;
import static org.jgroups.raft.util.Utils.Majority.no_change;
import static org.jgroups.raft.util.Utils.Majority.reached;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.raft.util.Utils;
import org.jgroups.raft.util.Utils.Majority;
import org.jgroups.util.Util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Test;

/**
 * @author Bela Ban
 * @since  1.0.6
 */
@Test(groups=Global.FUNCTIONAL)
public class UtilsTest {
    private static final Logger LOGGER = LogManager.getLogger(UtilsTest.class);

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
        LOGGER.info("old: {}, new: {}, result: {}", old, new_view, result);
        assertThat(result).isEqualTo(expected);
    }

    protected static void _test(View old, View new_view, boolean majority_reached, boolean majority_lost) {
        boolean maj_reached=Utils.majorityReached(old, new_view, MAJORITY);
        boolean maj_lost=Utils.majorityLost(old, new_view, MAJORITY);
        LOGGER.info("old view: {}, new view: {}, majority reached: {}, majority lost: {}",
                          old, new_view, maj_reached, maj_lost);
        assertThat(maj_reached).isEqualTo(majority_reached);
        assertThat(maj_lost).isEqualTo(majority_lost);
    }

    protected static View create(Address... mbrs) {
        return View.create(mbrs[0], 1, mbrs);
    }
}
