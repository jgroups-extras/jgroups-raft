package org.jgroups.raft.util;

import org.jgroups.Address;
import org.jgroups.View;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.testfwk.RaftTestUtils;

import java.util.Objects;

/**
 * @author Bela Ban
 * @since  1.0.6
 */
public class Utils {
    public enum Majority {
        reached, lost, leader_lost, no_change
    }

    public static boolean majorityReached(View old, View new_view, int majority) {
        return (old == null || old.size() < majority) && new_view.size() >= majority;
    }

    public static boolean majorityLost(View old, View new_view, int majority) {
        return (old != null && old.size() >= majority) && new_view.size() < majority;
    }

    public static Majority computeMajority(View old, View new_view, int majority, Address leader) {
        if(majorityReached(old, new_view, majority))
            return Majority.reached;
        if(majorityLost(old, new_view, majority))
            return Majority.lost;
        if(leader != null && !new_view.containsMember(leader))
            return Majority.leader_lost;
        return Majority.no_change;
    }

    /**
     * Verify if the coordinator change between two views.
     *
     * @param prev: The old {@link View}, it can be <code>null</code>.
     * @param curr: The recent {@link View}.
     * @return <code>true</code> if the coordinator changed, <code>false</code>, otherwise.
     */
    public static boolean viewCoordinatorChanged(View prev, View curr) {
        if (prev == null) return true;
        return !Objects.equals(prev.getCoord(), curr.getCoord());
    }

    /**
     * Deletes the log data for the given {@link RAFT} instance.
     * <p>
     *     <b>Warning:</b> This should be used in tests only.
     * </p>
     *
     * @param r: RAFT instance to delete the log contents.
     * @throws Exception: If an exception happens while deleting the log.
     * @deprecated Use {@link RaftTestUtils#deleteRaftLog(RAFT)} instead.
     */
    @Deprecated(since = "1.0.13", forRemoval = true)
    public static void deleteLog(RAFT r) throws Exception {
        RaftTestUtils.deleteRaftLog(r);
    }

}
