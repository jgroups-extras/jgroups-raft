package org.jgroups.raft.util;

import org.jgroups.Address;
import org.jgroups.View;

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
}
