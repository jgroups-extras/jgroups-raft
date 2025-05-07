package org.jgroups.raft.util;

import org.jgroups.Address;
import org.jgroups.View;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.testfwk.RaftTestUtils;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.Util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

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

    public static boolean isMajorityReached(Collection<String> oldMembers, Collection<String> newMembers, int majority) {
        return (oldMembers == null || oldMembers.size() < majority) && newMembers.size() >= majority;
    }

    public static boolean isMajorityLost(Collection<String> oldMembers, Collection<String> newMembers, int majority) {
        return (oldMembers != null && oldMembers.size() >= majority) && newMembers.size() < majority;
    }

    public static Majority computeMajority(View oldView, View newView, RAFT raft) {
        Collection<String> allRaftMembers = raft.members();
        Collection<String> oldMembers = convertJGroupsMemberToRaftMember(oldView, allRaftMembers);
        Collection<String> newMembers = convertJGroupsMemberToRaftMember(newView, allRaftMembers);

        if (isMajorityReached(oldMembers, newMembers, raft.majority()))
            return Majority.reached;

        if (isMajorityLost(oldMembers, newMembers, raft.majority()))
            return Majority.lost;

        if (raft.leader() != null && !newView.containsMember(raft.leader()))
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

    /**
     * Verifies if the given raft ID belongs to the list of members.
     *
     * @param raftId The member to verify.
     * @param raftMembers The list of members.
     * @return <code>true</code> if the member is a Raft member, <code>false</code>, otherwise.
     */
    public static boolean isRaftMember(String raftId, Collection<String> raftMembers) {
        return raftMembers.contains(raftId);
    }

    private static Collection<String> convertJGroupsMemberToRaftMember(View view, Collection<String> raftMembers) {
        if (view == null)
            return null;

        return Arrays.stream(view.getMembersRaw())
                .map(Utils::extractRaftId)
                .filter(raftMembers::contains)
                .collect(Collectors.toSet());
    }

    public static String extractRaftId(Address address) {
        if (!(address instanceof ExtendedUUID))
            throw new IllegalStateException("Address is not ExtendedUUID");

        ExtendedUUID uuid = (ExtendedUUID) address;
        return Util.bytesToString(uuid.get(RAFT.raft_id_key));
    }
}
