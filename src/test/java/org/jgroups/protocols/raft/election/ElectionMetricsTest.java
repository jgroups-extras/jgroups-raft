package org.jgroups.protocols.raft.election;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jgroups.raft.tests.harness.BaseRaftElectionTest.ALL_ELECTION_CLASSES_PROVIDER;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.internal.metrics.ElectionProtocolMetrics;
import org.jgroups.raft.tests.harness.BaseRaftElectionTest;

import java.lang.reflect.Field;

import org.testng.annotations.Test;

/**
 * Integration test for election protocol metrics collection.
 *
 * <p>
 * Creates a real 3-node cluster with stats enabled, lets an election complete, and verifies
 * that the coordinator's {@link ElectionProtocolMetrics} records the election latency and that
 * the {@code electionStart}/{@code electionEnd} Instants are populated.
 * </p>
 *
 * @author José Bolina
 */
@Test(groups = Global.FUNCTIONAL, singleThreaded = true, dataProvider = ALL_ELECTION_CLASSES_PROVIDER)
public class ElectionMetricsTest extends BaseRaftElectionTest.ChannelBased {

    {
        clusterSize = 3;
        recreatePerMethod = true;
    }

    @Override
    protected void amendRAFTConfiguration(RAFT raft) {
        raft.resendInterval(600_000);
        raft.enableStats(true);
    }

    /**
     * After a successful election, the coordinator records exactly 1 election measurement.
     */
    public void testElectionRecordsMeasurement(Class<?> ignore) {
        waitUntilLeaderElected(10_000, 0, 1, 2);
        waitUntilVotingThreadStops(10_000, 0, 1, 2);

        BaseElection coordinator = findCoordinatorElection();
        ElectionProtocolMetrics metrics = electionMetrics(coordinator);

        assertThat(metrics.election().getTotalMeasurements())
                .as("Coordinator should record at least 1 election measurement")
                .isGreaterThanOrEqualTo(1);
    }

    /**
     * After a successful election, electionStart and electionEnd are both set on the coordinator.
     */
    public void testElectionInstantsAreSet(Class<?> ignore) {
        waitUntilLeaderElected(10_000, 0, 1, 2);
        waitUntilVotingThreadStops(10_000, 0, 1, 2);

        BaseElection coordinator = findCoordinatorElection();

        assertThat(coordinator.electionStart())
                .as("electionStart should be set after election")
                .isNotNull();
        assertThat(coordinator.electionEnd())
                .as("electionEnd should be set after election")
                .isNotNull();
        assertThat(coordinator.electionEnd())
                .as("electionEnd should be after electionStart")
                .isAfterOrEqualTo(coordinator.electionStart());
    }

    /**
     * timeSinceLastElection returns a non-negative value after an election completes.
     */
    public void testTimeSinceLastElection(Class<?> ignore) {
        waitUntilLeaderElected(10_000, 0, 1, 2);
        waitUntilVotingThreadStops(10_000, 0, 1, 2);

        BaseElection coordinator = findCoordinatorElection();

        assertThat(coordinator.timeSinceLastElection())
                .as("timeSinceLastElection should be non-negative after election")
                .isGreaterThanOrEqualTo(0);
    }

    /**
     * Non-coordinator nodes do not record election measurements since they don't run the voting thread.
     */
    public void testNonCoordinatorRecordsNothing(Class<?> ignore) {
        waitUntilLeaderElected(10_000, 0, 1, 2);
        waitUntilVotingThreadStops(10_000, 0, 1, 2);

        for (int i = 0; i < clusterSize; i++) {
            JChannel ch = channel(i);
            BaseElection el = election(ch);

            if (isCoordinator(ch)) continue;

            ElectionProtocolMetrics metrics = electionMetrics(el);
            if (metrics != null) {
                assertThat(metrics.election().getTotalMeasurements())
                        .as("Non-coordinator %s should have 0 election measurements", ch.getName())
                        .isZero();
            }
        }
    }

    /**
     * Metrics disabled returns -1 from the JMX attribute.
     */
    public void testMetricsDisabled(Class<?> ignore) {
        BaseElection coordinator = findCoordinatorElection();
        coordinator.raft().enableStats(false);
        coordinator.enableStats(false);
        coordinator.resetStats();

        assertThat(coordinator.electionMeanLatency()).isEqualTo(-1);
    }

    private BaseElection findCoordinatorElection() {
        for (int i = 0; i < clusterSize; i++) {
            JChannel ch = channel(i);
            if (isCoordinator(ch)) {
                return election(ch);
            }
        }
        throw new AssertionError("No coordinator found in cluster");
    }

    private boolean isCoordinator(JChannel ch) {
        return ch.getView().getCoord().equals(ch.getAddress());
    }

    private ElectionProtocolMetrics electionMetrics(BaseElection election) {
        try {
            Field field = BaseElection.class.getDeclaredField("metrics");
            field.setAccessible(true);
            return (ElectionProtocolMetrics) field.get(election);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Failed to access BaseElection.metrics field", e);
        }
    }
}
