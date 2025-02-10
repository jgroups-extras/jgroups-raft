package org.jgroups.raft.logger;

import java.time.Instant;
import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import net.jcip.annotations.ThreadSafe;

/**
 * Event logger for important events in the Raft cluster.
 *
 * <p>
 * This interface provides methods to log and search for events within the Raft cluster. It is designed for use by the Raft
 * algorithm to record significant events during its operation, including those initiated by application requests. These logs aim
 * to offer insights into the cluster's behavior, aiding in debugging and monitoring. By default, this interface uses a no-op
 * implementation, allowing users to implement their own event logger if necessary.
 * </p>
 *
 * <h2>Events</h2>
 *
 * <p>
 * Implementations of this interface must be thread-safe as they may be accessed by multiple threads simultaneously. Internal events
 * logged by the Raft algorithm or application requests will be handled here. Applications do not need to log events; instead,
 * the primary method of interest is querying these logs.
 * </p>
 *
 * <h2>Search</h2>
 *
 * <p>
 * The search method enables searching for events based on specific criteria, such as a time range and attributes. Note that
 * implementations exposing events to third-party software may not need this method. This search functionality is intended solely
 * for user-facing applications, such as building dashboards.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 */
@ThreadSafe
public interface JRaftEventLogger {

    /**
     * Publish a new event that happened.
     *
     * <p>
     * This method is utilized to log events that occur within the Raft cluster. The implementation <b>must</b> be thread-safe
     * as it may be accessed by multiple threads simultaneously. The event should be logged in a way that allows for recovery
     * and analysis later.
     * </p>
     *
     * @param event The event to publish.
     */
    void logEvent(ClusterEvent event);

    /**
     * Search for events that match the given criteria.
     *
     * <p>
     * This method allows searching for events based on specific criteria, such as a time range and attributes. This implementation
     * is not required to all implementors.
     * </p>
     *
     * @param criteria The search criteria to match events against.
     * @return A stream of events that match the given criteria.
     */
    default Stream<ClusterEvent> search(SearchCriteria criteria) {
        return Stream.empty();
    }

    /**
     * Factory method to create a new event.
     *
     * @param eventId Identifies the event.
     * @param type The type of event.
     * @param timestamp The timestamp of the event.
     * @param details Additional details about the event.
     * @return A new instance of {@link ClusterEvent}.
     */
    static ClusterEvent create(String eventId, EventType type, Instant timestamp, Map<String, String> details) {
        return new ClusterEvent(eventId, type, timestamp, details);
    }

    /**
     * Factory method to create a new event with a generated ID and current timestamp.
     *
     * @param type The type of event.
     * @param details Additional details about the event.
     * @return A new instance of {@link ClusterEvent} with a generated ID and current timestamp.
     */
    static ClusterEvent create(EventType type, Map<String, String> details) {
        return create(UUID.randomUUID().toString(), type, Instant.now(), details);
    }

    record ClusterEvent(String eventId, EventType type, Instant timestamp, Map<String, String> details) { }

    /**
     * Represents the type of event that can be logged.
     *
     * <p>
     * These types are used to categorize events within the Raft cluster. They can be seen in two categories:
     *
     * <ul>
     *     <li>Raft internal: such as log compaction, leader election, etc.</li>
     *     <li>Reconfiguration: node added or removed from the core member list.</li>
     * </ul>
     * </p>
     */
    enum EventType {
        LEADER_ELECTION,
        NODE_JOINED,
        NODE_LEFT,
        SPLIT_BRAIN,
        LOG_COMPACTION,
        SNAPSHOT_CREATE,
        CONFIGURATION_CHANGE,
        ROLE_CHANGE,
    }

    record SearchCriteria(Instant start, Instant end, EnumSet<EventType> types, String raftId) { }

    static JRaftEventLogger disabled() {
        return new DisabledEventLogger();
    }

    final class DisabledEventLogger implements JRaftEventLogger {

        private DisabledEventLogger() { }

        @Override
        public void logEvent(ClusterEvent event) { }
    }
}
