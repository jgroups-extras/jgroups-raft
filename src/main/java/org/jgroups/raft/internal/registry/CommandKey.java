package org.jgroups.raft.internal.registry;

/**
 * Composite key identifying a unique command in the state machine by its id and version.
 *
 * @param id      the command identifier
 * @param version the command version
 * @since 2.0
 * @author José Bolina
 */
record CommandKey(int id, int version) { }
