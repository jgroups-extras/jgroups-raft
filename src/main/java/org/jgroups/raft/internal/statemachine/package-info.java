/**
 * Provides the high-level abstraction layer that bridges user-defined Java APIs with the JGroups Raft consensus protocol.
 *
 * <p>
 * The primary goal of this package is to hide the complexity of distributed consensus, serialization, and log compaction
 * from the end-user. It allows developers to define their distributed state machines using standard, strongly-typed Java
 * interfaces and concrete implementations, rather than interacting directly with raw byte arrays and the low-level Raft
 * protocol mechanics.
 * </p>
 *
 * <h2>Key Objectives</h2>
 *
 * <ul>
 *   <li><b>Transparent Replication:</b> To seamlessly translate standard local method invocations into replicated commands,
 *       guaranteeing distributed consensus across the Raft cluster before applying state changes.</li>
 *
 *   <li><b>Automated State Management:</b> To automatically handle the persistence and restoration of the state machine's
 *       internal state, facilitating efficient Raft log compaction and seamless state transfers to new or lagging nodes.</li>
 *
 *   <li><b>Concurrency Control:</b> To provide a safe and consistent execution environment that balances the strict,
 *       sequential application of committed Raft writes with the ability to perform fast, concurrent non-linearizable reads.</li>
 * </ul>
 *
 * <p>
 * By utilizing the components in this package, we can focus entirely on their application's business logic and state
 * transitions, relying on the underlying infrastructure to guarantee consistency, replication, and fault tolerance.
 * </p>
 *
 * @since 2.0
 * @author José Bolina
 * @see org.jgroups.raft.StateMachine
 * @see org.jgroups.raft.StateMachineField
 * @see org.jgroups.raft.StateMachineWrite
 * @see org.jgroups.raft.StateMachineRead
 */
package org.jgroups.raft.internal.statemachine;
