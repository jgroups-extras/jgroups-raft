package org.jgroups.protocols.raft;

/**
 * Marker interface for optional capabilities that a {@link Log} implementation may provide.
 *
 * <p>
 * Capabilities allow consumers to discover and use features beyond the core {@link Log} contract without coupling to
 * specific implementations. A capability is obtained via {@link Log#findCapability(Class)}.
 * </p>
 *
 * @since 2.0
 * @see Log#findCapability(Class)
 * @see LogCacheControl
 */
public interface LogCapability { }
