/**
 * Contains the implementation of the JGroups Raft Command Line Interface (CLI).
 *
 * <p>
 * This package acts as the entry point for the standalone {@code raft} executable tool used to monitor and manage Raft
 * clusters. It includes the command parsing logic (Picocli), the probe communication layer, and the response formatting utilities.
 * </p>
 *
 * <p>
 * <b>Note on Visibility:</b>
 * <br>
 * This package and its sub-packages are considered <b>internal</b> implementation details of the CLI tool.
 * <ul>
 *   <li>They are <b>not</b> part of the public JGroups Raft API.</li>
 *   <li>They are <b>not</b> exposed via the Java Platform Module System (JPMS).</li>
 *   <li>They are packaged in a separate executable JAR and should not be depended upon by other applications.</li>
 * </ul>
 * </p>
 *
 * @since 2.0
 */
package org.jgroups.raft.cli;
