# Pull Request Reviewing Instructions

## What Important means here
Reserve Important for findings that break correctness or compatibility: violated Raft safety rules,
lost or corrupted log data, stalls of the event loop, and incompatible changes after 2.0.0.Final.
Style, naming and refactoring suggestions are Nit at most.

## JGroups Raft concerns

- **Raft as implemented here.** Verify the safety rules hold:
  - term and vote are persisted before responding;
  - a leader only commits entries from its own term;
  - append and truncate preserve log matching;
  - uncommitted entries are never applied to the state machine;
  - linearizable reads are confirmed before answering.

  Equally, respect the deliberate differences from the paper. JGroups handles failure detection
  and membership, elections start from view changes, and there are no heartbeats, candidates or
  randomized election timeouts. Do not suggest adding them.
- **Stalling the event loop.** The core algorithm runs on a single thread, so anything it waits on
  delays replication for every client. Log I/O on the loop is expected. Flag waiting on futures,
  remote replies or locks held by other threads, sleeps or retry loops, and work that grows with
  the log size where it could be incremental. State owned by the loop must not be mutated from
  other threads.
- **Durability.** Log writes respect the configured fsync policy: when fsync is enabled, entries are
  synced to disk before they are acknowledged.
- **Compatibility.** Changes to `RaftHeader`, request/response messages, the log or snapshot formats,
  and user state machine annotations (`@StateMachineWrite` id/version, `@StateMachineField` order).
  Before 2.0.0.Final, mention breaks as a Nit so they are visible; from 2.0.0.Final on, they are Important.
- **Public API (`org.jgroups.raft`).** New public types or members must be intentional; prefer
  package-private, `final` and `sealed`.

## Documentation and design
Check whether the change affects the manual (`src/docs/manual/`), the CLI reference (`src/docs/cli/`)
or a design document (`src/docs/design/`). If an affected document was not updated, report it and name
the document. This finding does not count toward the Nit cap. Do not report missing docs for internal
refactorings that change neither behavior nor design.

## Cap the nits
Report at most five Nits per review. If you found more, say "plus N similar items" in the summary.
If everything you found is a Nit, lead the summary with "No blocking issues."

## Do not report
- Anything CI already enforces: checkstyle (`etc/checkstyle.xml`), spotbugs, pmd.
- Missing volatile or atomics on metric counters. Metrics are deliberately relaxed and tests read them with `eventually`.
- Test-only code that intentionally violates production rules.
- snake_case in existing code that the PR did not introduce.
