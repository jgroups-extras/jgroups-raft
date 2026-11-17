# Project Context: JGroups Raft

An implementation of the Raft consensus algorithm on top of JGroups. Java 17, Maven, TestNG.

## Layout
- `org.jgroups.protocols.raft` -- the protocol layer (RAFT, ELECTION, REDIRECT, ...) configured in the JGroups stack.
- `org.jgroups.raft` -- the user-facing API, logs, serialization, CLI and the test harness (`testfwk`).
- `src/test/java` -- tests; `src/test/demo` -- demo applications.

## Key concepts
- JGroups provides failure detection and membership. There are no heartbeats and no candidates as in the Raft paper:
  an election is triggered by a view change and run by dedicated election protocols.
- The core algorithm runs on a single event loop thread.

## Build and test
- `mvn clean install` -- build and run tests.
- `mvn package -Dgroups=functional` -- the test set CI runs.
- `mvn clean install -Pmanual` -- generate the documentation.

## Conventions
- Never use `var`.
- camelCase for new identifiers, including test locals. Some legacy code uses snake_case; do not copy it.
- Restrict visibility: package-private by default, public only when needed. Prefer `final` and `sealed` types.
- Javadoc on user-facing API explains what and why, never how, and never mentions internal types or protocol names.
- Use `--` and `->` instead of em-dash and arrow symbols in code, comments and docs.
- Tests use TestNG groups (`functional` runs in CI), the `org.jgroups.raft.testfwk` harness for clusters,
  and `eventually` instead of sleeping.

## Compatibility
Until 2.0.0.Final is released, wire, log, snapshot and API changes may break compatibility.
From 2.0.0.Final on, compatibility is required.

## Documentation and design
All documentation is AsciiDoc under `src/docs/`:
- `src/docs/design/` -- one design document per component (election, append entries, log, log integrity,
  serialization, learners, CLI, schema validation). Read the relevant one before changing a component;
  it explains the decisions behind the current behavior.
- `src/docs/manual/` -- the user manual, split into tutorials, how-to guides, reference and explanation.
  `protocols-generated.adoc` is generated from protocol properties; do not edit it.
- `src/docs/cli/` -- CLI command reference.

A change is complete only when the documents it affects are updated in the same PR:
- behavior, configuration or public API visible to users -> the manual (and the CLI reference for CLI changes);
- changes to how a component works internally -> its design document;
- a new component -> a new design document, included from `src/docs/design/index.adoc`.

## Reviewing
When reviewing pull requests, read and follow REVIEW.md.

## Maintaining AI context files
When a change alters the layout, build commands, conventions or architecture described here,
update AGENTS.md and REVIEW.md in the same PR.
