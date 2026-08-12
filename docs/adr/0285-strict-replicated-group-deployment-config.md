# ADR 0285: Strict replicated-group deployment configuration

- **Status:** accepted
- **Date:** 2026-08-12
- **Owners:** ChronosDB service, runtime, Raft, and operations maintainers

## Context and decision

Committed metadata is authoritative for tablet placement, binding, schema, and policy, but a node
still needs an external declaration of which Raft groups it hosts and the voter configuration used
to restore each deterministic group. Passing those facts as repeated loosely parsed daemon flags
would make duplicate, reordered, or truncated configuration difficult to audit.

The replicated daemon accepts one bounded text file parsed by `parse_replicated_group_config`. Its
first line is exactly `CHRONOSDB_REPLICATED_GROUPS_V1`. Every following nonempty line is a lowercase
canonical UUID, `=`, and a strictly increasing comma-separated list of positive canonical decimal
node IDs. Spaces, comments, carriage returns, leading zeroes, duplicate voters, duplicate groups,
and unknown framing are rejected. A final LF is optional. Parsed groups are sorted by UUID.

The parser is pure and does not read files. The daemon owns regular-file/no-symlink/size validation
and passes the complete bytes under a one-megabyte default bound. Group and per-group voter counts
are separately bounded.

This file is deployment configuration, not consensus or durable database state. Recovery still
validates it against the physical Raft log, and write admission still validates current stable
membership against committed placement. Changing the file cannot override recovered consensus.

## Consequences and validation

Configuration parsing is deterministic, allocation-bounded, and independently testable. The
format deliberately has no include, environment expansion, endpoint, credential, or permissive
comment syntax. Transport endpoints remain a separate authenticated transport configuration.

Focused tests cover canonical parsing/sorting and malformed UUIDs, nil groups, zero/leading-zero/
duplicate/unsorted voters, duplicate groups, blank lines, CRLF, and group/voter bounds. File-system
permission matrices, live reload, large deployment profiles, and administrative tooling remain
deferred.

No durable or network bytes change.

## References

- [Replicated group configuration](../operations/replicated-group-config.md)
- [ADR 0284](0284-committed-metadata-replicated-database-recovery.md)
