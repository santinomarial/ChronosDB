# Replicated Group Configuration

An already provisioned replicated node needs one external file listing every Raft group resident in
its physical log and the voter set used for recovery. Example:

```text
CHRONOSDB_REPLICATED_GROUPS_V1
11111111-1111-1111-1111-111111111111=1,2,3
22222222-2222-2222-2222-222222222222=1,2,3
```

The metadata group recorded in Database Bootstrap v1 must appear. Every recovered resident data
group must appear exactly once. UUIDs use lowercase canonical hyphenated form. Node IDs are positive
decimal integers in strictly increasing order with no leading zeroes. Lines contain no spaces or
comments. LF is the only line ending; the final LF is optional.

This file selects resident consensus groups; it does not define tablet routing or schema. The
committed metadata catalog must bind each configured data group to one tablet, and the coordinator
checks current stable membership against committed placement before every write. Editing this file
cannot rewrite recovered Raft state.

The daemon reads the file once during startup as a bounded regular file without following a final
symlink. Configuration reload and endpoint/credential configuration are not part of this format.
