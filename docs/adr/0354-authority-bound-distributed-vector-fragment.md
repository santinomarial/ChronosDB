# ADR 0354: Authority-bound distributed vector fragment

- **Status:** accepted
- **Date:** 2026-08-13
- **Owners:** ChronosDB query, manifest, schema, and distributed-systems maintainers
- **Extends:** [ADR 0166](0166-authority-bound-distributed-fragment-construction.md),
  [ADR 0353](0353-group-scoped-distributed-vector-fragment.md)

## Context

Vector Fragment Dispatch v1 can represent complete authority fields, but a caller could still
assemble them from unrelated metadata publications, Manifest generations, schema versions, and
read observations. Schema-neutral plan indices also need exact logical-type validation before they
can select aggregate kernels.

## Decision

`bind_distributed_vector_fragment` is the only implemented constructor from runtime authority to an
owned vector dispatch. It borrows one vector query plan, one exact read admission, one acquire-loaded
Manifest v2 snapshot, one committed placement, one destination schema, one immutable Raft group,
and one requested projection for the call; the result owns every value.

The binder uses the common read-admission validator shared with aggregate planning. It requires the
selected serving node in canonical committed placement and preserves leader-hint rules. The
Manifest tablet must exact-match database/table/tablet/group, admitted applied position, and
destination recovery schema ID/version. Newer or older durable state is not substituted.

Projection ordinals are nonempty, unique, and in the exact destination schema. The plan is validated
against that projection width. Every aggregate input is then mapped through the projection to its
exact logical type/nullability and passed through the existing local vector aggregate output-shape
validator. Unsupported operation/type combinations fail before dispatch creation. Event-time
bounds remain canonical. The binder performs no I/O or publication.

The shared `validate_distributed_read_admission` now has a policy-plus-fragment-span overload; the
aggregate-plan overload delegates to it without changing existing behavior.

## Consequences and validation

One focused Manifest-backed test constructs and encodes a grouped SUM/order/LIMIT vector dispatch
from exact committed authority, then rejects SUM over TIMESTAMP and an out-of-projection input.
All seven existing aggregate/grouped/metadata binding cases pass unchanged. Header self-containment
and installed consumption cover the public binder.

Compatible multi-tablet vector snapshot ownership, metadata-backed batch construction, and request
partial I/O are implemented separately. Group-keyed proof acquisition, worker execution, result
coordination, authenticated transport, and process integration remain incomplete. No Phase 16 exit
gate is claimed.

Invariants 4–6, 11, 14, and 18 apply.

## References

- [Distributed Vector Fragment Dispatch v1](../formats/distributed-vector-fragment-dispatch-v1.md)
- [Authority-bound distributed fragment construction](0166-authority-bound-distributed-fragment-construction.md)
- [Proof-bound distributed read admission](0115-proof-bound-distributed-read-admission.md)
