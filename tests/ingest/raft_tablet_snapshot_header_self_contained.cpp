#include "chronos/ingest/raft_tablet_snapshot.hpp"

namespace {
[[maybe_unused]] constexpr auto* kEncodeRaftTabletApplicationSnapshot =
    &chronos::ingest::encode_raft_tablet_application_snapshot_v1;
[[maybe_unused]] constexpr auto* kDecodeRaftTabletApplicationSnapshot =
    &chronos::ingest::decode_raft_tablet_application_snapshot_v1;
} // namespace
