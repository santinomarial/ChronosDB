#include "chronos/ingest/raft_tablet_snapshot.hpp"

namespace {
static_assert(chronos::ingest::kMaximumRaftTabletSnapshotSize == (std::size_t{1U} << 30U));
static_assert(chronos::ingest::RaftTabletSnapshotCodecLimits{}.maximum_snapshot_bytes ==
              (std::size_t{1U} << 28U));
[[maybe_unused]] constexpr auto* kEncodeRaftTabletApplicationSnapshot =
    &chronos::ingest::encode_raft_tablet_application_snapshot_v1;
[[maybe_unused]] constexpr auto* kDecodeRaftTabletApplicationSnapshot =
    &chronos::ingest::decode_raft_tablet_application_snapshot_v1;
} // namespace
