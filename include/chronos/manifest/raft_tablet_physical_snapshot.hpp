#ifndef CHRONOS_MANIFEST_RAFT_TABLET_PHYSICAL_SNAPSHOT_HPP_
#define CHRONOS_MANIFEST_RAFT_TABLET_PHYSICAL_SNAPSHOT_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/ingest/sha256.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/raft/multi_raft.hpp"

#include <cstdint>

namespace chronos::manifest {

// One canonical, source-neutral Manifest v2 projection containing exactly one Raft tablet, its
// CSEG descriptors, and its protected retry outcomes. CSEG bytes remain separate streaming
// objects. part_set_checksum is SHA-256 over the exact canonical part-descriptor table.
class EncodedRaftTabletPhysicalSnapshot {
public:
  EncodedRaftTabletPhysicalSnapshot() = delete;
  EncodedRaftTabletPhysicalSnapshot(const EncodedRaftTabletPhysicalSnapshot&) = delete;
  EncodedRaftTabletPhysicalSnapshot& operator=(const EncodedRaftTabletPhysicalSnapshot&) = delete;
  EncodedRaftTabletPhysicalSnapshot(EncodedRaftTabletPhysicalSnapshot&&) noexcept = default;
  EncodedRaftTabletPhysicalSnapshot&
  operator=(EncodedRaftTabletPhysicalSnapshot&&) noexcept = default;

  [[nodiscard]] common::ByteView bytes() const noexcept;
  [[nodiscard]] const ingest::Sha256Digest& part_set_checksum() const noexcept;

private:
  EncodedRaftTabletPhysicalSnapshot(EncodedTemporalManifest manifest,
                                    ingest::Sha256Digest part_set_checksum) noexcept;

  EncodedTemporalManifest manifest_;
  ingest::Sha256Digest part_set_checksum_;

  friend common::Result<EncodedRaftTabletPhysicalSnapshot>
  build_raft_tablet_physical_snapshot(const DecodedTemporalManifestView&, const raft::GroupId&,
                                      const schema::TabletId&, raft::LogIndex);
};

struct RaftTabletPhysicalSnapshotReport {
  DatabaseId database_id;
  raft::GroupId group_id;
  schema::TableId table_id;
  schema::TabletId tablet_id;
  std::uint64_t manifest_generation{};
  raft::LogIndex applied_position{};
  std::uint64_t part_count{};
  std::uint64_t retry_count{};
  ingest::Sha256Digest part_set_checksum;
};

// Projects one already-decoded authoritative Manifest v2 generation. The selected tablet must use
// the supplied Raft group as its source and must be durable exactly through applied_position.
[[nodiscard]] common::Result<EncodedRaftTabletPhysicalSnapshot> build_raft_tablet_physical_snapshot(
    const DecodedTemporalManifestView& selected, const raft::GroupId& group_id,
    const schema::TabletId& tablet_id, raft::LogIndex applied_position);

// Exact-decodes an untrusted projection and binds it to the full installed Raft snapshot identity.
// A successful report authorizes only subsequent verified CSEG transfer; it does not install files
// or publish a destination Manifest generation.
[[nodiscard]] common::Result<RaftTabletPhysicalSnapshotReport>
validate_raft_tablet_physical_snapshot(common::ByteView bytes, const raft::GroupId& group_id,
                                       const schema::TableId& table_id,
                                       const schema::TabletId& tablet_id,
                                       const raft::SnapshotMetadata& raft_snapshot,
                                       ManifestDecodeLimits limits = {});

} // namespace chronos::manifest

#endif // CHRONOS_MANIFEST_RAFT_TABLET_PHYSICAL_SNAPSHOT_HPP_
