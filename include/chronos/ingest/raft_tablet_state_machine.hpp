#ifndef CHRONOS_INGEST_RAFT_TABLET_STATE_MACHINE_HPP_
#define CHRONOS_INGEST_RAFT_TABLET_STATE_MACHINE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/ingest/raft_tablet_snapshot_storage.hpp"
#include "chronos/ingest/retry_directory.hpp"
#include "chronos/ingest/tablet_state.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/schema/table_schema.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace chronos::ingest {

inline constexpr std::uint8_t kRaftColumnarAppendEntryType = 1U;

struct RaftTabletApplicationReport {
  raft::LogIndex first_applied_index{};
  raft::LogIndex last_applied_index{};
  std::size_t applied_entries{};
  std::size_t matching_retries{};
};

struct RaftTabletSnapshotCompactionReport {
  raft::SnapshotMetadata snapshot;
  std::string file_name;
  std::size_t application_entries{};
  bool application_snapshot_already_present{false};
};

// Single-thread-affine owner of one Raft group's tablet application state. recover() accepts only
// fresh unpublished tablet/retry owners and reconstructs the entire committed prefix before
// returning them. A compacted prefix requires the overload that transfers ownership of the exact
// installed application-snapshot storage. Live apply_committed() publishes only committed entries,
// in index order, then durably advances the Raft applied index.
class RaftTabletStateMachine {
public:
  RaftTabletStateMachine() = delete;
  ~RaftTabletStateMachine();
  RaftTabletStateMachine(const RaftTabletStateMachine&) = delete;
  RaftTabletStateMachine& operator=(const RaftTabletStateMachine&) = delete;
  RaftTabletStateMachine(RaftTabletStateMachine&&) noexcept;
  RaftTabletStateMachine& operator=(RaftTabletStateMachine&&) noexcept;

  [[nodiscard]] static common::Result<RaftTabletStateMachine>
  recover(raft::GroupId group_id, raft::DurableMultiRaftRuntime& runtime,
          RetryDirectory retry_directory, TabletState tablet,
          std::vector<std::shared_ptr<const schema::TableSchema>> retained_schemas,
          ColumnarAppendDecodeLimits decode_limits = {});

  [[nodiscard]] static common::Result<RaftTabletStateMachine>
  recover(raft::GroupId group_id, raft::DurableMultiRaftRuntime& runtime,
          RaftTabletSnapshotStorage snapshot_storage, RetryDirectory retry_directory,
          TabletState tablet,
          std::vector<std::shared_ptr<const schema::TableSchema>> retained_schemas,
          ColumnarAppendDecodeLimits decode_limits = {});

  [[nodiscard]] common::Result<RaftTabletApplicationReport> apply_committed();
  // Installs an exact application snapshot before durably compacting Raft to the same applied
  // boundary. Requires snapshot-storage ownership supplied to recover().
  [[nodiscard]] common::Result<RaftTabletSnapshotCompactionReport>
  compact_applied_prefix(raft::LogIndex last_included_index, std::uint64_t manifest_generation,
                         std::array<std::byte, 32U> part_set_checksum);
  [[nodiscard]] common::Result<RaftTabletSnapshotReclamationReport> reclaim_obsolete_snapshots();
  [[nodiscard]] common::Result<raft::QuorumSyncReceipt>
  prove_applied_quorum_sync(raft::LogIndex index) const;

  [[nodiscard]] RetryDirectory& retry_directory() noexcept;
  [[nodiscard]] const RetryDirectory& retry_directory() const noexcept;
  [[nodiscard]] TabletState& tablet() noexcept;
  [[nodiscard]] const TabletState& tablet() const noexcept;
  [[nodiscard]] const raft::GroupId& group_id() const noexcept;
  [[nodiscard]] bool failed() const noexcept;
  [[nodiscard]] common::Status failure_status() const;

private:
  class Impl;
  [[nodiscard]] static common::Result<RaftTabletStateMachine>
  recover_impl(raft::GroupId group_id, raft::DurableMultiRaftRuntime& runtime,
               std::optional<RaftTabletSnapshotStorage> snapshot_storage,
               RetryDirectory retry_directory, TabletState tablet,
               std::vector<std::shared_ptr<const schema::TableSchema>> retained_schemas,
               ColumnarAppendDecodeLimits decode_limits);
  explicit RaftTabletStateMachine(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::ingest

#endif // CHRONOS_INGEST_RAFT_TABLET_STATE_MACHINE_HPP_
