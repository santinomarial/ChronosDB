#ifndef CHRONOS_RAFT_METADATA_RUNTIME_HPP_
#define CHRONOS_RAFT_METADATA_RUNTIME_HPP_

#include "chronos/common/result.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/raft/metadata.hpp"
#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/metadata_snapshot_storage.hpp"
#include "chronos/raft/schema_definition_codec.hpp"

#include <cstddef>
#include <memory>
#include <optional>
#include <string>

namespace chronos::raft {

struct MetadataApplicationReport {
  LogIndex first_applied_index{};
  LogIndex last_applied_index{};
  std::size_t applied_commands{};
};

struct MetadataSnapshotCompactionReport {
  SnapshotMetadata snapshot;
  std::string file_name;
  std::size_t application_entries{};
  bool application_snapshot_already_present{false};
};

// Single-thread-affine committed application owner for the dedicated metadata Raft group. The
// Recovery reconstructs fresh unpublished state from an exact installed snapshot plus committed
// retained suffix. Compaction installs application bytes before durably advancing Raft metadata.
class DurableMetadataStateMachine {
public:
  DurableMetadataStateMachine() = delete;
  ~DurableMetadataStateMachine();
  DurableMetadataStateMachine(const DurableMetadataStateMachine&) = delete;
  DurableMetadataStateMachine& operator=(const DurableMetadataStateMachine&) = delete;
  DurableMetadataStateMachine(DurableMetadataStateMachine&&) noexcept;
  DurableMetadataStateMachine& operator=(DurableMetadataStateMachine&&) noexcept;

  [[nodiscard]] static common::Result<DurableMetadataStateMachine>
  recover(GroupId group_id, DurableMultiRaftRuntime& runtime, MetadataLimits state_limits = {},
          MetadataCommandCodecLimits codec_limits = {},
          SchemaDefinitionCodecLimits schema_codec_limits = {});

  [[nodiscard]] static common::Result<DurableMetadataStateMachine>
  recover(GroupId group_id, DurableMultiRaftRuntime& runtime,
          MetadataSnapshotStorage snapshot_storage, MetadataLimits state_limits = {},
          MetadataCommandCodecLimits codec_limits = {},
          SchemaDefinitionCodecLimits schema_codec_limits = {});

  [[nodiscard]] common::Result<MetadataApplicationReport> apply_committed();
  [[nodiscard]] common::Result<MetadataSnapshotCompactionReport>
  compact_applied_prefix(LogIndex last_included_index);
  [[nodiscard]] common::Result<QuorumSyncReceipt> prove_applied_quorum_sync(LogIndex index) const;

  [[nodiscard]] const MetadataStateMachine& state() const noexcept;
  [[nodiscard]] const GroupId& group_id() const noexcept;
  [[nodiscard]] bool failed() const noexcept;
  [[nodiscard]] common::Status failure_status() const;

private:
  class Impl;
  [[nodiscard]] static common::Result<DurableMetadataStateMachine>
  recover_impl(GroupId group_id, DurableMultiRaftRuntime& runtime,
               std::optional<MetadataSnapshotStorage> snapshot_storage, MetadataLimits state_limits,
               MetadataCommandCodecLimits codec_limits,
               SchemaDefinitionCodecLimits schema_codec_limits);
  explicit DurableMetadataStateMachine(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::raft

#endif // CHRONOS_RAFT_METADATA_RUNTIME_HPP_
