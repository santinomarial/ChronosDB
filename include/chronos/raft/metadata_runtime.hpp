#ifndef CHRONOS_RAFT_METADATA_RUNTIME_HPP_
#define CHRONOS_RAFT_METADATA_RUNTIME_HPP_

#include "chronos/common/result.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/raft/metadata.hpp"
#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/schema_definition_codec.hpp"

#include <cstddef>
#include <memory>

namespace chronos::raft {

struct MetadataApplicationReport {
  LogIndex first_applied_index{};
  LogIndex last_applied_index{};
  std::size_t applied_commands{};
};

// Single-thread-affine committed application owner for the dedicated metadata Raft group. The
// complete retained committed log is replayed into fresh state on recovery. A compacted prefix is
// rejected until a metadata application snapshot format exists.
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

  [[nodiscard]] common::Result<MetadataApplicationReport> apply_committed();
  [[nodiscard]] common::Result<QuorumSyncReceipt> prove_applied_quorum_sync(LogIndex index) const;

  [[nodiscard]] const MetadataStateMachine& state() const noexcept;
  [[nodiscard]] const GroupId& group_id() const noexcept;
  [[nodiscard]] bool failed() const noexcept;
  [[nodiscard]] common::Status failure_status() const;

private:
  class Impl;
  explicit DurableMetadataStateMachine(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::raft

#endif // CHRONOS_RAFT_METADATA_RUNTIME_HPP_
