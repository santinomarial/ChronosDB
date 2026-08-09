#ifndef CHRONOS_INGEST_RAFT_TABLET_STATE_MACHINE_HPP_
#define CHRONOS_INGEST_RAFT_TABLET_STATE_MACHINE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/ingest/retry_directory.hpp"
#include "chronos/ingest/tablet_state.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstdint>
#include <memory>
#include <vector>

namespace chronos::ingest {

inline constexpr std::uint8_t kRaftColumnarAppendEntryType = 1U;

struct RaftTabletApplicationReport {
  raft::LogIndex first_applied_index{};
  raft::LogIndex last_applied_index{};
  std::size_t applied_entries{};
  std::size_t matching_retries{};
};

// Single-thread-affine owner of one Raft group's tablet application state. recover() accepts only
// fresh unpublished tablet/retry owners and reconstructs the entire committed prefix before
// returning them. Until application snapshots exist, a nonzero Raft snapshot boundary is rejected.
// Live apply_committed() publishes only committed entries, in index order, then durably advances
// the Raft applied index. The complete retained Raft log remains the recovery source of truth.
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

  [[nodiscard]] common::Result<RaftTabletApplicationReport> apply_committed();

  [[nodiscard]] RetryDirectory& retry_directory() noexcept;
  [[nodiscard]] const RetryDirectory& retry_directory() const noexcept;
  [[nodiscard]] TabletState& tablet() noexcept;
  [[nodiscard]] const TabletState& tablet() const noexcept;
  [[nodiscard]] const raft::GroupId& group_id() const noexcept;
  [[nodiscard]] bool failed() const noexcept;
  [[nodiscard]] common::Status failure_status() const;

private:
  class Impl;
  explicit RaftTabletStateMachine(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::ingest

#endif // CHRONOS_INGEST_RAFT_TABLET_STATE_MACHINE_HPP_
