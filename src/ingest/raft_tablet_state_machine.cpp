#include "chronos/ingest/raft_tablet_state_machine.hpp"

#include "chronos/ingest/committed_columnar_append.hpp"
#include "chronos/raft/membership.hpp"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace chronos::ingest {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status corruption(std::string message) {
  return common::Status{common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Status unavailable(std::string message) {
  return common::Status{common::StatusCode::kUnavailable, std::move(message)};
}

[[nodiscard]] common::Status unsupported(std::string message) {
  return common::Status{common::StatusCode::kNotSupported, std::move(message)};
}

[[nodiscard]] common::Status committed_decode_error(const ColumnarAppendDecodeError& error) {
  switch (error.kind()) {
  case ColumnarAppendDecodeErrorKind::kIncomplete:
  case ColumnarAppendDecodeErrorKind::kCorruption:
    return corruption("committed Raft COLUMNAR_APPEND is corrupt: " + error.status().message());
  case ColumnarAppendDecodeErrorKind::kUnsupported:
    return unsupported("committed Raft COLUMNAR_APPEND is unsupported: " +
                       error.status().message());
  case ColumnarAppendDecodeErrorKind::kResourceLimit:
  case ColumnarAppendDecodeErrorKind::kInternal:
    return error.status();
  }
  return corruption("committed Raft COLUMNAR_APPEND has an unknown decode classification");
}

} // namespace

class RaftTabletStateMachine::Impl {
public:
  Impl(raft::GroupId configured_group_id, raft::DurableMultiRaftRuntime& configured_runtime,
       RetryDirectory configured_retry_directory, TabletState configured_tablet,
       std::vector<std::shared_ptr<const schema::TableSchema>> configured_schemas,
       const ColumnarAppendDecodeLimits configured_decode_limits) noexcept
      : group_id(std::move(configured_group_id)), runtime(&configured_runtime),
        retry_directory(std::move(configured_retry_directory)),
        tablet(std::move(configured_tablet)), retained_schemas(std::move(configured_schemas)),
        decode_limits(configured_decode_limits) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (failure.is_ok()) {
      failure = std::move(status);
      static_cast<void>(tablet.fail_closed());
    }
    return failure;
  }

  [[nodiscard]] std::shared_ptr<const schema::TableSchema>
  find_schema(const DecodedColumnarAppendView& command) const noexcept {
    const auto found = std::ranges::find_if(retained_schemas, [&](const auto& candidate) {
      return candidate != nullptr && candidate->schema_id() == command.schema_id() &&
             candidate->version() == command.schema_version();
    });
    return found == retained_schemas.end() ? nullptr : *found;
  }

  [[nodiscard]] common::Result<DecodedColumnarAppendView>
  decode(const raft::LogEntry& entry) const {
    if (entry.type != kRaftColumnarAppendEntryType) {
      return common::make_unexpected(
          unsupported("committed Raft entry type has no tablet state-machine handler"));
    }
    ColumnarAppendDecodeResult decoded =
        decode_columnar_append_v1_exact(entry.payload, decode_limits);
    if (!decoded.has_value()) {
      return common::make_unexpected(committed_decode_error(decoded.error()));
    }
    if (find_schema(*decoded) == nullptr) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kNotFound,
                         "committed Raft command schema is absent from retained lineage"});
    }
    return std::move(*decoded);
  }

  [[nodiscard]] common::Status preflight(const std::span<const raft::LogEntry> entries) const {
    for (const raft::LogEntry& entry : entries) {
      if (raft::is_membership_entry_type(entry.type))
        continue;
      auto command = decode(entry);
      if (!command.has_value()) {
        return command.error();
      }
      const common::Status status =
          validate_columnar_append_schema(*command, *find_schema(*command));
      if (!status.is_ok()) {
        return corruption("committed Raft command disagrees with retained schema: " +
                          status.message());
      }
      auto snapshot = tablet.snapshot();
      if (!snapshot.has_value()) {
        return snapshot.error();
      }
      if (command->table_id() != snapshot->table_id() ||
          command->tablet_id() != snapshot->tablet_id()) {
        return corruption("committed Raft command targets a different tablet");
      }
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Result<RaftTabletApplicationReport>
  apply_entries(const std::span<const raft::LogEntry> entries, const bool persist_applied) {
    RaftTabletApplicationReport report;
    if (entries.empty()) {
      return report;
    }
    const common::Status checked = preflight(entries);
    if (!checked.is_ok()) {
      return common::make_unexpected(fail(checked));
    }
    report.first_applied_index = entries.front().index;
    for (const raft::LogEntry& entry : entries) {
      if (raft::is_membership_entry_type(entry.type)) {
        report.last_applied_index = entry.index;
        continue;
      }
      auto command = decode(entry);
      if (!command.has_value()) {
        return common::make_unexpected(fail(command.error()));
      }
      auto applied = apply_committed_columnar_append(
          *command, find_schema(*command), head::HeadCommitPosition::raft(group_id, entry.index),
          retry_directory, tablet, decode_limits);
      if (!applied.has_value()) {
        return common::make_unexpected(fail(applied.error()));
      }
      ++report.applied_entries;
      if (applied->kind == CommittedColumnarAppendKind::kMatchingRetry) {
        ++report.matching_retries;
      }
      report.last_applied_index = entry.index;
    }
    if (!persist_applied) {
      return report;
    }
    auto marked =
        runtime->execute_batch({{group_id, raft::MarkAppliedOperation{report.last_applied_index}}});
    if (!marked.has_value()) {
      return common::make_unexpected(fail(marked.error()));
    }
    if (marked->size() != 1U || !marked->front().status.is_ok()) {
      const common::Status status =
          marked->empty() ? unavailable("Raft applied-index persistence returned no result")
                          : marked->front().status;
      return common::make_unexpected(fail(status));
    }
    return report;
  }

  raft::GroupId group_id;
  raft::DurableMultiRaftRuntime* runtime;
  RetryDirectory retry_directory;
  TabletState tablet;
  std::vector<std::shared_ptr<const schema::TableSchema>> retained_schemas;
  ColumnarAppendDecodeLimits decode_limits;
  common::Status failure;
};

RaftTabletStateMachine::RaftTabletStateMachine(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
RaftTabletStateMachine::~RaftTabletStateMachine() = default;
RaftTabletStateMachine::RaftTabletStateMachine(RaftTabletStateMachine&&) noexcept = default;
RaftTabletStateMachine&
RaftTabletStateMachine::operator=(RaftTabletStateMachine&&) noexcept = default;

common::Result<RaftTabletStateMachine> RaftTabletStateMachine::recover(
    raft::GroupId group_id, raft::DurableMultiRaftRuntime& runtime, RetryDirectory retry_directory,
    TabletState tablet, std::vector<std::shared_ptr<const schema::TableSchema>> retained_schemas,
    const ColumnarAppendDecodeLimits decode_limits) {
  if (group_id.is_nil() || retained_schemas.empty() ||
      std::ranges::any_of(retained_schemas, [](const auto& item) { return item == nullptr; })) {
    return common::make_unexpected(invalid("Raft tablet application configuration is invalid"));
  }
  const raft::RaftNode* node = runtime.find_group(group_id);
  if (node == nullptr) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "Raft tablet group does not exist"});
  }
  auto initial = tablet.snapshot();
  if (!initial.has_value()) {
    return common::make_unexpected(initial.error());
  }
  if (initial->visible_row_count() != 0U || initial->retry_entry_count() != 0U ||
      initial->applied_position().has_value() || retry_directory.metrics().entries != 0U) {
    return common::make_unexpected(
        invalid("Raft tablet recovery requires fresh tablet and retry owners"));
  }
  if (retained_schemas.front()->table_id() != initial->table_id() ||
      std::ranges::any_of(retained_schemas, [&](const auto& item) {
        return item->table_id() != initial->table_id();
      })) {
    return common::make_unexpected(
        invalid("Raft tablet retained schemas do not match the owning table"));
  }
  const raft::PersistentState& persistent = node->persistent_state();
  if (persistent.snapshot.last_included_index != 0U) {
    return common::make_unexpected(
        unsupported("Raft tablet recovery requires an application snapshot for compacted history"));
  }
  if (persistent.commit_index > persistent.log.size()) {
    return common::make_unexpected(corruption("Raft committed prefix exceeds retained log"));
  }

  auto impl = std::make_unique<Impl>(group_id, runtime, std::move(retry_directory),
                                     std::move(tablet), std::move(retained_schemas), decode_limits);
  const std::span<const raft::LogEntry> committed{
      persistent.log.data(), static_cast<std::size_t>(persistent.commit_index)};
  auto recovered =
      impl->apply_entries(committed, persistent.applied_index < persistent.commit_index);
  if (!recovered.has_value()) {
    return common::make_unexpected(recovered.error());
  }
  return RaftTabletStateMachine{std::move(impl)};
}

common::Result<RaftTabletApplicationReport> RaftTabletStateMachine::apply_committed() {
  if (!impl_->failure.is_ok()) {
    return common::make_unexpected(impl_->failure);
  }
  const raft::RaftNode* node = impl_->runtime->find_group(impl_->group_id);
  if (node == nullptr) {
    return common::make_unexpected(impl_->fail(unavailable("Raft tablet group disappeared")));
  }
  if (node->persistent_state().snapshot.last_included_index != 0U) {
    return common::make_unexpected(impl_->fail(
        unsupported("Raft tablet application cannot cross an uninstalled application snapshot")));
  }
  return impl_->apply_entries(node->committed_unapplied(), true);
}

common::Result<raft::QuorumSyncReceipt>
RaftTabletStateMachine::prove_applied_quorum_sync(const raft::LogIndex index) const {
  if (!impl_->failure.is_ok()) {
    return common::make_unexpected(impl_->failure);
  }
  const raft::RaftNode* const node = impl_->runtime->find_group(impl_->group_id);
  if (node == nullptr || node->applied_index() < index) {
    return common::make_unexpected(
        unavailable("QUORUM_SYNC entry has not been applied by the tablet state machine"));
  }
  auto snapshot = impl_->tablet.snapshot();
  if (!snapshot.has_value()) {
    return common::make_unexpected(snapshot.error());
  }
  if (!snapshot->applied_position().has_value() ||
      snapshot->applied_position()->source != head::CommitSource::kRaft ||
      snapshot->applied_position()->raft_group_id != impl_->group_id ||
      snapshot->applied_position()->record_sequence < index) {
    return common::make_unexpected(
        corruption("Raft applied index is not covered by the tablet publication frontier"));
  }
  return impl_->runtime->prove_quorum_sync(impl_->group_id, index);
}

RetryDirectory& RaftTabletStateMachine::retry_directory() noexcept {
  return impl_->retry_directory;
}
const RetryDirectory& RaftTabletStateMachine::retry_directory() const noexcept {
  return impl_->retry_directory;
}
TabletState& RaftTabletStateMachine::tablet() noexcept {
  return impl_->tablet;
}
const TabletState& RaftTabletStateMachine::tablet() const noexcept {
  return impl_->tablet;
}
const raft::GroupId& RaftTabletStateMachine::group_id() const noexcept {
  return impl_->group_id;
}
bool RaftTabletStateMachine::failed() const noexcept {
  return !impl_->failure.is_ok();
}
common::Status RaftTabletStateMachine::failure_status() const {
  return impl_->failure;
}

} // namespace chronos::ingest
