#include "chronos/raft/metadata_runtime.hpp"

#include <cstddef>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

namespace chronos::raft {
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

} // namespace

class DurableMetadataStateMachine::Impl {
public:
  Impl(GroupId configured_group_id, DurableMultiRaftRuntime& configured_runtime,
       MetadataStateMachine configured_state,
       const MetadataCommandCodecLimits configured_codec_limits) noexcept
      : group_id(std::move(configured_group_id)), runtime(&configured_runtime),
        metadata(std::move(configured_state)), codec_limits(configured_codec_limits) {}

  [[nodiscard]] common::Status fail(common::Status status) {
    if (failure.is_ok())
      failure = std::move(status);
    return failure;
  }

  [[nodiscard]] common::Result<MetadataApplicationReport>
  apply_entries(const std::span<const LogEntry> entries, const bool persist_applied) {
    MetadataApplicationReport report;
    if (entries.empty())
      return report;
    std::vector<MetadataCommand> commands;
    commands.reserve(entries.size());
    for (const LogEntry& entry : entries) {
      if (entry.type != kRaftMetadataCommandEntryType) {
        return common::make_unexpected(
            fail(unsupported("committed metadata Raft entry type is unsupported")));
      }
      auto decoded = decode_metadata_command_v1(entry.payload, codec_limits);
      if (!decoded.has_value())
        return common::make_unexpected(fail(decoded.error()));
      commands.push_back(std::move(*decoded));
    }
    report.first_applied_index = entries.front().index;
    for (std::size_t ordinal = 0U; ordinal < entries.size(); ++ordinal) {
      common::Status status =
          metadata.apply_committed(entries[ordinal].index, std::move(commands[ordinal]));
      if (!status.is_ok())
        return common::make_unexpected(fail(status));
      ++report.applied_commands;
      report.last_applied_index = entries[ordinal].index;
    }
    if (!persist_applied)
      return report;
    auto marked =
        runtime->execute_batch({{group_id, MarkAppliedOperation{report.last_applied_index}}});
    if (!marked.has_value())
      return common::make_unexpected(fail(marked.error()));
    if (marked->size() != 1U || !marked->front().status.is_ok()) {
      return common::make_unexpected(fail(
          marked->empty() ? unavailable("metadata applied-index persistence returned no result")
                          : marked->front().status));
    }
    return report;
  }

  GroupId group_id;
  DurableMultiRaftRuntime* runtime;
  MetadataStateMachine metadata;
  MetadataCommandCodecLimits codec_limits;
  common::Status failure;
};

DurableMetadataStateMachine::DurableMetadataStateMachine(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
DurableMetadataStateMachine::~DurableMetadataStateMachine() = default;
DurableMetadataStateMachine::DurableMetadataStateMachine(DurableMetadataStateMachine&&) noexcept =
    default;
DurableMetadataStateMachine&
DurableMetadataStateMachine::operator=(DurableMetadataStateMachine&&) noexcept = default;

common::Result<DurableMetadataStateMachine>
DurableMetadataStateMachine::recover(GroupId group_id, DurableMultiRaftRuntime& runtime,
                                     const MetadataLimits state_limits,
                                     const MetadataCommandCodecLimits codec_limits) {
  if (group_id.is_nil())
    return common::make_unexpected(invalid("metadata Raft group identity is nil"));
  const RaftNode* const node = runtime.find_group(group_id);
  if (node == nullptr) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "metadata Raft group does not exist"});
  }
  auto state = MetadataStateMachine::create(state_limits);
  if (!state.has_value())
    return common::make_unexpected(state.error());
  const PersistentState& persistent = node->persistent_state();
  if (persistent.snapshot.last_included_index != 0U) {
    return common::make_unexpected(
        unsupported("metadata recovery requires a snapshot for compacted Raft history"));
  }
  if (persistent.commit_index > persistent.log.size()) {
    return common::make_unexpected(corruption("metadata committed prefix exceeds retained log"));
  }
  auto impl = std::make_unique<Impl>(group_id, runtime, std::move(*state), codec_limits);
  const std::span<const LogEntry> committed{persistent.log.data(),
                                            static_cast<std::size_t>(persistent.commit_index)};
  auto recovered =
      impl->apply_entries(committed, persistent.applied_index < persistent.commit_index);
  if (!recovered.has_value())
    return common::make_unexpected(recovered.error());
  return DurableMetadataStateMachine{std::move(impl)};
}

common::Result<MetadataApplicationReport> DurableMetadataStateMachine::apply_committed() {
  if (!impl_->failure.is_ok())
    return common::make_unexpected(impl_->failure);
  const RaftNode* const node = impl_->runtime->find_group(impl_->group_id);
  if (node == nullptr)
    return common::make_unexpected(impl_->fail(unavailable("metadata Raft group disappeared")));
  if (node->persistent_state().snapshot.last_included_index != 0U) {
    return common::make_unexpected(
        impl_->fail(unsupported("metadata application cannot cross an uninstalled snapshot")));
  }
  return impl_->apply_entries(node->committed_unapplied(), true);
}

common::Result<QuorumSyncReceipt>
DurableMetadataStateMachine::prove_applied_quorum_sync(const LogIndex index) const {
  if (!impl_->failure.is_ok())
    return common::make_unexpected(impl_->failure);
  if (index == 0U || impl_->metadata.applied_index() < index) {
    return common::make_unexpected(unavailable("QUORUM_SYNC metadata entry has not been applied"));
  }
  return impl_->runtime->prove_quorum_sync(impl_->group_id, index);
}

const MetadataStateMachine& DurableMetadataStateMachine::state() const noexcept {
  return impl_->metadata;
}
const GroupId& DurableMetadataStateMachine::group_id() const noexcept {
  return impl_->group_id;
}
bool DurableMetadataStateMachine::failed() const noexcept {
  return !impl_->failure.is_ok();
}
common::Status DurableMetadataStateMachine::failure_status() const {
  return impl_->failure;
}

} // namespace chronos::raft
