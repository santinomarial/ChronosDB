#include "chronos/service/replicated_ingest_operation.hpp"

#include "chronos/ingest/retry_directory.hpp"

#include <memory>
#include <new>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace chronos::service {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status unavailable(std::string message) {
  return {common::StatusCode::kUnavailable, std::move(message)};
}

[[nodiscard]] common::Status corruption(std::string message) {
  return {common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return {common::StatusCode::kResourceExhausted, std::move(message)};
}

} // namespace

class ReplicatedIngestOperation::Impl {
public:
  enum class Phase : std::uint8_t { kProposal, kReceipt, kComplete };

  Impl(raft::GroupId group, const raft::Term term, ingest::RetryIdentity retry,
       ingest::ColumnarAppendMutationIdentity mutation,
       const ingest::ColumnarAppendDecodeLimits limits,
       raft::AsyncDurableMultiRaftRuntime& configured_runtime,
       ingest::AsyncRaftTabletApplication& configured_application,
       raft::AsyncDurableRaftCompletion configured_proposal) noexcept
      : group_id(group), leader_term(term), retry_identity(retry), mutation_identity(mutation),
        decode_limits(limits), runtime(&configured_runtime), application(&configured_application),
        proposal(std::move(configured_proposal)) {}

  [[nodiscard]] common::Result<std::optional<ReplicatedIngestResult>> poll() {
    if (phase == Phase::kComplete)
      return common::make_unexpected(invalid("replicated ingest operation is already complete"));
    if (phase == Phase::kProposal) {
      if (!proposal.is_ready())
        return std::optional<ReplicatedIngestResult>{};
      auto result = proposal.wait();
      if (!result.has_value())
        return common::make_unexpected(result.error());
      if (result->size() != 1U)
        return common::make_unexpected(
            corruption("replicated ingest proposal result is not singular"));
      const raft::DurableRaftResult& proposed = result->front();
      if (!proposed.status.is_ok())
        return common::make_unexpected(proposed.status);
      if (!proposed.transition.has_value() || !proposed.transition->persistence.has_value())
        return common::make_unexpected(corruption("replicated ingest proposal did not persist"));
      const raft::GroupPersistentState& persistence = *proposed.transition->persistence;
      if (persistence.group_id != group_id || persistence.state.current_term != leader_term ||
          persistence.state.log.empty())
        return common::make_unexpected(corruption("replicated ingest proposal identity changed"));
      const raft::LogEntry& entry = persistence.state.log.back();
      if (entry.index == 0U || entry.term != leader_term ||
          entry.type != ingest::kRaftColumnarAppendEntryType)
        return common::make_unexpected(corruption("replicated ingest log entry changed"));
      auto decoded = ingest::decode_columnar_append_v1_exact(entry.payload, decode_limits);
      if (!decoded.has_value())
        return common::make_unexpected(corruption("replicated ingest persisted invalid command"));
      const ingest::RetryIdentity decoded_retry{decoded->client_id(), decoded->client_batch_id()};
      const ingest::ColumnarAppendMutationIdentity decoded_mutation{
          decoded->table_id(), decoded->tablet_id(), decoded->request_digest()};
      if (decoded_retry != retry_identity || decoded_mutation != mutation_identity)
        return common::make_unexpected(
            corruption("replicated ingest persisted a different command"));
      auto waiting = application->request_quorum_sync(*runtime, group_id, leader_term, entry.index);
      if (!waiting.has_value())
        return common::make_unexpected(waiting.error());
      receipt.emplace(std::move(*waiting));
      phase = Phase::kReceipt;
    }
    if (!receipt.has_value())
      return common::make_unexpected(corruption("replicated ingest receipt owner is absent"));
    if (!receipt->is_ready())
      return std::optional<ReplicatedIngestResult>{};
    auto proved = receipt->wait();
    if (!proved.has_value())
      return common::make_unexpected(proved.error());
    auto snapshot = application->snapshot(group_id);
    if (!snapshot.has_value())
      return common::make_unexpected(snapshot.error());
    const auto outcome = snapshot->retry_outcome(retry_identity);
    if (outcome == nullptr || outcome->mutation != mutation_identity ||
        outcome->commit_source != head::CommitSource::kRaft || outcome->raft_group_id != group_id ||
        outcome->record_sequence == 0U || outcome->record_sequence > proved->log_index)
      return common::make_unexpected(corruption("replicated ingest retry outcome changed"));
    phase = Phase::kComplete;
    return std::optional<ReplicatedIngestResult>{
        ReplicatedIngestResult{.outcome = outcome->record_sequence == proved->log_index
                                              ? network::IngestOutcome::kApplied
                                              : network::IngestOutcome::kMatchingRetry,
                               .applied_row_count = outcome->applied_row_count,
                               .receipt = *proved}};
  }

  raft::GroupId group_id;
  raft::Term leader_term{};
  ingest::RetryIdentity retry_identity;
  ingest::ColumnarAppendMutationIdentity mutation_identity;
  ingest::ColumnarAppendDecodeLimits decode_limits;
  raft::AsyncDurableMultiRaftRuntime* runtime;
  ingest::AsyncRaftTabletApplication* application;
  raft::AsyncDurableRaftCompletion proposal;
  std::optional<ingest::AsyncRaftTabletQuorumCompletion> receipt;
  Phase phase{Phase::kProposal};
};

ReplicatedIngestOperation::ReplicatedIngestOperation(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
ReplicatedIngestOperation::~ReplicatedIngestOperation() = default;
ReplicatedIngestOperation::ReplicatedIngestOperation(ReplicatedIngestOperation&&) noexcept =
    default;
ReplicatedIngestOperation&
ReplicatedIngestOperation::operator=(ReplicatedIngestOperation&&) noexcept = default;

common::Result<ReplicatedIngestOperation>
ReplicatedIngestOperation::submit(raft::GroupId group_id, const raft::Term required_leader_term,
                                  std::vector<std::byte> encoded_columnar_append,
                                  raft::AsyncDurableMultiRaftRuntime& runtime,
                                  ingest::AsyncRaftTabletApplication& application,
                                  const ingest::ColumnarAppendDecodeLimits decode_limits) {
  if (group_id.is_nil() || required_leader_term == 0U || encoded_columnar_append.empty())
    return common::make_unexpected(invalid("replicated ingest submission identity is invalid"));
  if (!runtime.owns_worker_extension(application))
    return common::make_unexpected(invalid("replicated ingest uses a different application owner"));
  auto decoded = ingest::decode_columnar_append_v1_exact(encoded_columnar_append, decode_limits);
  if (!decoded.has_value())
    return common::make_unexpected(decoded.error().status());
  const ingest::RetryIdentity retry{decoded->client_id(), decoded->client_batch_id()};
  const ingest::ColumnarAppendMutationIdentity mutation{decoded->table_id(), decoded->tablet_id(),
                                                        decoded->request_digest()};
  auto proposal = runtime.try_submit(
      {raft::DurableRaftRequest{group_id,
                                raft::ProposeOperation{ingest::kRaftColumnarAppendEntryType,
                                                       std::move(encoded_columnar_append)},
                                required_leader_term}});
  if (!proposal.has_value())
    return common::make_unexpected(proposal.error());
  try {
    return ReplicatedIngestOperation{std::make_unique<Impl>(group_id, required_leader_term, retry,
                                                            mutation, decode_limits, runtime,
                                                            application, std::move(*proposal))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("replicated ingest operation allocation failed"));
  }
}

common::Result<std::optional<ReplicatedIngestResult>> ReplicatedIngestOperation::poll() {
  if (impl_ == nullptr)
    return common::make_unexpected(unavailable("replicated ingest operation is invalid"));
  return impl_->poll();
}

common::Result<std::vector<std::byte>>
encode_replicated_ingest_acknowledgement(const ReplicatedIngestResult& result) {
  return network::encode_quorum_sync_ingest_acknowledgement(
      {.outcome = result.outcome,
       .group_id = result.receipt.group_id,
       .leader_node_id = result.receipt.leader_node_id,
       .leader_term = result.receipt.leader_term,
       .log_index = result.receipt.log_index,
       .entry_term = result.receipt.entry_term,
       .local_durable_physical_sequence = result.receipt.local_durable_physical_sequence});
}

} // namespace chronos::service
