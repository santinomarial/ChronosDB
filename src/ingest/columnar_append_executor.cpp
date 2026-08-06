#include "chronos/ingest/columnar_append_executor.hpp"

#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/ingest/columnar_append.hpp"

#include <string>
#include <utility>

namespace chronos::ingest {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status unavailable(std::string message) {
  return common::Status{common::StatusCode::kUnavailable, std::move(message)};
}

[[nodiscard]] common::Status internal(std::string message) {
  return common::Status{common::StatusCode::kInternal, std::move(message)};
}

[[nodiscard]] common::Status conflict() {
  return common::Status{common::StatusCode::kAlreadyExists,
                        "client batch identity conflicts with a different mutation"};
}

[[nodiscard]] common::Status
validate_wal_commit(const wal::WalCommitResult& commit,
                    const wal::WalDurabilityMode requested_durability) {
  if (commit.requested_durability != requested_durability ||
      commit.effective_durability != requested_durability) {
    return internal("WAL completion changed the requested durability mode");
  }
  const wal::WalAppendResult& append = commit.append;
  if (append.record_sequence == 0U || !append.record_start.wal_id.is_valid() ||
      append.record_start.wal_id != append.record_end.wal_id ||
      append.record_start.segment_number == 0U ||
      append.record_start.segment_number != append.record_end.segment_number ||
      append.record_start.byte_offset >= append.record_end.byte_offset) {
    return internal("WAL completion returned an invalid append position");
  }
  if (requested_durability == wal::WalDurabilityMode::kAsync) {
    if (commit.synchronization_position.has_value() || commit.durable_record_sequence.has_value()) {
      return internal("ASYNC WAL completion unexpectedly reported a synchronization frontier");
    }
    return common::Status::ok();
  }
  if (requested_durability != wal::WalDurabilityMode::kLocalSync) {
    return invalid("unknown WAL durability mode");
  }
  if (!commit.synchronization_position.has_value() || !commit.durable_record_sequence.has_value() ||
      commit.synchronization_position->wal_id != append.record_start.wal_id ||
      *commit.durable_record_sequence < append.record_sequence) {
    return internal("LOCAL_SYNC WAL completion does not cover the appended command");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status cancel_before_wal(RetryReservation& reservation,
                                               PreparedTabletAppend& tablet_append) {
  common::Status tablet_status = tablet_append.cancel_before_wal();
  common::Status retry_status = reservation.cancel_before_wal();
  if (!tablet_status.is_ok()) {
    return tablet_status;
  }
  return retry_status;
}

} // namespace

common::Result<ColumnarAppendExecutionResult>
execute_columnar_append(const ColumnarAppendExecutionInput& input, RetryDirectory& retry_directory,
                        TabletState& tablet, wal::WalCommitCoordinator& wal_coordinator) {
  if (input.batch == nullptr) {
    return common::make_unexpected(invalid("columnar append execution requires an owning batch"));
  }

  common::Result<TabletSnapshot> before = tablet.snapshot();
  if (!before.has_value()) {
    return common::make_unexpected(before.error());
  }
  common::Result<columnar::EncodedColumnarBatch> encoded_batch =
      columnar::encode_columnar_batch_v1(*input.batch);
  if (!encoded_batch.has_value()) {
    return common::make_unexpected(encoded_batch.error());
  }
  common::Result<Sha256Digest> request_digest = compute_columnar_append_v1_request_digest(
      ColumnarAppendDigestInput{.table_id = input.batch->schema().table_id(),
                                .tablet_id = before->tablet_id(),
                                .schema_id = input.batch->schema().schema_id(),
                                .schema_version = input.batch->schema().version(),
                                .encoded_batch = encoded_batch->bytes()});
  if (!request_digest.has_value()) {
    return common::make_unexpected(request_digest.error());
  }

  const RetryIdentity retry_identity{.client_id = input.client_id,
                                     .client_batch_id = input.client_batch_id};
  const ColumnarAppendMutationIdentity mutation{.table_id = input.batch->schema().table_id(),
                                                .tablet_id = before->tablet_id(),
                                                .request_digest = *request_digest};
  common::Result<RetryDecision> decision = retry_directory.try_reserve(retry_identity, mutation);
  if (!decision.has_value()) {
    return common::make_unexpected(decision.error());
  }
  switch (decision->kind()) {
  case RetryDecisionKind::kMatchingCommitted:
    if (decision->committed_outcome() == nullptr) {
      return common::make_unexpected(internal("matching retry is missing its published outcome"));
    }
    return ColumnarAppendExecutionResult{.kind = ColumnarAppendExecutionKind::kMatchingRetry,
                                         .outcome = decision->committed_outcome(),
                                         .requested_durability = input.durability,
                                         .wal_commit = std::nullopt};
  case RetryDecisionKind::kInFlight:
    return common::make_unexpected(
        unavailable("client batch identity already has an in-flight mutation"));
  case RetryDecisionKind::kConflict:
    return common::make_unexpected(conflict());
  case RetryDecisionKind::kReserved:
    break;
  }
  if (decision->reservation() == nullptr) {
    return common::make_unexpected(internal("reserved retry decision has no owner handle"));
  }
  RetryReservation reservation = std::move(*decision->reservation());

  common::Result<wal::EncodedApplicationPayload> command =
      encode_columnar_append_v1(ColumnarAppendEncodeInput{.client_id = input.client_id,
                                                          .client_batch_id = input.client_batch_id,
                                                          .tablet_id = before->tablet_id()},
                                *encoded_batch);
  if (!command.has_value()) {
    return common::make_unexpected(command.error());
  }
  common::Result<PreparedTabletAppend> prepared =
      tablet.prepare_append(retry_identity, mutation, input.batch);
  if (!prepared.has_value()) {
    return common::make_unexpected(prepared.error());
  }

  common::Result<wal::WalCommitCompletion> completion =
      wal_coordinator.try_submit(command->bytes(), input.durability);
  if (!completion.has_value()) {
    const common::Status cancellation = cancel_before_wal(reservation, *prepared);
    return common::make_unexpected(cancellation.is_ok() ? completion.error() : cancellation);
  }

  common::Status retry_started = reservation.mark_wal_started();
  if (!retry_started.is_ok()) {
    static_cast<void>(prepared->mark_wal_started());
    static_cast<void>(tablet.fail_closed());
    static_cast<void>(completion->wait());
    return common::make_unexpected(retry_started);
  }
  common::Status tablet_started = prepared->mark_wal_started();
  if (!tablet_started.is_ok()) {
    static_cast<void>(tablet.fail_closed());
    static_cast<void>(completion->wait());
    return common::make_unexpected(tablet_started);
  }

  common::Result<wal::WalCommitResult> wal_result = completion->wait();
  if (!wal_result.has_value()) {
    return common::make_unexpected(wal_result.error());
  }
  const common::Status wal_status = validate_wal_commit(*wal_result, input.durability);
  if (!wal_status.is_ok()) {
    static_cast<void>(tablet.fail_closed());
    return common::make_unexpected(wal_status);
  }

  common::Result<TabletAppendResult> published = prepared->publish(
      head::HeadCommitPosition{.wal_id = wal_result->append.record_start.wal_id,
                               .record_sequence = wal_result->append.record_sequence});
  if (!published.has_value()) {
    return common::make_unexpected(published.error());
  }
  common::Result<std::shared_ptr<const ColumnarAppendRetryOutcome>> committed =
      reservation.commit_published(published->outcome);
  if (!committed.has_value()) {
    static_cast<void>(tablet.fail_closed());
    return common::make_unexpected(committed.error());
  }
  if (committed->get() != published->outcome.get()) {
    static_cast<void>(tablet.fail_closed());
    return common::make_unexpected(
        internal("global retry directory changed the tablet-published outcome object"));
  }

  return ColumnarAppendExecutionResult{.kind = ColumnarAppendExecutionKind::kApplied,
                                       .outcome = std::move(*committed),
                                       .requested_durability = input.durability,
                                       .wal_commit = std::move(*wal_result)};
}

} // namespace chronos::ingest
