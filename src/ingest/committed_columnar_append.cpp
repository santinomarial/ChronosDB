#include "chronos/ingest/committed_columnar_append.hpp"

#include "chronos/columnar/column_vector.hpp"

#include <exception>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace chronos::ingest {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status conflict() {
  return common::Status{common::StatusCode::kAlreadyExists,
                        "committed client batch identity conflicts with another mutation"};
}

[[nodiscard]] common::Status unavailable(std::string message) {
  return common::Status{common::StatusCode::kUnavailable, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] common::Status internal(std::string message) {
  return common::Status{common::StatusCode::kInternal, std::move(message)};
}

[[nodiscard]] common::Result<std::shared_ptr<const columnar::OwnedColumnarBatch>>
own_batch(const columnar::DecodedColumnarBatchView& decoded,
          std::shared_ptr<const schema::TableSchema> schema,
          const ColumnarAppendDecodeLimits& limits) {
  try {
    std::vector<columnar::OwnedColumnVector> columns;
    columns.reserve(decoded.columns().size());
    for (const columnar::ColumnVectorView& view : decoded.columns()) {
      columnar::ColumnVectorBuffers buffers{
          .validity = std::vector<std::byte>{view.validity().begin(), view.validity().end()},
          .offsets = std::vector<std::byte>{view.offsets().begin(), view.offsets().end()},
          .values = std::vector<std::byte>{view.values().begin(), view.values().end()},
      };
      auto column = columnar::OwnedColumnVector::create(
          columnar::ColumnVectorMetadata{.column_id = view.column_id(),
                                         .type = view.type(),
                                         .nullable = view.nullable(),
                                         .row_count = view.row_count(),
                                         .null_count = view.null_count()},
          std::move(buffers));
      if (!column.has_value()) {
        return common::make_unexpected(column.error());
      }
      columns.push_back(std::move(*column));
    }
    auto batch = columnar::OwnedColumnarBatch::create(
        std::move(schema), std::move(columns),
        columnar::ColumnarBatchLimits{.max_rows = limits.batch.max_rows,
                                      .max_columns = limits.batch.max_columns,
                                      .max_buffer_bytes = limits.batch.max_batch_length,
                                      .max_retained_buffer_bytes = limits.batch.max_batch_length});
    if (!batch.has_value()) {
      return common::make_unexpected(batch.error());
    }
    return std::make_shared<const columnar::OwnedColumnarBatch>(std::move(*batch));
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("committed batch allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("committed batch exceeds container limits"));
  }
}

[[nodiscard]] common::Result<CommittedColumnarAppendResult>
matching_result(const DecodedColumnarAppendView& command, const RetryIdentity& retry_identity,
                const ColumnarAppendMutationIdentity& mutation,
                const std::shared_ptr<const ColumnarAppendRetryOutcome>& outcome,
                const head::HeadCommitPosition& position, TabletState& tablet) {
  if (outcome == nullptr || outcome->applied_row_count != command.row_count()) {
    return common::make_unexpected(
        invalid("matching committed retry disagrees with its decoded command"));
  }
  auto current = tablet.snapshot();
  if (!current.has_value()) {
    return common::make_unexpected(current.error());
  }
  if (current->retry_outcome(retry_identity).get() != outcome.get()) {
    return common::make_unexpected(
        invalid("matching committed retry is absent from the owning tablet"));
  }
  if (current->applied_position().has_value() && *current->applied_position() == position) {
    return CommittedColumnarAppendResult{CommittedColumnarAppendKind::kMatchingRetry,
                                         std::move(*current), outcome};
  }
  auto advanced = tablet.advance_recovered_retry(retry_identity, mutation, outcome, position);
  if (!advanced.has_value()) {
    return common::make_unexpected(advanced.error());
  }
  return CommittedColumnarAppendResult{CommittedColumnarAppendKind::kMatchingRetry,
                                       std::move(*advanced), outcome};
}

} // namespace

common::Result<CommittedColumnarAppendResult>
apply_committed_columnar_append(const DecodedColumnarAppendView& command,
                                std::shared_ptr<const schema::TableSchema> retained_schema,
                                const head::HeadCommitPosition position,
                                RetryDirectory& retry_directory, TabletState& tablet,
                                const ColumnarAppendDecodeLimits limits) {
  if (retained_schema == nullptr || !position.is_valid()) {
    return common::make_unexpected(
        invalid("committed append requires a schema and valid commit position"));
  }
  const common::Status schema_status = validate_columnar_append_schema(command, *retained_schema);
  if (!schema_status.is_ok()) {
    return common::make_unexpected(schema_status);
  }
  auto before = tablet.snapshot();
  if (!before.has_value()) {
    return common::make_unexpected(before.error());
  }
  if (before->table_id() != command.table_id() || before->tablet_id() != command.tablet_id()) {
    return common::make_unexpected(
        invalid("committed append command does not target the owning tablet"));
  }

  const RetryIdentity retry_identity{command.client_id(), command.client_batch_id()};
  const ColumnarAppendMutationIdentity mutation{command.table_id(), command.tablet_id(),
                                                command.request_digest()};
  auto decision = retry_directory.try_reserve(retry_identity, mutation);
  if (!decision.has_value()) {
    return common::make_unexpected(decision.error());
  }
  switch (decision->kind()) {
  case RetryDecisionKind::kConflict:
    return common::make_unexpected(conflict());
  case RetryDecisionKind::kInFlight:
    return common::make_unexpected(
        unavailable("committed append encountered an in-flight retry identity"));
  case RetryDecisionKind::kMatchingCommitted:
    return matching_result(command, retry_identity, mutation, decision->committed_outcome(),
                           position, tablet);
  case RetryDecisionKind::kReserved:
    break;
  }
  if (decision->reservation() == nullptr) {
    return common::make_unexpected(internal("committed append reservation has no owner"));
  }
  RetryReservation reservation = std::move(*decision->reservation());
  auto batch = own_batch(command.batch(), std::move(retained_schema), limits);
  if (!batch.has_value()) {
    return common::make_unexpected(batch.error());
  }
  auto prepared = tablet.prepare_append(retry_identity, mutation, std::move(*batch));
  if (!prepared.has_value()) {
    return common::make_unexpected(prepared.error());
  }
  common::Status status = reservation.mark_wal_started();
  if (!status.is_ok()) {
    static_cast<void>(prepared->mark_wal_started());
    static_cast<void>(tablet.fail_closed());
    return common::make_unexpected(status);
  }
  status = prepared->mark_wal_started();
  if (!status.is_ok()) {
    static_cast<void>(tablet.fail_closed());
    return common::make_unexpected(status);
  }
  auto published = prepared->publish(position);
  if (!published.has_value()) {
    return common::make_unexpected(published.error());
  }
  auto committed = reservation.commit_published(published->outcome);
  if (!committed.has_value()) {
    static_cast<void>(tablet.fail_closed());
    return common::make_unexpected(committed.error());
  }
  if (committed->get() != published->outcome.get()) {
    static_cast<void>(tablet.fail_closed());
    return common::make_unexpected(
        internal("committed append changed the tablet-published retry outcome"));
  }
  return CommittedColumnarAppendResult{CommittedColumnarAppendKind::kApplied,
                                       std::move(published->snapshot), std::move(*committed)};
}

} // namespace chronos::ingest
