#include "chronos/query/temporal_command_executor.hpp"

#include "chronos/query/value.hpp"

#include <cstddef>
#include <cstdint>
#include <exception>
#include <new>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status internal(std::string message) {
  return common::Status{common::StatusCode::kInternal, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] common::Status
validate_wal_commit(const wal::WalCommitResult& commit,
                    const wal::WalDurabilityMode requested_durability) {
  if (commit.requested_durability != requested_durability ||
      commit.effective_durability != requested_durability) {
    return internal("WAL completion changed the requested temporal durability mode");
  }
  const wal::WalAppendResult& append = commit.append;
  if (append.record_sequence == 0U || !append.record_start.wal_id.is_valid() ||
      append.record_start.wal_id != append.record_end.wal_id ||
      append.record_start.segment_number == 0U ||
      append.record_start.segment_number != append.record_end.segment_number ||
      append.record_start.byte_offset < wal::kSegmentHeaderSize ||
      append.record_start.byte_offset >= append.record_end.byte_offset ||
      append.record_end.byte_offset > wal::kSegmentSizeLimit) {
    return internal("WAL completion returned an invalid temporal append position");
  }
  if (requested_durability == wal::WalDurabilityMode::kAsync) {
    if (commit.synchronization_position.has_value() || commit.durable_record_sequence.has_value()) {
      return internal("ASYNC temporal completion unexpectedly reported a sync frontier");
    }
    return common::Status::ok();
  }
  if (requested_durability != wal::WalDurabilityMode::kLocalSync) {
    return invalid("unknown temporal WAL durability mode");
  }
  if (!commit.synchronization_position.has_value() || !commit.durable_record_sequence.has_value() ||
      commit.synchronization_position->wal_id != append.record_start.wal_id ||
      commit.synchronization_position->segment_number == 0U ||
      commit.synchronization_position->byte_offset < wal::kSegmentHeaderSize ||
      commit.synchronization_position->byte_offset > wal::kSegmentSizeLimit ||
      *commit.durable_record_sequence < append.record_sequence) {
    return internal("LOCAL_SYNC temporal completion does not cover the appended command");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Result<std::vector<TemporalMutation>>
materialize(const columnar::OwnedColumnarBatch& batch,
            const std::vector<TemporalMutationDescriptor>& descriptors) {
  if (descriptors.empty() || descriptors.size() != batch.row_count()) {
    return common::make_unexpected(
        invalid("temporal execution mutation count does not match its batch"));
  }
  try {
    std::vector<TemporalMutation> mutations;
    mutations.reserve(descriptors.size());
    for (std::uint32_t row = 0U; row < batch.row_count(); ++row) {
      std::vector<ScalarValue> columns;
      columns.reserve(batch.columns().size());
      for (const columnar::OwnedColumnVector& column : batch.columns()) {
        auto cell = column.cell(row);
        if (!cell.has_value()) {
          return common::make_unexpected(cell.error());
        }
        auto scalar = ScalarValue::from_column_cell(column.type(), *cell);
        if (!scalar.has_value()) {
          return common::make_unexpected(scalar.error());
        }
        columns.push_back(std::move(*scalar));
      }
      mutations.push_back(TemporalMutation{.logical_identity = descriptors[row].logical_identity,
                                           .columns = std::move(columns),
                                           .event_time_ns = descriptors[row].event_time_ns,
                                           .receive_time_ns = descriptors[row].receive_time_ns,
                                           .wal_id = {},
                                           .record_sequence = 0U,
                                           .row_ordinal = row,
                                           .kind = descriptors[row].kind});
    }
    return mutations;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("temporal execution materialization allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("temporal execution materialization exceeded container limits"));
  } catch (const std::exception& error) {
    return common::make_unexpected(
        internal(std::string{"temporal execution materialization threw: "} + error.what()));
  }
}

template <typename Result>
[[nodiscard]] common::Result<Result> fail_after_admission(TemporalSnapshotProvider& provider,
                                                          common::Status status) {
  const common::Status failed = provider.fail_closed();
  return common::make_unexpected(failed.is_ok() ? std::move(status) : failed);
}

} // namespace

common::Result<TemporalCommandExecutionResult>
execute_temporal_command(TemporalCommandExecutionInput input, TemporalSnapshotProvider& provider,
                         wal::WalCommitCoordinator& wal_coordinator) {
  if (input.batch == nullptr) {
    return common::make_unexpected(invalid("temporal execution requires an owning batch"));
  }
  if (input.durability != wal::WalDurabilityMode::kAsync &&
      input.durability != wal::WalDurabilityMode::kLocalSync) {
    return common::make_unexpected(invalid("unknown temporal WAL durability mode"));
  }
  if (input.batch->schema().table_id() != provider.schema().table_id() ||
      input.batch->schema().schema_id() != provider.schema().schema_id() ||
      input.batch->schema().version() != provider.schema().version()) {
    return common::make_unexpected(
        invalid("temporal execution batch does not match the provider schema"));
  }

  auto mutations = materialize(*input.batch, input.mutations);
  if (!mutations.has_value()) {
    return common::make_unexpected(mutations.error());
  }
  const common::Status precommit =
      provider.validate_next_commit(input.system_commit_time_ns, *mutations);
  if (!precommit.is_ok()) {
    return common::make_unexpected(precommit);
  }
  auto encoded =
      encode_temporal_command_v1(*input.batch, input.mutations, input.system_commit_time_ns);
  if (!encoded.has_value()) {
    return common::make_unexpected(encoded.error());
  }
  auto completion = wal_coordinator.try_submit(encoded->bytes(), input.durability);
  if (!completion.has_value()) {
    return common::make_unexpected(completion.error());
  }

  auto wal_result = completion->wait();
  if (!wal_result.has_value()) {
    return fail_after_admission<TemporalCommandExecutionResult>(provider, wal_result.error());
  }
  const common::Status wal_status = validate_wal_commit(*wal_result, input.durability);
  if (!wal_status.is_ok()) {
    return fail_after_admission<TemporalCommandExecutionResult>(provider, wal_status);
  }
  for (TemporalMutation& mutation : *mutations) {
    mutation.wal_id = common::Uuid{wal_result->append.record_start.wal_id.bytes};
    mutation.record_sequence = wal_result->append.record_sequence;
  }
  const common::Status applied = provider.apply_committed(
      wal_result->append.record_sequence, input.system_commit_time_ns, std::move(*mutations));
  if (!applied.is_ok()) {
    return fail_after_admission<TemporalCommandExecutionResult>(provider, applied);
  }
  return TemporalCommandExecutionResult{
      .wal_commit = *wal_result,
      .application = {.system_commit_position = wal_result->append.record_sequence,
                      .applied_mutation_count = input.batch->row_count()}};
}

} // namespace chronos::query
