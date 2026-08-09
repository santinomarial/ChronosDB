#include "chronos/query/committed_temporal_command.hpp"

#include "chronos/columnar/column_vector.hpp"
#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/query/value.hpp"

#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
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

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

[[nodiscard]] common::Status internal(std::string message) {
  return common::Status{common::StatusCode::kInternal, std::move(message)};
}

[[nodiscard]] common::Result<std::vector<TemporalMutation>>
materialize_mutations(const DecodedTemporalCommandView& command,
                      const schema::TableSchema& retained_schema,
                      const std::uint64_t system_commit_position, const wal::WalId wal_id) {
  if (system_commit_position == 0U || !wal_id.is_valid()) {
    return common::make_unexpected(
        invalid("committed temporal command requires a valid WAL source position"));
  }
  const common::Status schema_status =
      columnar::validate_columnar_batch_schema(command.batch(), retained_schema);
  if (!schema_status.is_ok()) {
    return common::make_unexpected(schema_status);
  }
  if (command.mutations().size() != command.batch().row_count() ||
      command.mutations().size() > std::numeric_limits<std::uint32_t>::max()) {
    return common::make_unexpected(
        invalid("committed temporal command row metadata does not match its batch"));
  }
  try {
    std::vector<TemporalMutation> mutations;
    mutations.reserve(command.mutations().size());
    for (std::uint32_t row = 0U; row < command.batch().row_count(); ++row) {
      const DecodedTemporalMutationDescriptor& descriptor = command.mutations()[row];
      std::vector<ScalarValue> columns;
      columns.reserve(command.batch().columns().size());
      for (const columnar::ColumnVectorView& column : command.batch().columns()) {
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
      mutations.push_back(TemporalMutation{
          .logical_identity = std::vector<std::byte>{descriptor.logical_identity.begin(),
                                                     descriptor.logical_identity.end()},
          .columns = std::move(columns),
          .event_time_ns = descriptor.event_time_ns,
          .receive_time_ns = descriptor.receive_time_ns,
          .wal_id = common::Uuid{wal_id.bytes},
          .record_sequence = system_commit_position,
          .row_ordinal = row,
          .kind = descriptor.kind});
    }
    return mutations;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("committed temporal materialization allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("committed temporal materialization exceeded container limits"));
  } catch (const std::exception& error) {
    return common::make_unexpected(
        internal(std::string{"committed temporal materialization threw: "} + error.what()));
  } catch (...) {
    return common::make_unexpected(
        internal("committed temporal materialization threw an unknown exception"));
  }
}

} // namespace

common::Result<CommittedTemporalCommandResult>
apply_committed_temporal_command(const DecodedTemporalCommandView& command,
                                 const schema::TableSchema& retained_schema,
                                 const std::uint64_t system_commit_position,
                                 const wal::WalId wal_id, TemporalSnapshotProvider& provider) {
  common::Result<std::vector<TemporalMutation>> mutations =
      materialize_mutations(command, retained_schema, system_commit_position, wal_id);
  if (!mutations.has_value()) {
    return common::make_unexpected(mutations.error());
  }
  common::Status applied = provider.apply_committed(
      system_commit_position, command.system_commit_time_ns(), std::move(*mutations));
  if (!applied.is_ok()) {
    return common::make_unexpected(std::move(applied));
  }
  return CommittedTemporalCommandResult{.system_commit_position = system_commit_position,
                                        .applied_mutation_count =
                                            static_cast<std::uint32_t>(command.mutations().size())};
}

common::Result<CommittedTemporalCommandResult> verify_retained_temporal_command(
    const DecodedTemporalCommandView& command, const schema::TableSchema& retained_schema,
    const std::uint64_t system_commit_position, const wal::WalId wal_id,
    const TemporalSnapshotProvider& provider) {
  common::Result<std::vector<TemporalMutation>> mutations =
      materialize_mutations(command, retained_schema, system_commit_position, wal_id);
  if (!mutations.has_value()) {
    return common::make_unexpected(mutations.error());
  }
  common::Status verified = provider.verify_retained_commit(
      system_commit_position, command.system_commit_time_ns(), *mutations);
  if (!verified.is_ok()) {
    return common::make_unexpected(std::move(verified));
  }
  return CommittedTemporalCommandResult{.system_commit_position = system_commit_position,
                                        .applied_mutation_count =
                                            static_cast<std::uint32_t>(mutations->size())};
}

} // namespace chronos::query
