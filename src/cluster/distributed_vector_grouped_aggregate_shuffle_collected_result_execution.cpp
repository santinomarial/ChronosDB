#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_collected_result_execution.hpp"

#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/query/vector_chunk.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

namespace chronos::cluster {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] bool valid_limits(
    const DistributedVectorGroupedAggregateShuffleCollectedResultExecutionLimits& limits) noexcept {
  return validate_distributed_vector_grouped_aggregate_shuffle_result_stream_limits(
             limits.stream) &&
         limits.maximum_batch_working_bytes > 0U &&
         limits.maximum_batch_working_bytes <=
             kMaximumDistributedVectorGroupedAggregateShuffleCollectedResultWorkingBytes &&
         limits.maximum_working_memory_bytes >= limits.maximum_batch_working_bytes &&
         limits.maximum_working_memory_bytes <=
             kMaximumDistributedVectorGroupedAggregateShuffleCollectedResultWorkingBytes;
}

[[nodiscard]] bool
raw_schema_matches(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
                   const query::DistributedVectorResultSchema& result_schema) {
  const auto keys = authority.key_definitions();
  const auto aggregates = authority.aggregate_definitions();
  if (result_schema.columns.size() != keys.size() + aggregates.size())
    return false;
  for (std::size_t ordinal = 0U; ordinal < keys.size(); ++ordinal) {
    if (result_schema.columns[ordinal].type != keys[ordinal].type ||
        result_schema.columns[ordinal].nullable != keys[ordinal].nullable) {
      return false;
    }
  }
  for (std::size_t ordinal = 0U; ordinal < aggregates.size(); ++ordinal) {
    auto shape = query::vector_aggregate_output_shape(aggregates[ordinal]);
    if (!shape.has_value() || result_schema.columns[keys.size() + ordinal].type != shape->type ||
        result_schema.columns[keys.size() + ordinal].nullable != shape->nullable) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] bool
descriptors_match(const network::QueryResultBatchView& batch,
                  const query::DistributedVectorResultSchema& expected) noexcept {
  if (batch.columns().size() != expected.columns.size())
    return false;
  for (std::size_t ordinal = 0U; ordinal < expected.columns.size(); ++ordinal) {
    const auto& actual = batch.columns()[ordinal];
    const auto& wanted = expected.columns[ordinal];
    if (actual.name != wanted.name || actual.type != wanted.type ||
        actual.nullable != wanted.nullable) {
      return false;
    }
  }
  return true;
}

[[nodiscard]] std::size_t fixed_width(const schema::LogicalTypeKind kind) noexcept {
  using schema::LogicalTypeKind;
  switch (kind) {
  case LogicalTypeKind::kBool:
  case LogicalTypeKind::kInt8:
  case LogicalTypeKind::kUInt8:
    return 1U;
  case LogicalTypeKind::kInt16:
  case LogicalTypeKind::kUInt16:
    return 2U;
  case LogicalTypeKind::kInt32:
  case LogicalTypeKind::kUInt32:
  case LogicalTypeKind::kFloat32:
  case LogicalTypeKind::kDate:
    return 4U;
  case LogicalTypeKind::kInt64:
  case LogicalTypeKind::kUInt64:
  case LogicalTypeKind::kFloat64:
  case LogicalTypeKind::kTimestampNs:
    return 8U;
  case LogicalTypeKind::kDecimal:
  case LogicalTypeKind::kUuid:
    return 16U;
  case LogicalTypeKind::kSymbol:
  case LogicalTypeKind::kString:
  case LogicalTypeKind::kBinary:
    return 0U;
  }
  return 0U;
}

[[nodiscard]] common::Result<std::size_t>
materialized_batch_bytes(const network::QueryResultBatchView& batch,
                         const query::DistributedVectorResultSchema& expected_schema) {
  const std::size_t rows = batch.row_count();
  auto total = common::checked_multiply(rows, sizeof(std::uint32_t));
  if (!total.has_value())
    return common::make_unexpected(exhausted("collected result selection size overflows"));
  for (std::size_t column = 0U; column < expected_schema.columns.size(); ++column) {
    const auto& descriptor = expected_schema.columns[column];
    if (descriptor.nullable) {
      total = common::checked_add(*total, columnar::bitmap_size(batch.row_count()));
      if (!total.has_value())
        return common::make_unexpected(exhausted("collected result validity size overflows"));
    }
    if (descriptor.type.kind() == schema::LogicalTypeKind::kBool) {
      total = common::checked_add(*total, columnar::bitmap_size(batch.row_count()));
      if (!total.has_value())
        return common::make_unexpected(exhausted("collected result Boolean size overflows"));
      continue;
    }
    if (descriptor.type.is_variable_width()) {
      const auto offsets = common::checked_multiply(rows + 1U, sizeof(std::uint32_t));
      total = offsets.has_value() ? common::checked_add(*total, *offsets) : std::nullopt;
      if (!total.has_value())
        return common::make_unexpected(exhausted("collected result offsets size overflows"));
      for (std::uint32_t row = 0U; row < batch.row_count(); ++row) {
        const network::QueryResultCell* cell = batch.cell(row, column);
        if (cell == nullptr)
          return common::make_unexpected(corruption("collected result cell is absent"));
        total = common::checked_add(*total, cell->value.size());
        if (!total.has_value())
          return common::make_unexpected(exhausted("collected result variable size overflows"));
      }
      continue;
    }
    const std::size_t width = fixed_width(descriptor.type.kind());
    const auto values = common::checked_multiply(rows, width);
    total = values.has_value() ? common::checked_add(*total, *values) : std::nullopt;
    if (width == 0U || !total.has_value())
      return common::make_unexpected(exhausted("collected result fixed size overflows"));
  }
  return *total;
}

void set_bitmap(std::vector<std::byte>& bytes, const std::uint32_t row) noexcept {
  bytes[row / 8U] |= static_cast<std::byte>(1U << (row % 8U));
}

void store_u32(std::vector<std::byte>& bytes, const std::size_t offset,
               const std::uint32_t value) noexcept {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & std::uint32_t{0xffU});
  }
}

[[nodiscard]] common::Result<query::AccountedVectorChunk> materialize_batch(
    const network::QueryResultBatchView& batch,
    const query::DistributedVectorResultSchema& expected_schema,
    const DistributedVectorGroupedAggregateShuffleCollectedResultExecutionLimits& limits,
    const query::QueryResourceContext& resources) {
  try {
    auto retained = materialized_batch_bytes(batch, expected_schema);
    if (!retained.has_value())
      return common::make_unexpected(retained.error());
    if (*retained > limits.maximum_batch_working_bytes)
      return common::make_unexpected(exhausted("collected result batch memory is exhausted"));
    auto reservation = resources.reserve(*retained);
    if (!reservation.has_value())
      return common::make_unexpected(reservation.error());

    std::vector<columnar::OwnedPhysicalColumn> columns;
    columns.reserve(expected_schema.columns.size());
    for (std::size_t column = 0U; column < expected_schema.columns.size(); ++column) {
      const auto& descriptor = expected_schema.columns[column];
      columnar::ColumnVectorBuffers buffers;
      if (descriptor.nullable)
        buffers.validity.resize(columnar::bitmap_size(batch.row_count()));
      std::uint32_t null_count{};
      if (descriptor.type.kind() == schema::LogicalTypeKind::kBool) {
        buffers.values.resize(columnar::bitmap_size(batch.row_count()));
      } else if (descriptor.type.is_variable_width()) {
        std::size_t payload_bytes{};
        for (std::uint32_t row = 0U; row < batch.row_count(); ++row) {
          const network::QueryResultCell* cell = batch.cell(row, column);
          if (cell == nullptr)
            return common::make_unexpected(corruption("collected result cell is absent"));
          const auto next = common::checked_add(payload_bytes, cell->value.size());
          if (!next.has_value() || *next > std::numeric_limits<std::uint32_t>::max()) {
            return common::make_unexpected(
                exhausted("collected result variable payload is too large"));
          }
          payload_bytes = *next;
        }
        buffers.offsets.resize((static_cast<std::size_t>(batch.row_count()) + 1U) *
                               sizeof(std::uint32_t));
        buffers.values.resize(payload_bytes);
      } else {
        buffers.values.resize(static_cast<std::size_t>(batch.row_count()) *
                              fixed_width(descriptor.type.kind()));
      }

      std::size_t variable_offset{};
      for (std::uint32_t row = 0U; row < batch.row_count(); ++row) {
        const network::QueryResultCell* cell = batch.cell(row, column);
        if (cell == nullptr)
          return common::make_unexpected(corruption("collected result cell is absent"));
        if (descriptor.type.is_variable_width()) {
          store_u32(buffers.offsets, static_cast<std::size_t>(row) * sizeof(std::uint32_t),
                    static_cast<std::uint32_t>(variable_offset));
        }
        if (cell->is_null) {
          ++null_count;
          continue;
        }
        if (descriptor.nullable)
          set_bitmap(buffers.validity, row);
        if (descriptor.type.kind() == schema::LogicalTypeKind::kBool) {
          if (cell->value.size() != 1U ||
              (cell->value.front() != std::byte{} && cell->value.front() != std::byte{1U})) {
            return common::make_unexpected(corruption("collected result Boolean is invalid"));
          }
          if (cell->value.front() == std::byte{1U})
            set_bitmap(buffers.values, row);
        } else if (descriptor.type.is_variable_width()) {
          std::ranges::copy(cell->value,
                            buffers.values.begin() + static_cast<std::ptrdiff_t>(variable_offset));
          variable_offset += cell->value.size();
        } else {
          const std::size_t width = fixed_width(descriptor.type.kind());
          if (cell->value.size() != width)
            return common::make_unexpected(corruption("collected result fixed width is invalid"));
          std::ranges::copy(cell->value,
                            buffers.values.begin() +
                                static_cast<std::ptrdiff_t>(static_cast<std::size_t>(row) * width));
        }
      }
      if (descriptor.type.is_variable_width()) {
        store_u32(buffers.offsets,
                  static_cast<std::size_t>(batch.row_count()) * sizeof(std::uint32_t),
                  static_cast<std::uint32_t>(variable_offset));
      }
      auto owned = columnar::OwnedPhysicalColumn::create({.type = descriptor.type,
                                                          .nullable = descriptor.nullable,
                                                          .row_count = batch.row_count(),
                                                          .null_count = null_count},
                                                         std::move(buffers));
      if (!owned.has_value())
        return common::make_unexpected(corruption("collected result column is not canonical"));
      columns.push_back(std::move(*owned));
    }
    auto selection = query::VectorSelection::all(batch.row_count());
    if (!selection.has_value())
      return common::make_unexpected(selection.error());
    auto chunk = query::VectorChunk::create(
        std::move(columns), std::move(*selection),
        {.maximum_rows = limits.stream.frame.result_batch.maximum_rows,
         .maximum_columns = limits.stream.frame.result_batch.maximum_columns,
         .maximum_buffer_bytes = limits.maximum_batch_working_bytes,
         .maximum_retained_buffer_bytes = limits.maximum_batch_working_bytes});
    if (!chunk.has_value())
      return common::make_unexpected(chunk.error());
    return query::AccountedVectorChunk::create(std::move(*chunk), std::move(*reservation),
                                               resources);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("collected result batch allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("collected result batch exceeds limits"));
  }
}

} // namespace

class DistributedVectorGroupedAggregateShuffleCollectedResultExecution::Impl {
public:
  Impl(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
       const query::DistributedVectorResultSchema& result_schema,
       query::QueryResourceContext resources,
       std::vector<DistributedVectorGroupedAggregateShuffleCompleteResultStream> streams,
       const DistributedVectorGroupedAggregateShuffleCollectedResultExecutionLimits limits)
      : authority_(authority), result_schema_(result_schema), resources_(std::move(resources)),
        streams_(std::move(streams)), limits_(limits) {
    metrics_.total_partitions = streams_.size();
  }

  [[nodiscard]] common::Result<query::PhysicalOperatorStep> fail(common::Status status) {
    if (!failed_) {
      failure_ = std::move(status);
      failed_ = true;
    }
    return common::make_unexpected(failure_);
  }

  std::reference_wrapper<const DistributedVectorGroupedAggregateShuffleAuthority> authority_;
  std::reference_wrapper<const query::DistributedVectorResultSchema> result_schema_;
  query::QueryResourceContext resources_;
  std::vector<DistributedVectorGroupedAggregateShuffleCompleteResultStream> streams_;
  DistributedVectorGroupedAggregateShuffleCollectedResultExecutionLimits limits_;
  std::size_t partition_index_{};
  std::size_t batch_index_{};
  DistributedVectorGroupedAggregateShuffleCollectedResultExecutionMetrics metrics_;
  common::Status failure_{common::StatusCode::kInternal,
                          "collected grouped shuffle result execution has not failed"};
  bool failed_{};
  bool complete_{};
};

DistributedVectorGroupedAggregateShuffleCollectedResultExecution::
    DistributedVectorGroupedAggregateShuffleCollectedResultExecution() noexcept = default;
DistributedVectorGroupedAggregateShuffleCollectedResultExecution::
    ~DistributedVectorGroupedAggregateShuffleCollectedResultExecution() = default;
DistributedVectorGroupedAggregateShuffleCollectedResultExecution::
    DistributedVectorGroupedAggregateShuffleCollectedResultExecution(
        std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
DistributedVectorGroupedAggregateShuffleCollectedResultExecution::
    DistributedVectorGroupedAggregateShuffleCollectedResultExecution(
        DistributedVectorGroupedAggregateShuffleCollectedResultExecution&&) noexcept = default;
DistributedVectorGroupedAggregateShuffleCollectedResultExecution&
DistributedVectorGroupedAggregateShuffleCollectedResultExecution::operator=(
    DistributedVectorGroupedAggregateShuffleCollectedResultExecution&&) noexcept = default;

common::Result<DistributedVectorGroupedAggregateShuffleCollectedResultExecution>
DistributedVectorGroupedAggregateShuffleCollectedResultExecution::create(
    const DistributedVectorGroupedAggregateShuffleAuthority& authority,
    const query::DistributedVectorResultSchema& result_schema,
    std::vector<DistributedVectorGroupedAggregateShuffleCompleteResultStream> streams,
    const DistributedVectorGroupedAggregateShuffleCollectedResultExecutionLimits limits) {
  if (!valid_limits(limits) || streams.size() != authority.partition_count() || streams.empty() ||
      !query::validate_distributed_vector_result_schema_value(result_schema).is_ok() ||
      !raw_schema_matches(authority, result_schema)) {
    return common::make_unexpected(
        invalid("collected grouped shuffle result configuration is invalid"));
  }
  const raft::NodeId coordinator_node_id = streams.front().target_node_id;
  for (std::size_t partition = 0U; partition < streams.size(); ++partition) {
    const auto source = authority.destination_node(static_cast<std::uint32_t>(partition));
    const auto& stream = streams[partition];
    if (!source.has_value() || stream.query_id != authority.query_id() ||
        stream.partition_id != partition || stream.source_node_id != *source ||
        stream.target_node_id != coordinator_node_id || coordinator_node_id == 0U ||
        stream.source_node_id == coordinator_node_id) {
      return common::make_unexpected(
          invalid("collected grouped shuffle result coverage is invalid"));
    }
    auto canonical = DistributedVectorGroupedAggregateShuffleResultStreamSender::create(
        authority, result_schema, stream.partition_id, stream.source_node_id, coordinator_node_id,
        stream.encoded_result_batches, limits.stream);
    if (!canonical.has_value())
      return common::make_unexpected(canonical.error());
    if (canonical->frame_count() != stream.frame_count ||
        canonical->encoded_bytes() != stream.encoded_bytes) {
      return common::make_unexpected(
          invalid("collected grouped shuffle result extent is not canonical"));
    }
  }
  auto resources = query::QueryResourceContext::create(limits.maximum_working_memory_bytes);
  if (!resources.has_value())
    return common::make_unexpected(resources.error());
  try {
    return DistributedVectorGroupedAggregateShuffleCollectedResultExecution{std::make_unique<Impl>(
        authority, result_schema, std::move(*resources), std::move(streams), limits)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("collected grouped shuffle result allocation failed"));
  }
}

common::Result<query::PhysicalOperatorStep>
DistributedVectorGroupedAggregateShuffleCollectedResultExecution::next() {
  if (!implementation_)
    return common::make_unexpected(invalid("collected grouped shuffle result execution is empty"));
  Impl& impl = *implementation_;
  if (impl.failed_)
    return common::make_unexpected(impl.failure_);
  if (impl.complete_)
    return query::PhysicalOperatorStep::end();
  while (impl.partition_index_ < impl.streams_.size()) {
    const auto& stream = impl.streams_[impl.partition_index_];
    if (impl.batch_index_ == stream.encoded_result_batches.size()) {
      ++impl.partition_index_;
      impl.batch_index_ = 0U;
      ++impl.metrics_.completed_partitions;
      continue;
    }
    const auto& encoded = stream.encoded_result_batches[impl.batch_index_];
    auto batch =
        network::decode_query_result_batch(encoded, impl.limits_.stream.frame.result_batch);
    if (!batch.has_value())
      return impl.fail(batch.error());
    if (!descriptors_match(*batch, impl.result_schema_.get()) || batch->row_count() == 0U)
      return impl.fail(corruption("collected grouped shuffle result batch is invalid"));
    auto chunk =
        materialize_batch(*batch, impl.result_schema_.get(), impl.limits_, impl.resources_);
    if (!chunk.has_value())
      return impl.fail(chunk.error());
    ++impl.batch_index_;
    ++impl.metrics_.decoded_batches;
    impl.metrics_.decoded_rows += batch->row_count();
    impl.metrics_.decoded_batch_bytes += encoded.size();
    return query::PhysicalOperatorStep::chunk(std::move(*chunk));
  }
  impl.complete_ = true;
  return query::PhysicalOperatorStep::end();
}

std::span<const query::VectorGroupKeyDefinition>
DistributedVectorGroupedAggregateShuffleCollectedResultExecution::key_definitions() const noexcept {
  return implementation_ ? implementation_->authority_.get().key_definitions()
                         : std::span<const query::VectorGroupKeyDefinition>{};
}

std::span<const query::VectorAggregateDefinition>
DistributedVectorGroupedAggregateShuffleCollectedResultExecution::aggregate_definitions()
    const noexcept {
  return implementation_ ? implementation_->authority_.get().aggregate_definitions()
                         : std::span<const query::VectorAggregateDefinition>{};
}

std::optional<query::QueryResourceContext>
DistributedVectorGroupedAggregateShuffleCollectedResultExecution::output_resources()
    const noexcept {
  return implementation_ ? std::optional<query::QueryResourceContext>{implementation_->resources_}
                         : std::nullopt;
}

DistributedVectorGroupedAggregateShuffleCollectedResultExecutionMetrics
DistributedVectorGroupedAggregateShuffleCollectedResultExecution::metrics() const noexcept {
  return implementation_
             ? implementation_->metrics_
             : DistributedVectorGroupedAggregateShuffleCollectedResultExecutionMetrics{};
}

const DistributedVectorGroupedAggregateShuffleAuthority*
DistributedVectorGroupedAggregateShuffleCollectedResultExecution::authority() const noexcept {
  return implementation_ ? std::addressof(implementation_->authority_.get()) : nullptr;
}

const query::DistributedVectorResultSchema*
DistributedVectorGroupedAggregateShuffleCollectedResultExecution::result_schema() const noexcept {
  return implementation_ ? std::addressof(implementation_->result_schema_.get()) : nullptr;
}

const common::Status&
DistributedVectorGroupedAggregateShuffleCollectedResultExecution::failure() const noexcept {
  static const common::Status empty{common::StatusCode::kInvalidArgument,
                                    "collected grouped shuffle result execution is empty"};
  return implementation_ ? implementation_->failure_ : empty;
}

} // namespace chronos::cluster
