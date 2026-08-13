#include "chronos/service/replicated_distributed_query_worker.hpp"

#include "chronos/cluster/distributed_vector_result_exchange.hpp"
#include "chronos/common/checked_math.hpp"

#include <algorithm>
#include <array>
#include <functional>
#include <limits>
#include <new>
#include <stdexcept>
#include <utility>

namespace chronos::service {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] bool valid_vector_aggregate_worker_limits(
    const query::DistributedVectorAggregateWorkerLimitsV2& limits) noexcept {
  return limits.maximum_query_memory_bytes > 0U &&
         limits.maximum_query_memory_bytes <=
             query::kMaximumDistributedVectorRowsWorkerMemoryBytesV2 &&
         limits.scan.maximum_rows_per_chunk > 0U && limits.scan.chunk.maximum_rows > 0U &&
         limits.scan.chunk.maximum_columns > 0U && limits.scan.chunk.maximum_buffer_bytes > 0U &&
         limits.scan.chunk.maximum_retained_buffer_bytes > 0U &&
         limits.projection.maximum_rows > 0U && limits.projection.maximum_columns > 0U &&
         limits.projection.maximum_buffer_bytes > 0U &&
         limits.projection.maximum_retained_buffer_bytes > 0U &&
         limits.projection.maximum_rows >=
             std::min(limits.scan.maximum_rows_per_chunk, limits.scan.chunk.maximum_rows) &&
         limits.maximum_aggregates > 0U &&
         limits.maximum_aggregates <= query::kMaximumUngroupedAggregateWidth &&
         limits.maximum_variable_extremum_bytes > 0U &&
         limits.maximum_variable_extremum_bytes <=
             query::distributed_vector_aggregate_state_format::kMaximumExtremumBytes &&
         limits.maximum_retained_configuration_bytes > 0U;
}

class VectorResultCollector final : public query::DistributedVectorRowsChunkConsumerV2 {
public:
  VectorResultCollector(const query::DistributedVectorFragmentDispatchV2& dispatch,
                        const ReplicatedDistributedVectorQueryWorkerLimitsV2& limits)
      : dispatch_(dispatch), limits_(limits) {
    columns_.reserve(dispatch.result_schema.columns.size());
    for (const query::DistributedVectorResultColumn& column : dispatch.result_schema.columns)
      columns_.push_back({column.name, column.type, column.nullable});
  }

  common::Status consume(const query::VectorChunk& chunk) override {
    if (messages_.size() == limits_.get().maximum_messages)
      return exhausted("replicated vector worker message limit exceeded");
    if (chunk.column_count() != columns_.size() ||
        chunk.selected_row_count() > std::numeric_limits<std::uint32_t>::max()) {
      return invalid("replicated vector worker chunk shape is invalid");
    }
    const std::uint32_t rows = static_cast<std::uint32_t>(chunk.selected_row_count());
    const auto cell_count =
        common::checked_multiply(static_cast<std::size_t>(rows), columns_.size());
    if (!cell_count.has_value())
      return exhausted("replicated vector worker cell count overflows");
    try {
      std::vector<network::QueryResultCell> cells;
      cells.reserve(*cell_count);
      std::vector<std::byte> booleans(*cell_count);
      for (std::size_t row = 0U; row < rows; ++row) {
        for (std::size_t column = 0U; column < columns_.size(); ++column) {
          auto cell = chunk.cell({.column_ordinal = column, .selected_row = row});
          if (!cell.has_value())
            return cell.error();
          if (cell->is_null()) {
            cells.push_back({.is_null = true});
          } else if (cell->kind() == columnar::ColumnCellView::Kind::kBoolean) {
            auto value = cell->boolean();
            if (!value.has_value())
              return value.error();
            const std::size_t ordinal = cells.size();
            booleans[ordinal] = *value ? std::byte{1U} : std::byte{0U};
            cells.push_back({.value = {&booleans[ordinal], 1U}});
          } else {
            auto value = cell->bytes();
            if (!value.has_value())
              return value.error();
            cells.push_back({.value = *value});
          }
        }
      }
      auto encoded =
          network::encode_query_result_batch(rows, columns_, cells, limits_.get().result);
      if (!encoded.has_value())
        return encoded.error();
      const auto frame_bytes = common::checked_add(
          encoded->size(),
          cluster::distributed_vector_result_exchange_v2_format::kHeaderLength +
              cluster::distributed_vector_result_exchange_v2_format::kTrailerLength);
      if (!frame_bytes.has_value() ||
          *frame_bytes > limits_.get().maximum_total_encoded_bytes - retained_encoded_bytes_) {
        return exhausted("replicated vector worker encoded-byte limit exceeded");
      }
      messages_.push_back({.query_id = dispatch_.get().dispatch.query_id,
                           .tablet_id = dispatch_.get().dispatch.tablet_id,
                           .sequence = static_cast<std::uint64_t>(messages_.size()) + 1U,
                           .encoded_result_batch = std::move(*encoded)});
      retained_encoded_bytes_ += *frame_bytes;
      return common::Status::ok();
    } catch (const std::bad_alloc&) {
      return exhausted("replicated vector worker result allocation failed");
    } catch (const std::length_error&) {
      return exhausted("replicated vector worker result exceeds container limits");
    }
  }

  [[nodiscard]] common::Result<std::vector<cluster::DistributedVectorResultExchangeMessage>>
  finish() && {
    constexpr std::size_t kTerminalBytes =
        cluster::distributed_vector_result_exchange_v2_format::kHeaderLength +
        cluster::distributed_vector_result_exchange_v2_format::kTrailerLength;
    try {
      if (messages_.empty()) {
        if (limits_.get().maximum_messages == 0U ||
            kTerminalBytes > limits_.get().maximum_total_encoded_bytes) {
          return common::make_unexpected(
              exhausted("replicated vector worker terminal exceeds limits"));
        }
        messages_.push_back({.query_id = dispatch_.get().dispatch.query_id,
                             .tablet_id = dispatch_.get().dispatch.tablet_id,
                             .sequence = 1U,
                             .terminal = true});
      } else {
        messages_.back().terminal = true;
      }
      return std::move(messages_);
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(
          exhausted("replicated vector worker terminal allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(
          exhausted("replicated vector worker terminal exceeds container limits"));
    }
  }

private:
  std::reference_wrapper<const query::DistributedVectorFragmentDispatchV2> dispatch_;
  std::reference_wrapper<const ReplicatedDistributedVectorQueryWorkerLimitsV2> limits_;
  std::vector<network::QueryResultColumn> columns_;
  std::vector<cluster::DistributedVectorResultExchangeMessage> messages_;
  std::size_t retained_encoded_bytes_{};
};

} // namespace

ReplicatedDistributedQueryWorker::ReplicatedDistributedQueryWorker(
    ReplicatedDistributedQueryWorkerConfig config) noexcept
    : config_(config) {}

common::Result<ReplicatedDistributedQueryWorker>
ReplicatedDistributedQueryWorker::create(ReplicatedDistributedQueryWorkerConfig config) {
  if (config.local_node_id == 0U || config.storage == nullptr ||
      config.context_provider == nullptr) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument,
                       "replicated distributed query worker configuration is invalid"});
  }
  return ReplicatedDistributedQueryWorker{config};
}

common::Result<query::ExchangeMessage> ReplicatedDistributedQueryWorker::execute(
    const query::DistributedAggregateFragmentDispatch& dispatch) {
  auto context = config_.context_provider->acquire(dispatch);
  if (!context.has_value())
    return common::make_unexpected(context.error());
  if (!context->lineage) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument,
                       "replicated distributed query worker context has no schema lineage"});
  }
  return query::execute_distributed_aggregate_fragment(
      {.dispatch = std::cref(dispatch),
       .storage = std::cref(*config_.storage),
       .snapshot = std::cref(context->snapshot),
       .lineage = std::cref(*context->lineage),
       .placement = std::cref(context->placement),
       .raft_group_id = context->raft_group_id,
       .local_node = config_.local_node_id,
       .local_linearizable_barrier = context->local_linearizable_barrier,
       .limits = config_.limits});
}

ReplicatedDistributedGroupedQueryWorker::ReplicatedDistributedGroupedQueryWorker(
    ReplicatedDistributedGroupedQueryWorkerConfig config) noexcept
    : config_(config) {}

common::Result<ReplicatedDistributedGroupedQueryWorker>
ReplicatedDistributedGroupedQueryWorker::create(
    ReplicatedDistributedGroupedQueryWorkerConfig config) {
  if (config.local_node_id == 0U || config.storage == nullptr ||
      config.context_provider == nullptr) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument,
                       "replicated grouped query worker configuration is invalid"});
  }
  return ReplicatedDistributedGroupedQueryWorker{config};
}

common::Result<query::DistributedGroupedFloat64WorkerResult>
ReplicatedDistributedGroupedQueryWorker::execute(
    const query::DistributedGroupedFloat64FragmentDispatch& dispatch) {
  auto context = config_.context_provider->acquire(dispatch);
  if (!context.has_value())
    return common::make_unexpected(context.error());
  if (!context->lineage) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument,
                       "replicated grouped query worker context has no schema lineage"});
  }
  return query::execute_distributed_grouped_float64_fragment(
      {.dispatch = std::cref(dispatch),
       .storage = std::cref(*config_.storage),
       .snapshot = std::cref(context->snapshot),
       .lineage = std::cref(*context->lineage),
       .placement = std::cref(context->placement),
       .raft_group_id = context->raft_group_id,
       .local_node = config_.local_node_id,
       .local_linearizable_barrier = context->local_linearizable_barrier,
       .limits = config_.limits});
}

ReplicatedDistributedVectorQueryWorkerV2::ReplicatedDistributedVectorQueryWorkerV2(
    ReplicatedDistributedVectorQueryWorkerConfigV2 config) noexcept
    : config_(config) {}

common::Result<ReplicatedDistributedVectorQueryWorkerV2>
ReplicatedDistributedVectorQueryWorkerV2::create(
    ReplicatedDistributedVectorQueryWorkerConfigV2 config) {
  if (config.local_node_id == 0U || config.storage == nullptr ||
      config.context_provider == nullptr || config.limits.maximum_messages == 0U ||
      config.limits.maximum_messages > query::kMaximumDistributedCoordinatorMessages ||
      config.limits.rows.maximum_query_memory_bytes == 0U ||
      config.limits.rows.maximum_query_memory_bytes >
          query::kMaximumDistributedVectorRowsWorkerMemoryBytesV2 ||
      config.limits.rows.scan.maximum_rows_per_chunk == 0U ||
      config.limits.rows.scan.chunk.maximum_rows == 0U ||
      config.limits.rows.scan.chunk.maximum_columns == 0U ||
      config.limits.rows.scan.chunk.maximum_buffer_bytes == 0U ||
      config.limits.rows.scan.chunk.maximum_retained_buffer_bytes == 0U ||
      config.limits.rows.output.maximum_rows == 0U ||
      config.limits.rows.output.maximum_columns == 0U ||
      config.limits.rows.output.maximum_buffer_bytes == 0U ||
      config.limits.rows.output.maximum_retained_buffer_bytes == 0U ||
      config.limits.maximum_total_encoded_bytes <
          cluster::distributed_vector_result_exchange_v2_format::kHeaderLength +
              cluster::distributed_vector_result_exchange_v2_format::kTrailerLength ||
      config.limits.maximum_total_encoded_bytes >
          cluster::kMaximumDistributedVectorResultCoordinatorBytesV2 ||
      config.limits.result.protocol.maximum_payload_size == 0U ||
      config.limits.result.protocol.maximum_payload_size > network::kDefaultMaximumPayloadSize ||
      config.limits.result.maximum_rows == 0U || config.limits.result.maximum_columns == 0U ||
      config.limits.result.maximum_columns >
          query::distributed_vector_result_schema_format::kMaximumColumns ||
      config.limits.result.maximum_column_name_bytes == 0U ||
      config.limits.result.maximum_column_name_bytes > 65'536U ||
      config.limits.rows.output.maximum_rows <
          std::min(config.limits.rows.scan.maximum_rows_per_chunk,
                   config.limits.rows.scan.chunk.maximum_rows) ||
      config.limits.result.maximum_rows < std::min(config.limits.rows.scan.maximum_rows_per_chunk,
                                                   config.limits.rows.scan.chunk.maximum_rows)) {
    return common::make_unexpected(
        invalid("replicated distributed vector worker configuration is invalid"));
  }
  return ReplicatedDistributedVectorQueryWorkerV2{config};
}

common::Result<std::vector<cluster::DistributedVectorResultExchangeMessage>>
ReplicatedDistributedVectorQueryWorkerV2::execute(
    const query::DistributedVectorFragmentDispatchV2& dispatch) {
  try {
    auto context = config_.context_provider->acquire(dispatch);
    if (!context.has_value())
      return common::make_unexpected(context.error());
    if (!context->lineage) {
      return common::make_unexpected(
          invalid("replicated distributed vector worker context has no schema lineage"));
    }
    VectorResultCollector collector{dispatch, config_.limits};
    auto executed = query::execute_distributed_vector_rows_fragment_v2(
        {.dispatch = std::cref(dispatch),
         .storage = std::cref(*config_.storage),
         .snapshot = std::cref(context->snapshot),
         .lineage = std::cref(*context->lineage),
         .placement = std::cref(context->placement),
         .raft_group_id = context->raft_group_id,
         .local_node = config_.local_node_id,
         .local_linearizable_barrier = context->local_linearizable_barrier,
         .limits = config_.limits.rows},
        collector);
    if (!executed.has_value())
      return common::make_unexpected(executed.error());
    if (executed->output_chunks == 0U && executed->output_rows != 0U) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kCorruption,
                         "replicated vector worker output accounting is invalid"});
    }
    return std::move(collector).finish();
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("replicated vector worker allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("replicated vector worker exceeds container limits"));
  }
}

ReplicatedDistributedVectorAggregateQueryWorkerV2::
    ReplicatedDistributedVectorAggregateQueryWorkerV2(
        ReplicatedDistributedVectorAggregateQueryWorkerConfigV2 config) noexcept
    : config_(config) {}

common::Result<ReplicatedDistributedVectorAggregateQueryWorkerV2>
ReplicatedDistributedVectorAggregateQueryWorkerV2::create(
    ReplicatedDistributedVectorAggregateQueryWorkerConfigV2 config) {
  if (config.local_node_id == 0U || config.storage == nullptr ||
      config.context_provider == nullptr || !valid_vector_aggregate_worker_limits(config.limits)) {
    return common::make_unexpected(
        invalid("replicated distributed vector aggregate worker configuration is invalid"));
  }
  return ReplicatedDistributedVectorAggregateQueryWorkerV2{config};
}

common::Result<std::vector<query::VectorAggregateDefinition>>
ReplicatedDistributedVectorAggregateQueryWorkerV2::bind_definitions(
    const query::DistributedVectorFragmentDispatchV2& dispatch) {
  try {
    auto context = config_.context_provider->acquire(dispatch);
    if (!context.has_value())
      return common::make_unexpected(context.error());
    if (!context->lineage) {
      return common::make_unexpected(
          invalid("replicated vector aggregate worker context has no schema lineage"));
    }
    return query::bind_distributed_vector_aggregate_worker_definitions_v2(
        {.dispatch = std::cref(dispatch),
         .storage = std::cref(*config_.storage),
         .snapshot = std::cref(context->snapshot),
         .lineage = std::cref(*context->lineage),
         .placement = std::cref(context->placement),
         .raft_group_id = context->raft_group_id,
         .local_node = config_.local_node_id,
         .local_linearizable_barrier = context->local_linearizable_barrier,
         .limits = config_.limits});
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("replicated vector aggregate definition binding allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("replicated vector aggregate definition binding exceeds container limits"));
  }
}

common::Result<query::DistributedVectorAggregateWorkerResultV2>
ReplicatedDistributedVectorAggregateQueryWorkerV2::execute(
    const query::DistributedVectorFragmentDispatchV2& dispatch) {
  try {
    auto context = config_.context_provider->acquire(dispatch);
    if (!context.has_value())
      return common::make_unexpected(context.error());
    if (!context->lineage) {
      return common::make_unexpected(
          invalid("replicated vector aggregate worker context has no schema lineage"));
    }
    return query::execute_distributed_vector_aggregate_fragment_v2(
        {.dispatch = std::cref(dispatch),
         .storage = std::cref(*config_.storage),
         .snapshot = std::cref(context->snapshot),
         .lineage = std::cref(*context->lineage),
         .placement = std::cref(context->placement),
         .raft_group_id = context->raft_group_id,
         .local_node = config_.local_node_id,
         .local_linearizable_barrier = context->local_linearizable_barrier,
         .limits = config_.limits});
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("replicated vector aggregate worker allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("replicated vector aggregate worker exceeds container limits"));
  }
}

} // namespace chronos::service
