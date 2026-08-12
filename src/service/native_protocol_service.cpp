#include "chronos/service/native_protocol_service.hpp"

#include "chronos/common/checked_math.hpp"
#include "chronos/ingest/columnar_append_executor.hpp"
#include "chronos/ingest/committed_columnar_append.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
#include "chronos/query/resource_context.hpp"
#include "chronos/query/tablet_state_pipeline.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::service {
namespace {

[[nodiscard]] network::DurabilityMode
protocol_durability(const wal::WalDurabilityMode mode) noexcept {
  return mode == wal::WalDurabilityMode::kLocalSync ? network::DurabilityMode::kLocalSync
                                                    : network::DurabilityMode::kAsync;
}

[[nodiscard]] wal::WalDurabilityMode wal_durability(const network::DurabilityMode mode) noexcept {
  return mode == network::DurabilityMode::kLocalSync ? wal::WalDurabilityMode::kLocalSync
                                                     : wal::WalDurabilityMode::kAsync;
}

[[nodiscard]] network::ProtocolErrorCode error_code(const common::StatusCode code) noexcept {
  switch (code) {
  case common::StatusCode::kCancelled:
    return network::ProtocolErrorCode::kCancelled;
  case common::StatusCode::kResourceExhausted:
    return network::ProtocolErrorCode::kOverloaded;
  case common::StatusCode::kUnauthenticated:
    return network::ProtocolErrorCode::kUnauthorized;
  case common::StatusCode::kInvalidArgument:
  case common::StatusCode::kOutOfRange:
  case common::StatusCode::kNotFound:
  case common::StatusCode::kAlreadyExists:
    return network::ProtocolErrorCode::kInvalidRequest;
  case common::StatusCode::kInternal:
  case common::StatusCode::kCorruption:
    return network::ProtocolErrorCode::kInternal;
  case common::StatusCode::kIoError:
  case common::StatusCode::kUnavailable:
  case common::StatusCode::kNotSupported:
    return network::ProtocolErrorCode::kExecutionFailure;
  case common::StatusCode::kOk:
    return network::ProtocolErrorCode::kInternal;
  }
  return network::ProtocolErrorCode::kInternal;
}

[[nodiscard]] common::Result<network::NetworkTask> response(network::NetworkTask request,
                                                            const network::MessageType type,
                                                            std::vector<std::byte> payload) {
  request.frame = {.header = {.message_type = type,
                              .request_id = request.frame.header.request_id,
                              .payload_size = static_cast<std::uint32_t>(payload.size())},
                   .payload = std::move(payload)};
  return request;
}

[[nodiscard]] common::Result<network::NetworkTask>
error_response(network::NetworkTask request, const common::Status& status,
               const network::ProtocolLimits& limits) {
  const std::size_t maximum_message =
      limits.maximum_payload_size > network::kErrorEnvelopeSize
          ? limits.maximum_payload_size - network::kErrorEnvelopeSize
          : 0U;
  const std::string_view source =
      status.message().empty() ? "native request failed" : std::string_view{status.message()};
  const std::string_view message = source.substr(0U, std::min(source.size(), maximum_message));
  auto payload = network::encode_error_message(error_code(status.code()), message, limits);
  if (!payload.has_value())
    return common::make_unexpected(payload.error());
  return response(std::move(request), network::MessageType::kError, std::move(*payload));
}

[[nodiscard]] common::Status invalid(std::string_view message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::string{message}};
}

[[nodiscard]] common::Status exhausted(std::string_view message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::string{message}};
}

[[nodiscard]] common::Status internal(std::string_view message) {
  return common::Status{common::StatusCode::kInternal, std::string{message}};
}

struct ResponseRoute {
  std::uint64_t connection_id;
  std::uint64_t principal_id;
  std::uint64_t request_id;
};

[[nodiscard]] ResponseRoute route(const network::NetworkTask& request) noexcept {
  return {.connection_id = request.connection_id,
          .principal_id = request.principal_id,
          .request_id = request.frame.header.request_id};
}

[[nodiscard]] network::NetworkTask make_response(const ResponseRoute& target,
                                                 const network::MessageType type,
                                                 std::vector<std::byte> payload = {}) {
  return {.connection_id = target.connection_id,
          .principal_id = target.principal_id,
          .frame = {.header = {.message_type = type,
                               .request_id = target.request_id,
                               .payload_size = static_cast<std::uint32_t>(payload.size())},
                    .payload = std::move(payload)}};
}

[[nodiscard]] common::Result<NativeProtocolResponseSequence>
query_error(const ResponseRoute& target, const common::Status& status,
            const network::ProtocolLimits& limits) {
  network::NetworkTask shell{
      .connection_id = target.connection_id,
      .principal_id = target.principal_id,
      .frame = {.header = {.message_type = network::MessageType::kQueryRequest,
                           .request_id = target.request_id}}};
  auto encoded = error_response(std::move(shell), status, limits);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  try {
    NativeProtocolResponseSequence result;
    result.payload_bytes = encoded->frame.payload.size();
    result.responses.push_back(std::move(*encoded));
    return result;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("query error response allocation failed"));
  }
}

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_query_chunk(const query::VectorChunk& chunk,
                   std::span<const network::QueryResultColumn> columns,
                   const network::QueryResultLimits& limits) {
  if (chunk.column_count() != columns.size() ||
      chunk.selected_row_count() > std::numeric_limits<std::uint32_t>::max())
    return common::make_unexpected(internal("query chunk shape disagrees with bound outputs"));
  const std::uint32_t rows = static_cast<std::uint32_t>(chunk.selected_row_count());
  const auto cell_count = common::checked_multiply(static_cast<std::size_t>(rows), columns.size());
  if (!cell_count.has_value())
    return common::make_unexpected(exhausted("query result cell count exceeds limits"));
  try {
    std::vector<network::QueryResultCell> cells;
    cells.reserve(*cell_count);
    std::vector<std::byte> booleans(*cell_count);
    for (std::size_t row = 0U; row < rows; ++row) {
      for (std::size_t column = 0U; column < columns.size(); ++column) {
        auto cell = chunk.cell({.column_ordinal = column, .selected_row = row});
        if (!cell.has_value())
          return common::make_unexpected(cell.error());
        if (cell->is_null()) {
          cells.push_back({.is_null = true});
        } else if (cell->kind() == columnar::ColumnCellView::Kind::kBoolean) {
          auto value = cell->boolean();
          if (!value.has_value())
            return common::make_unexpected(value.error());
          const std::size_t ordinal = cells.size();
          booleans[ordinal] = *value ? std::byte{1U} : std::byte{0U};
          cells.push_back({.is_null = false, .value = {&booleans[ordinal], 1U}});
        } else {
          auto value = cell->bytes();
          if (!value.has_value())
            return common::make_unexpected(value.error());
          cells.push_back({.is_null = false, .value = *value});
        }
      }
    }
    return network::encode_query_result_batch(rows, columns, cells, limits);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("query result cell allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("query result cell count exceeds container limits"));
  }
}

} // namespace

NativeProtocolService::NativeProtocolService(SingleNodeDatabase& database,
                                             NativeProtocolServiceLimits limits) noexcept
    : database_(&database), limits_(limits) {}

common::Result<network::NetworkTask>
NativeProtocolService::execute_ingest(network::NetworkTask request) {
  if (request.frame.header.message_type != network::MessageType::kIngestRequest ||
      request.frame.header.request_id == 0U) {
    return error_response(std::move(request), invalid("expected a nonzero INGEST_REQUEST"),
                          limits_.protocol);
  }
  const auto envelope = network::decode_ingest_request(request.frame.payload, limits_.protocol);
  if (!envelope.has_value())
    return error_response(std::move(request), invalid(envelope.error().message()),
                          limits_.protocol);
  const auto command = ingest::decode_columnar_append_v1_exact(envelope->encoded_columnar_append,
                                                               limits_.columnar_append);
  if (!command.has_value())
    return error_response(std::move(request), invalid(command.error().status().message()),
                          limits_.protocol);

  const schema::SchemaLineage* const lineage = database_->find_lineage(command->table_id());
  if (lineage == nullptr)
    return error_response(std::move(request), invalid("ingest table is not routable"),
                          limits_.protocol);
  const std::shared_ptr<const schema::TableSchema> active_schema = lineage->current();
  if (active_schema == nullptr || active_schema->schema_id() != command->schema_id())
    return error_response(std::move(request), invalid("ingest requires the active table schema"),
                          limits_.protocol);
  ingest::TabletState* const tablet = database_->find_tablet(command->tablet_id());
  if (tablet == nullptr)
    return error_response(std::move(request), invalid("ingest tablet is not local"),
                          limits_.protocol);
  auto batch =
      ingest::own_decoded_columnar_append_batch(*command, active_schema, limits_.columnar_append);
  if (!batch.has_value())
    return error_response(std::move(request), batch.error(), limits_.protocol);

  auto executed = ingest::execute_columnar_append(
      {.client_id = command->client_id(),
       .client_batch_id = command->client_batch_id(),
       .batch = std::move(*batch),
       .durability = wal_durability(envelope->durability)},
      database_->retry_directory(), *tablet, database_->wal_coordinator());
  if (!executed.has_value())
    return error_response(std::move(request), executed.error(), limits_.protocol);

  network::IngestAcknowledgement acknowledgement{
      .requested_durability = protocol_durability(executed->requested_durability),
      .effective_durability = protocol_durability(executed->requested_durability),
      .outcome = executed->kind == ingest::ColumnarAppendExecutionKind::kApplied
                     ? network::IngestOutcome::kApplied
                     : network::IngestOutcome::kMatchingRetry};
  if (executed->wal_commit.has_value()) {
    acknowledgement.effective_durability =
        protocol_durability(executed->wal_commit->effective_durability);
    acknowledgement.record_sequence = executed->wal_commit->append.record_sequence;
    acknowledgement.segment_number = executed->wal_commit->append.record_start.segment_number;
    acknowledgement.byte_offset = executed->wal_commit->append.record_start.byte_offset;
  }
  auto payload = network::encode_ingest_acknowledgement(acknowledgement);
  if (!payload.has_value())
    return common::make_unexpected(payload.error());
  return response(std::move(request), network::MessageType::kIngestAcknowledgement,
                  std::move(*payload));
}

common::Result<NativeProtocolResponseSequence>
NativeProtocolService::execute_query(network::NetworkTask request) {
  const ResponseRoute target = route(request);
  if (request.frame.header.message_type != network::MessageType::kQueryRequest ||
      target.request_id == 0U) {
    return query_error(target, invalid("expected a nonzero QUERY_REQUEST"), limits_.protocol);
  }
  const auto sql = network::decode_query_request(request.frame.payload, limits_.protocol);
  if (!sql.has_value())
    return query_error(target, invalid(sql.error().message()), limits_.protocol);
  if (limits_.maximum_query_memory_bytes == 0U || limits_.maximum_result_rows == 0U ||
      limits_.maximum_result_batches == 0U || limits_.maximum_response_payload_bytes == 0U) {
    return query_error(target, invalid("native query limits are invalid"), limits_.protocol);
  }
  if (limits_.maximum_result_batches > 65'536U)
    return query_error(target, invalid("native query batch limit exceeds the supported maximum"),
                       limits_.protocol);

  try {
    std::string sql_text(sql->size(), '\0');
    std::memcpy(sql_text.data(), sql->data(), sql->size());
    auto parsed = query::parse_sql_v1_select(sql_text, limits_.sql_parser);
    if (!parsed.has_value())
      return query_error(target, parsed.error().status(), limits_.protocol);
    auto bound = query::bind_sql_v1_select(std::move(*parsed), database_->query_catalog(),
                                           limits_.sql_binder);
    if (!bound.has_value())
      return query_error(target, bound.error().status(), limits_.protocol);
    auto physical = query::lower_bound_sql_select(*bound, limits_.physical_lowering);
    if (!physical.has_value())
      return query_error(target, physical.error().status(), limits_.protocol);
    if (bound->sources().size() != 1U)
      return query_error(target, invalid("native SELECT requires exactly one table source"),
                         limits_.protocol);
    const std::shared_ptr<const schema::TableSchema>& source_schema =
        bound->sources().front().schema_ptr();
    const schema::SchemaLineage* const lineage = database_->find_lineage(source_schema->table_id());
    if (lineage == nullptr)
      return query_error(target, internal("bound query table has no runtime lineage"),
                         limits_.protocol);
    auto snapshots = database_->table_snapshots(source_schema->table_id());
    if (!snapshots.has_value())
      return query_error(target, snapshots.error(), limits_.protocol);
    auto resources = query::QueryResourceContext::create(limits_.maximum_query_memory_bytes);
    if (!resources.has_value())
      return query_error(target, resources.error(), limits_.protocol);
    auto pipeline = query::instantiate_tablet_states_pipeline(*resources, *snapshots, *lineage,
                                                              source_schema->schema_id(), *physical,
                                                              limits_.tablet_pipeline);
    if (!pipeline.has_value())
      return query_error(target, pipeline.error(), limits_.protocol);

    std::vector<network::QueryResultColumn> columns;
    columns.reserve(bound->outputs().size());
    for (const query::BoundOutputColumn& output : bound->outputs())
      columns.push_back({.name = output.name, .type = output.type, .nullable = output.nullable});
    network::QueryResultLimits result_limits = limits_.query_result;
    result_limits.protocol = limits_.protocol;

    NativeProtocolResponseSequence result;
    result.responses.reserve(limits_.maximum_result_batches + 1U);
    bool emitted_result{};
    for (;;) {
      auto step = (*pipeline)->next(*resources);
      if (!step.has_value())
        return query_error(target, step.error(), limits_.protocol);
      if (step->kind() == query::PhysicalOperatorStepKind::kEnd)
        break;
      if (step->chunk() == nullptr)
        return query_error(target, internal("query operator returned a missing chunk"),
                           limits_.protocol);
      const std::size_t rows = step->chunk()->chunk().selected_row_count();
      if (rows > result_limits.maximum_rows)
        return query_error(target, exhausted("query result batch row limit exceeded"),
                           limits_.protocol);
      if (rows > limits_.maximum_result_rows - result.result_rows)
        return query_error(target, exhausted("query result row limit exceeded"), limits_.protocol);
      if (result.responses.size() >= limits_.maximum_result_batches)
        return query_error(target, exhausted("query result batch limit exceeded"),
                           limits_.protocol);
      auto payload = encode_query_chunk(step->chunk()->chunk(), columns, result_limits);
      if (!payload.has_value())
        return query_error(target, payload.error(), limits_.protocol);
      if (payload->size() > limits_.maximum_response_payload_bytes - result.payload_bytes)
        return query_error(target, exhausted("query response byte limit exceeded"),
                           limits_.protocol);
      result.result_rows += rows;
      result.payload_bytes += payload->size();
      result.responses.push_back(
          make_response(target, network::MessageType::kQueryResult, std::move(*payload)));
      emitted_result = true;
    }
    if (!emitted_result) {
      auto payload = network::encode_query_result_batch(0U, columns, {}, result_limits);
      if (!payload.has_value())
        return query_error(target, payload.error(), limits_.protocol);
      if (payload->size() > limits_.maximum_response_payload_bytes)
        return query_error(target, exhausted("query response byte limit exceeded"),
                           limits_.protocol);
      result.payload_bytes = payload->size();
      result.responses.push_back(
          make_response(target, network::MessageType::kQueryResult, std::move(*payload)));
    }
    result.responses.push_back(make_response(target, network::MessageType::kQueryEnd));
    return result;
  } catch (const std::bad_alloc&) {
    return query_error(target, exhausted("native query allocation failed"), limits_.protocol);
  } catch (const std::length_error&) {
    return query_error(target, exhausted("native query exceeds container limits"),
                       limits_.protocol);
  }
}

} // namespace chronos::service
