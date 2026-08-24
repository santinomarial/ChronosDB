#include "chronos/service/native_protocol_service.hpp"

#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/ingest/columnar_append_executor.hpp"
#include "chronos/ingest/committed_columnar_append.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/query/binder.hpp"
#include "chronos/query/parser.hpp"
#include "chronos/query/physical_lowering.hpp"
#include "chronos/query/resource_context.hpp"
#include "chronos/query/snapshot_pipeline.hpp"
#include "chronos/query/statement_binder.hpp"
#include "chronos/service/replicated_read_barrier.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace chronos::service {
namespace {

[[nodiscard]] network::DurabilityMode
protocol_durability(const wal::WalDurabilityMode mode) noexcept {
  return mode == wal::WalDurabilityMode::kLocalSync ? network::DurabilityMode::kLocalSync
                                                    : network::DurabilityMode::kAsync;
}

[[nodiscard]] std::optional<wal::WalDurabilityMode>
wal_durability(const network::DurabilityMode mode) noexcept {
  switch (mode) {
  case network::DurabilityMode::kAsync:
    return wal::WalDurabilityMode::kAsync;
  case network::DurabilityMode::kLocalSync:
    return wal::WalDurabilityMode::kLocalSync;
  case network::DurabilityMode::kQuorumSync:
    return std::nullopt;
  }
  return std::nullopt;
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

[[nodiscard]] common::Status unsupported(std::string_view message) {
  return common::Status{common::StatusCode::kNotSupported, std::string{message}};
}

[[nodiscard]] common::UuidGenerator& system_identity_generator() {
  static common::SystemUuidGenerator generator;
  return generator;
}

struct ResponseRoute {
  std::uint64_t connection_id;
  std::uint64_t principal_id;
  std::uint64_t request_id;
  network::NetworkTaskProtocolContext protocol;
};

[[nodiscard]] ResponseRoute route(const network::NetworkTask& request) noexcept {
  return {.connection_id = request.connection_id,
          .principal_id = request.principal_id,
          .request_id = request.frame.header.request_id,
          .protocol = request.protocol};
}

[[nodiscard]] network::NetworkTask make_response(const ResponseRoute& target,
                                                 const network::MessageType type,
                                                 std::vector<std::byte> payload = {}) {
  return {.connection_id = target.connection_id,
          .principal_id = target.principal_id,
          .protocol = target.protocol,
          .frame = {.header = {.protocol_major = target.protocol.protocol_major,
                               .protocol_minor = target.protocol.protocol_minor,
                               .message_type = type,
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
      .protocol = target.protocol,
      .frame = {.header = {.protocol_major = target.protocol.protocol_major,
                           .protocol_minor = target.protocol.protocol_minor,
                           .message_type = network::MessageType::kQueryRequest,
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

[[nodiscard]] common::Result<NativeProtocolResponseSequence>
query_redirect(const ResponseRoute& target, const ReplicatedQueryLeaderRoute& route,
               const network::ProtocolLimits& limits) {
  if (limits.maximum_payload_size < network::kLeaderRedirectSize)
    return common::make_unexpected(exhausted("query redirect exceeds the protocol payload limit"));
  auto payload = network::encode_leader_redirect({.group_id = route.group_id,
                                                  .leader_node_id = route.leader_node_id,
                                                  .leader_term = route.leader_term,
                                                  .placement_epoch = route.placement_epoch});
  if (!payload.has_value())
    return common::make_unexpected(payload.error());
  try {
    NativeProtocolResponseSequence result;
    result.payload_bytes = payload->size();
    result.responses.push_back(
        make_response(target, network::MessageType::kLeaderRedirect, std::move(*payload)));
    return result;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("query redirect response allocation failed"));
  }
}

[[nodiscard]] bool valid_distributed_rows_config(
    const NativeDistributedMutableVectorRowsQueryConfig& config) noexcept {
  constexpr std::chrono::milliseconds kMaximumExecutionTimeout{300'000};
  constexpr std::chrono::milliseconds kMaximumPollWait{1000};
  return config.source_node_id != 0U && config.authenticator != nullptr &&
         config.node_authorizer != nullptr && !config.tls_contexts.empty() &&
         config.maximum_authority_rebindings <= 1024U && config.execution_timeout.count() > 0 &&
         config.execution_timeout <= kMaximumExecutionTimeout &&
         config.maximum_poll_wait.count() > 0 && config.maximum_poll_wait <= kMaximumPollWait;
}

[[nodiscard]] bool retryable_authority_failure(const common::StatusCode code) noexcept {
  return code == common::StatusCode::kUnavailable ||
         code == common::StatusCode::kResourceExhausted || code == common::StatusCode::kIoError;
}

[[nodiscard]] cluster::DistributedVectorRowFinalizationLimitsV2
bounded_finalization_limits(const NativeDistributedMutableVectorRowsQueryConfig& config,
                            const NativeProtocolServiceLimits& service) noexcept {
  cluster::DistributedVectorRowFinalizationLimitsV2 limits = config.finalization;
  limits.maximum_input_rows = std::min(limits.maximum_input_rows, service.maximum_result_rows);
  limits.maximum_output_batches =
      std::min(limits.maximum_output_batches, service.maximum_result_batches);
  limits.maximum_output_encoded_bytes =
      std::min(limits.maximum_output_encoded_bytes, service.maximum_response_payload_bytes);
  limits.output_batch.maximum_rows = static_cast<std::uint32_t>(
      std::min<std::uint64_t>(limits.output_batch.maximum_rows,
                              std::min<std::uint64_t>(service.maximum_result_rows,
                                                      std::numeric_limits<std::uint32_t>::max())));
  limits.output_batch.maximum_columns =
      std::min(limits.output_batch.maximum_columns, service.query_result.maximum_columns);
  limits.output_batch.maximum_column_name_bytes =
      std::min(limits.output_batch.maximum_column_name_bytes,
               service.query_result.maximum_column_name_bytes);
  limits.output_batch.protocol = service.protocol;
  return limits;
}

[[nodiscard]] cluster::DistributedVectorAggregateRowsFinalizationLimitsV2
bounded_aggregate_finalization_limits(const NativeDistributedMutableVectorRowsQueryConfig& config,
                                      const NativeProtocolServiceLimits& service) noexcept {
  cluster::DistributedVectorAggregateRowsFinalizationLimitsV2 limits =
      config.aggregate_finalization;
  limits.maximum_query_memory_bytes =
      std::min(limits.maximum_query_memory_bytes, service.maximum_query_memory_bytes);
  limits.output.maximum_output_encoded_bytes =
      std::min(limits.output.maximum_output_encoded_bytes, service.maximum_response_payload_bytes);
  limits.output.output_batch.maximum_rows = static_cast<std::uint32_t>(
      std::min<std::uint64_t>(limits.output.output_batch.maximum_rows,
                              std::min<std::uint64_t>(service.maximum_result_rows,
                                                      std::numeric_limits<std::uint32_t>::max())));
  limits.output.output_batch.maximum_columns =
      std::min(limits.output.output_batch.maximum_columns, service.query_result.maximum_columns);
  limits.output.output_batch.maximum_column_name_bytes =
      std::min(limits.output.output_batch.maximum_column_name_bytes,
               service.query_result.maximum_column_name_bytes);
  limits.output.output_batch.protocol = service.protocol;
  return limits;
}

[[nodiscard]] common::Result<NativeProtocolResponseSequence>
distributed_rows_result(const ResponseRoute& target,
                        cluster::DistributedVectorRowsFinalizedResultV2&& result,
                        const NativeProtocolServiceLimits& limits) {
  if (result.row_count > limits.maximum_result_rows ||
      result.encoded_batches.size() > limits.maximum_result_batches ||
      result.encoded_bytes > limits.maximum_response_payload_bytes) {
    return query_error(target, exhausted("distributed row result exceeds Native query limits"),
                       limits.protocol);
  }
  try {
    NativeProtocolResponseSequence responses;
    responses.result_rows = result.row_count;
    responses.payload_bytes = result.encoded_bytes;
    responses.responses.reserve(result.encoded_batches.size() + 1U);
    for (std::vector<std::byte>& payload : result.encoded_batches) {
      responses.responses.push_back(
          make_response(target, network::MessageType::kQueryResult, std::move(payload)));
    }
    responses.responses.push_back(make_response(target, network::MessageType::kQueryEnd));
    return responses;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("distributed row response allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("distributed row response exceeds limits"));
  }
}

[[nodiscard]] common::Result<NativeProtocolResponseSequence>
distributed_aggregate_result(const ResponseRoute& target,
                             cluster::DistributedVectorAggregateFinalizedResultV2&& result,
                             const NativeProtocolServiceLimits& limits) {
  if (result.row_count > limits.maximum_result_rows ||
      result.encoded_bytes > limits.maximum_response_payload_bytes ||
      result.encoded_bytes != result.encoded_batch.size()) {
    return query_error(target,
                       exhausted("distributed aggregate result exceeds Native query limits"),
                       limits.protocol);
  }
  try {
    NativeProtocolResponseSequence responses;
    responses.result_rows = result.row_count;
    responses.payload_bytes = result.encoded_bytes;
    responses.responses.reserve(2U);
    responses.responses.push_back(
        make_response(target, network::MessageType::kQueryResult, std::move(result.encoded_batch)));
    responses.responses.push_back(make_response(target, network::MessageType::kQueryEnd));
    return responses;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("distributed aggregate response allocation failed"));
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

[[nodiscard]] common::Result<NativeProtocolResponseSequence>
ddl_result(const ResponseRoute& target, const CreatedSingleNodeTable& created,
           const NativeProtocolServiceLimits& limits) {
  auto uuid_type = schema::LogicalType::create(schema::LogicalTypeKind::kUuid);
  auto uint64_type = schema::LogicalType::create(schema::LogicalTypeKind::kUInt64);
  auto bool_type = schema::LogicalType::create(schema::LogicalTypeKind::kBool);
  if (!uuid_type.has_value() || !uint64_type.has_value() || !bool_type.has_value())
    return query_error(target, internal("DDL result logical types are unavailable"),
                       limits.protocol);
  const std::array columns{
      network::QueryResultColumn{"table_id", *uuid_type, false},
      network::QueryResultColumn{"schema_id", *uuid_type, false},
      network::QueryResultColumn{"tablet_id", *uuid_type, false},
      network::QueryResultColumn{"metadata_index", *uint64_type, false},
      network::QueryResultColumn{"resumed_incomplete_creation", *bool_type, false}};
  std::array<std::byte, sizeof(std::uint64_t)> metadata_index{};
  common::ByteWriter writer{metadata_index};
  const common::Status written = writer.write_u64_le(created.metadata_index);
  if (!written.is_ok())
    return query_error(target, written, limits.protocol);
  const std::byte resumed = created.resumed_incomplete_creation ? std::byte{1U} : std::byte{0U};
  const std::array cells{network::QueryResultCell{.value = created.table_id.bytes()},
                         network::QueryResultCell{.value = created.schema_id.bytes()},
                         network::QueryResultCell{.value = created.tablet_id.bytes()},
                         network::QueryResultCell{.value = metadata_index},
                         network::QueryResultCell{.value = {&resumed, 1U}}};
  network::QueryResultLimits result_limits = limits.query_result;
  result_limits.protocol = limits.protocol;
  auto payload = network::encode_query_result_batch(1U, columns, cells, result_limits);
  if (!payload.has_value())
    return query_error(target, payload.error(), limits.protocol);
  if (payload->size() > limits.maximum_response_payload_bytes)
    return query_error(target, exhausted("DDL response byte limit exceeded"), limits.protocol);
  try {
    NativeProtocolResponseSequence result;
    result.result_rows = 1U;
    result.payload_bytes = payload->size();
    result.responses.reserve(2U);
    result.responses.push_back(
        make_response(target, network::MessageType::kQueryResult, std::move(*payload)));
    result.responses.push_back(make_response(target, network::MessageType::kQueryEnd));
    return result;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("DDL response allocation failed"));
  }
}

[[nodiscard]] std::array<std::byte, sizeof(std::uint64_t)>
u64_bytes(const std::uint64_t value) noexcept {
  std::array<std::byte, sizeof(std::uint64_t)> bytes{};
  for (std::size_t index = 0U; index < bytes.size(); ++index)
    bytes[index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
  return bytes;
}

[[nodiscard]] common::Result<NativeProtocolResponseSequence>
insert_result(const ResponseRoute& target, const std::uint32_t applied_rows,
              const ingest::ColumnarAppendExecutionResult& executed,
              const NativeProtocolServiceLimits& limits) {
  auto uint64_type = schema::LogicalType::create(schema::LogicalTypeKind::kUInt64);
  auto bool_type = schema::LogicalType::create(schema::LogicalTypeKind::kBool);
  if (!uint64_type.has_value() || !bool_type.has_value())
    return query_error(target, internal("INSERT result logical types are unavailable"),
                       limits.protocol);
  const std::array columns{network::QueryResultColumn{"applied_rows", *uint64_type, false},
                           network::QueryResultColumn{"record_sequence", *uint64_type, false},
                           network::QueryResultColumn{"segment_number", *uint64_type, false},
                           network::QueryResultColumn{"byte_offset", *uint64_type, false},
                           network::QueryResultColumn{"matching_retry", *bool_type, false}};
  const auto rows = u64_bytes(applied_rows);
  const auto record_sequence =
      u64_bytes(executed.wal_commit.has_value() ? executed.wal_commit->append.record_sequence : 0U);
  const auto segment_number = u64_bytes(
      executed.wal_commit.has_value() ? executed.wal_commit->append.record_start.segment_number
                                      : 0U);
  const auto byte_offset = u64_bytes(
      executed.wal_commit.has_value() ? executed.wal_commit->append.record_start.byte_offset : 0U);
  const std::byte matching_retry =
      executed.kind == ingest::ColumnarAppendExecutionKind::kMatchingRetry ? std::byte{1U}
                                                                           : std::byte{0U};
  const std::array cells{network::QueryResultCell{.value = rows},
                         network::QueryResultCell{.value = record_sequence},
                         network::QueryResultCell{.value = segment_number},
                         network::QueryResultCell{.value = byte_offset},
                         network::QueryResultCell{.value = {&matching_retry, 1U}}};
  network::QueryResultLimits result_limits = limits.query_result;
  result_limits.protocol = limits.protocol;
  auto payload = network::encode_query_result_batch(1U, columns, cells, result_limits);
  if (!payload.has_value())
    return query_error(target, payload.error(), limits.protocol);
  if (payload->size() > limits.maximum_response_payload_bytes)
    return query_error(target, exhausted("INSERT response byte limit exceeded"), limits.protocol);
  try {
    NativeProtocolResponseSequence result;
    result.result_rows = 1U;
    result.payload_bytes = payload->size();
    result.responses.reserve(2U);
    result.responses.push_back(
        make_response(target, network::MessageType::kQueryResult, std::move(*payload)));
    result.responses.push_back(make_response(target, network::MessageType::kQueryEnd));
    return result;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("INSERT response allocation failed"));
  }
}

} // namespace

NativeProtocolService::NativeProtocolService(SingleNodeDatabase& database,
                                             NativeProtocolServiceLimits limits) noexcept
    : database_(&database), identities_(&system_identity_generator()), limits_(limits) {}
NativeProtocolService::NativeProtocolService(SingleNodeDatabase& database,
                                             NativeIdentityGenerator& identities,
                                             NativeProtocolServiceLimits limits) noexcept
    : database_(&database), identities_(&identities), limits_(limits) {}
NativeProtocolService::NativeProtocolService(ReplicatedIngestDatabase& database,
                                             NativeProtocolServiceLimits limits) noexcept
    : replicated_database_(&database), identities_(&system_identity_generator()), limits_(limits) {}
NativeProtocolService::NativeProtocolService(ReplicatedIngestDatabase& database,
                                             ReplicatedReadBarrier& read_barrier,
                                             NativeProtocolServiceLimits limits) noexcept
    : replicated_database_(&database), replicated_read_barrier_(&read_barrier),
      identities_(&system_identity_generator()), limits_(limits) {}
NativeProtocolService::NativeProtocolService(
    ReplicatedIngestDatabase& database, ReplicatedReadBarrier& read_barrier,
    const NativeDistributedMutableVectorRowsQueryConfig& distributed_rows,
    NativeProtocolServiceLimits limits) noexcept
    : replicated_database_(&database), replicated_read_barrier_(&read_barrier),
      identities_(&system_identity_generator()), distributed_mutable_query_(&distributed_rows),
      limits_(limits) {}
NativeProtocolService::NativeProtocolService(
    ReplicatedIngestDatabase& database, ReplicatedReadBarrier& read_barrier,
    NativeIdentityGenerator& identities,
    const NativeDistributedMutableVectorRowsQueryConfig& distributed_rows,
    NativeProtocolServiceLimits limits) noexcept
    : replicated_database_(&database), replicated_read_barrier_(&read_barrier),
      identities_(&identities), distributed_mutable_query_(&distributed_rows), limits_(limits) {}

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
  if (database_ == nullptr)
    return error_response(std::move(request),
                          unsupported("replicated ingest uses the asynchronous QUORUM_SYNC path"),
                          limits_.protocol);
  const std::optional<wal::WalDurabilityMode> durability = wal_durability(envelope->durability);
  if (!durability.has_value())
    return error_response(std::move(request),
                          unsupported("single-node ingest cannot provide QUORUM_SYNC"),
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

  auto executed = database_->execute_append(command->tablet_id(),
                                            {.client_id = command->client_id(),
                                             .client_batch_id = command->client_batch_id(),
                                             .batch = std::move(*batch),
                                             .durability = *durability});
  if (!executed.has_value())
    return error_response(std::move(request), executed.error(), limits_.protocol);
  auto flushed = database_->flush_ready_heads();
  if (!flushed.has_value())
    return error_response(std::move(request), flushed.error(), limits_.protocol);

  network::IngestAcknowledgement acknowledgement{
      .requested_durability = protocol_durability(executed->requested_durability),
      .effective_durability = protocol_durability(executed->requested_durability),
      .outcome = executed->kind == ingest::ColumnarAppendExecutionKind::kApplied
                     ? network::IngestOutcome::kApplied
                     : network::IngestOutcome::kMatchingRetry};
  const auto& wal_commit = executed->wal_commit;
  if (wal_commit.has_value()) {
    acknowledgement.effective_durability = protocol_durability(wal_commit->effective_durability);
    acknowledgement.record_sequence = wal_commit->append.record_sequence;
    acknowledgement.segment_number = wal_commit->append.record_start.segment_number;
    acknowledgement.byte_offset = wal_commit->append.record_start.byte_offset;
  }
  auto payload = network::encode_ingest_acknowledgement(acknowledgement);
  if (!payload.has_value())
    return common::make_unexpected(payload.error());
  return response(std::move(request), network::MessageType::kIngestAcknowledgement,
                  std::move(*payload));
}

common::Result<NativeProtocolResponseSequence>
NativeProtocolService::execute_query(network::NetworkTask request) {
  return execute_query(std::move(request), nullptr);
}

common::Result<NativeProtocolResponseSequence>
NativeProtocolService::execute_query(network::NetworkTask request,
                                     const NativeQueryCancellation* const cancellation) {
  const ResponseRoute target = route(request);
  if (request.frame.header.message_type != network::MessageType::kQueryRequest ||
      target.request_id == 0U) {
    return query_error(target, invalid("expected a nonzero QUERY_REQUEST"), limits_.protocol);
  }
  if (cancellation != nullptr && cancellation->requested()) {
    return query_error(target, {common::StatusCode::kCancelled, "native query was cancelled"},
                       limits_.protocol);
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
    auto tokens = query::tokenize_sql_v1(sql_text, limits_.sql_parser.lexer);
    if (!tokens.has_value())
      return query_error(target, tokens.error().status(), limits_.protocol);
    const std::span<const query::SqlToken> statement_tokens = tokens->tokens();
    if (statement_tokens.empty() ||
        statement_tokens.front().kind() != query::SqlTokenKind::kKeyword)
      return query_error(target, invalid("native SQL requires a supported statement keyword"),
                         limits_.protocol);
    if (statement_tokens.front().keyword() == query::SqlKeyword::kCreate) {
      if (database_ == nullptr)
        return query_error(target, unsupported("replicated native CREATE TABLE is not implemented"),
                           limits_.protocol);
      if (identities_ == nullptr)
        return query_error(target, unsupported("native CREATE TABLE has no identity source"),
                           limits_.protocol);
      if (limits_.ddl_retry_retention_positions == 0U)
        return query_error(target, invalid("DDL retry retention limit is invalid"),
                           limits_.protocol);
      auto parsed = query::parse_sql_v1_create_table(sql_text, limits_.sql_parser);
      if (!parsed.has_value())
        return query_error(target, parsed.error().status(), limits_.protocol);
      const std::size_t column_count = parsed->columns().size();
      auto bound = query::bind_sql_v1_create_table(std::move(*parsed), database_->query_catalog());
      if (!bound.has_value())
        return query_error(target, bound.error().status(), limits_.protocol);
      std::vector<common::Uuid> generated;
      generated.reserve(column_count + 3U);
      for (std::size_t index = 0U; index < column_count + 3U; ++index) {
        auto identity = identities_->generate();
        if (!identity.has_value())
          return query_error(target, identity.error(), limits_.protocol);
        if (identity->is_nil())
          return query_error(target, internal("identity source returned a nil UUID"),
                             limits_.protocol);
        generated.push_back(*identity);
      }
      auto unique = generated;
      std::ranges::sort(unique);
      if (std::ranges::adjacent_find(unique) != unique.end())
        return query_error(target, internal("identity source returned a duplicate UUID"),
                           limits_.protocol);
      auto table_id = schema::TableId::from_uuid(generated[0]);
      auto schema_id = schema::SchemaId::from_uuid(generated[1]);
      auto tablet_id = schema::TabletId::from_uuid(generated[2]);
      if (!table_id.has_value() || !schema_id.has_value() || !tablet_id.has_value())
        return query_error(target, internal("identity source returned an invalid durable UUID"),
                           limits_.protocol);
      std::vector<schema::ColumnId> column_ids;
      column_ids.reserve(column_count);
      for (std::size_t index = 0U; index < column_count; ++index) {
        auto column_id = schema::ColumnId::from_uuid(generated[index + 3U]);
        if (!column_id.has_value())
          return query_error(target, internal("identity source returned an invalid column UUID"),
                             limits_.protocol);
        column_ids.push_back(*column_id);
      }
      auto created = database_->create_table(*bound,
                                             {.table_id = *table_id,
                                              .schema_id = *schema_id,
                                              .column_ids = std::move(column_ids),
                                              .tablet_id = *tablet_id},
                                             limits_.ddl_retry_retention_positions);
      if (!created.has_value())
        return query_error(target, created.error(), limits_.protocol);
      return ddl_result(target, *created, limits_);
    }
    if (statement_tokens.front().keyword() == query::SqlKeyword::kInsert) {
      if (database_ == nullptr)
        return query_error(target, unsupported("replicated native INSERT is not implemented"),
                           limits_.protocol);
      if (identities_ == nullptr)
        return query_error(target, unsupported("native INSERT has no identity source"),
                           limits_.protocol);
      auto parsed = query::parse_sql_v1_insert(sql_text, limits_.sql_parser);
      if (!parsed.has_value())
        return query_error(target, parsed.error().status(), limits_.protocol);
      auto bound = query::bind_sql_v1_insert(std::move(*parsed), database_->query_catalog(),
                                             limits_.sql_insert);
      if (!bound.has_value())
        return query_error(target, bound.error().status(), limits_.protocol);
      auto materialized = query::materialize_sql_v1_insert_rows(*bound);
      if (!materialized.has_value())
        return query_error(target, materialized.error().status(), limits_.protocol);
      auto owned_batch =
          query::materialize_sql_v1_insert_batch(*materialized, limits_.insert_batch);
      if (!owned_batch.has_value())
        return query_error(target, owned_batch.error(), limits_.protocol);
      auto snapshots = database_->table_snapshots(bound->schema_ptr()->table_id());
      if (!snapshots.has_value())
        return query_error(target, snapshots.error(), limits_.protocol);
      if (snapshots->size() != 1U)
        return query_error(target, unsupported("native INSERT requires exactly one local tablet"),
                           limits_.protocol);
      ingest::TabletState* const tablet = database_->find_tablet(snapshots->front().tablet_id());
      if (tablet == nullptr)
        return query_error(target, internal("INSERT target tablet disappeared"), limits_.protocol);
      auto client_uuid = identities_->generate();
      auto batch_uuid = identities_->generate();
      if (!client_uuid.has_value())
        return query_error(target, client_uuid.error(), limits_.protocol);
      if (!batch_uuid.has_value())
        return query_error(target, batch_uuid.error(), limits_.protocol);
      if (client_uuid->is_nil() || batch_uuid->is_nil() || *client_uuid == *batch_uuid)
        return query_error(target, internal("identity source returned invalid INSERT UUIDs"),
                           limits_.protocol);
      auto client_id = ingest::ClientId::from_uuid(*client_uuid);
      auto client_batch_id = ingest::ClientBatchId::from_uuid(*batch_uuid);
      if (!client_id.has_value() || !client_batch_id.has_value())
        return query_error(target, internal("identity source returned invalid INSERT identities"),
                           limits_.protocol);
      const std::uint32_t applied_rows = owned_batch->row_count();
      auto retained_batch =
          std::make_shared<const columnar::OwnedColumnarBatch>(std::move(*owned_batch));
      auto executed = database_->execute_append(snapshots->front().tablet_id(),
                                                {.client_id = *client_id,
                                                 .client_batch_id = *client_batch_id,
                                                 .batch = std::move(retained_batch),
                                                 .durability = wal::WalDurabilityMode::kLocalSync});
      if (!executed.has_value())
        return query_error(target, executed.error(), limits_.protocol);
      auto flushed = database_->flush_ready_heads();
      if (!flushed.has_value())
        return query_error(target, flushed.error(), limits_.protocol);
      return insert_result(target, applied_rows, *executed, limits_);
    }
    if (statement_tokens.front().keyword() != query::SqlKeyword::kSelect)
      return query_error(target, unsupported("native SQL statement is not implemented"),
                         limits_.protocol);
    auto parsed = query::parse_sql_v1_select(sql_text, limits_.sql_parser);
    if (!parsed.has_value())
      return query_error(target, parsed.error().status(), limits_.protocol);
    if (distributed_mutable_query_ != nullptr &&
        !valid_distributed_rows_config(*distributed_mutable_query_)) {
      return query_error(target, invalid("distributed Native query config is invalid"),
                         limits_.protocol);
    }
    if (replicated_database_ != nullptr && replicated_read_barrier_ != nullptr &&
        distributed_mutable_query_ == nullptr &&
        target.protocol.protocol_major == network::kProtocolV2Major &&
        (target.protocol.feature_bits & network::kProtocolV2LeaderRedirectFeature) != 0U) {
      auto preliminary = replicated_database_->acquire_query_snapshot();
      if (!preliminary.has_value())
        return query_error(target, preliminary.error(), limits_.protocol);
      auto preliminary_bound =
          query::bind_sql_v1_select(std::move(*parsed), preliminary->catalog(), limits_.sql_binder);
      if (!preliminary_bound.has_value())
        return query_error(target, preliminary_bound.error().status(), limits_.protocol);
      if (!preliminary_bound->syntax().system_time().has_value() &&
          preliminary_bound->asof_joins().empty() && preliminary_bound->sources().size() == 1U) {
        const schema::TableId& table_id =
            preliminary_bound->sources().front().schema_ptr()->table_id();
        const ReplicatedSingleGroupQueryRoute* const query_route =
            preliminary->single_group_route(table_id);
        if (query_route != nullptr) {
          auto leader = replicated_database_->resolve_query_leader(*query_route);
          if (!leader.has_value())
            return query_error(target, leader.error(), limits_.protocol);
          if (leader->has_value())
            return query_redirect(target, **leader, limits_.protocol);
        }
      }
      parsed = query::parse_sql_v1_select(sql_text, limits_.sql_parser);
      if (!parsed.has_value())
        return query_error(target, parsed.error().status(), limits_.protocol);
    }
    std::optional<ReplicatedQuerySnapshot> replicated_snapshot;
    std::optional<std::vector<ReplicatedReadAuthority>> replicated_authorities;
    std::shared_ptr<const query::QueryCatalogSnapshot> query_catalog;
    if (replicated_database_ != nullptr) {
      common::Result<ReplicatedQuerySnapshot> acquired = [&]() {
        if (replicated_read_barrier_ == nullptr)
          return replicated_database_->acquire_query_snapshot();
        if (distributed_mutable_query_ == nullptr) {
          auto barriers = replicated_read_barrier_->await();
          if (!barriers.has_value())
            return common::Result<ReplicatedQuerySnapshot>{
                common::make_unexpected(std::move(barriers.error()))};
          return replicated_database_->acquire_query_snapshot(*barriers);
        }
        auto authorities = replicated_read_barrier_->await_authority();
        if (!authorities.has_value()) {
          return common::Result<ReplicatedQuerySnapshot>{
              common::make_unexpected(std::move(authorities.error()))};
        }
        std::vector<raft::GroupReadBarrier> barriers;
        barriers.reserve(authorities->size());
        for (const ReplicatedReadAuthority& authority : *authorities)
          barriers.push_back(authority.barrier);
        auto snapshot = replicated_database_->acquire_query_snapshot(barriers);
        if (snapshot.has_value())
          replicated_authorities.emplace(std::move(*authorities));
        return snapshot;
      }();
      if (!acquired.has_value())
        return query_error(target, acquired.error(), limits_.protocol);
      replicated_snapshot.emplace(std::move(*acquired));
      query_catalog = replicated_snapshot->catalog();
    } else if (database_ != nullptr) {
      query_catalog = database_->query_catalog();
    } else {
      return query_error(target, internal("native query database is unavailable"),
                         limits_.protocol);
    }
    auto bound =
        query::bind_sql_v1_select(std::move(*parsed), std::move(query_catalog), limits_.sql_binder);
    if (!bound.has_value())
      return query_error(target, bound.error().status(), limits_.protocol);
    if (bound->syntax().system_time().has_value()) {
      return query_error(target, unsupported("native FOR SYSTEM_TIME storage is not configured"),
                         limits_.protocol);
    }
    if (distributed_mutable_query_ != nullptr) {
      const NativeDistributedMutableVectorRowsQueryConfig& config = *distributed_mutable_query_;
      if (!replicated_snapshot.has_value() || !replicated_authorities.has_value()) {
        return query_error(target,
                           internal("distributed Native row query authority is unavailable"),
                           limits_.protocol);
      }
      std::optional<query::DistributedVectorRowsSqlPlan> lowered_rows;
      std::optional<query::DistributedVectorAggregateSqlPlan> lowered_aggregate;
      const query::DistributedVectorRowsSqlPlan* execution_sql{};
      if (bound->aggregate_query()) {
        auto aggregate = query::lower_bound_sql_select_to_distributed_vector_aggregate(
            *bound, config.aggregate_sql_lowering);
        if (!aggregate.has_value())
          return query_error(target, aggregate.error().status(), limits_.protocol);
        lowered_aggregate.emplace(std::move(*aggregate));
        execution_sql = &lowered_aggregate->input_rows;
      } else {
        auto rows =
            query::lower_bound_sql_select_to_distributed_vector_rows(*bound, config.sql_lowering);
        if (!rows.has_value())
          return query_error(target, rows.error().status(), limits_.protocol);
        lowered_rows.emplace(std::move(*rows));
        execution_sql = &*lowered_rows;
      }
      if (identities_ == nullptr) {
        return query_error(target, internal("distributed Native query identity source is absent"),
                           limits_.protocol);
      }
      auto query_id = identities_->generate();
      if (!query_id.has_value())
        return query_error(target, query_id.error(), limits_.protocol);
      if (query_id->is_nil()) {
        return query_error(target, internal("identity source returned a nil query UUID"),
                           limits_.protocol);
      }
      auto prepared = replicated_snapshot->prepare_linearizable_mutable_vector_rows_query(
          {.query_id = *query_id,
           .sql_plan = std::cref(*execution_sql),
           .group_authorities = *replicated_authorities},
          config.tls_contexts, config.route_resolution);
      if (!prepared.has_value())
        return query_error(target, prepared.error(), limits_.protocol);
      if (prepared->fragments.empty()) {
        return query_error(target, internal("distributed Native query prepared no fragments"),
                           limits_.protocol);
      }
      auto logical_identity = cluster::distributed_mutable_vector_query_logical_identity(
          std::span<const query::DistributedMutableVectorFragment>{prepared->fragments});
      if (!logical_identity.has_value())
        return query_error(target, logical_identity.error(), limits_.protocol);
      const auto execution_deadline = std::chrono::steady_clock::now() + config.execution_timeout;
      ReplicatedRoutedMutableVectorQuery current = std::move(*prepared);
      std::size_t authority_rebindings{};
      auto install_fresh_authority = [&](common::Status failure) -> common::Status {
        for (;;) {
          if (!retryable_authority_failure(failure.code()) ||
              authority_rebindings >= config.maximum_authority_rebindings) {
            return failure;
          }
          if (cancellation != nullptr && cancellation->requested()) {
            return {common::StatusCode::kCancelled, "native query was cancelled"};
          }
          if (std::chrono::steady_clock::now() >= execution_deadline) {
            return {common::StatusCode::kCancelled,
                    "distributed Native query execution deadline expired"};
          }
          ++authority_rebindings;
          auto authorities = replicated_read_barrier_->await_authority();
          if (!authorities.has_value()) {
            failure = std::move(authorities.error());
          } else {
            std::vector<raft::GroupReadBarrier> barriers;
            barriers.reserve(authorities->size());
            for (const ReplicatedReadAuthority& authority : *authorities)
              barriers.push_back(authority.barrier);
            auto snapshot = replicated_database_->acquire_query_snapshot(barriers);
            if (!snapshot.has_value()) {
              failure = std::move(snapshot.error());
            } else {
              auto replacement = snapshot->prepare_linearizable_mutable_vector_rows_query(
                  {.query_id = *query_id,
                   .sql_plan = std::cref(*execution_sql),
                   .group_authorities = *authorities},
                  config.tls_contexts, config.route_resolution);
              if (!replacement.has_value()) {
                failure = std::move(replacement.error());
              } else {
                auto replacement_identity =
                    cluster::distributed_mutable_vector_query_logical_identity(
                        std::span<const query::DistributedMutableVectorFragment>{
                            replacement->fragments});
                if (!replacement_identity.has_value())
                  return replacement_identity.error();
                if (*replacement_identity != *logical_identity) {
                  return invalid(
                      "fresh distributed Native authority changes logical query identity");
                }
                if (cancellation != nullptr && cancellation->requested())
                  return {common::StatusCode::kCancelled, "native query was cancelled"};
                if (std::chrono::steady_clock::now() >= execution_deadline) {
                  return {common::StatusCode::kCancelled,
                          "distributed Native query execution deadline expired"};
                }
                current = std::move(*replacement);
                return common::Status::ok();
              }
            }
          }
          if (!retryable_authority_failure(failure.code()) ||
              authority_rebindings >= config.maximum_authority_rebindings) {
            return failure;
          }
          std::this_thread::sleep_for(config.maximum_poll_wait);
        }
      };

      for (;;) {
        std::optional<common::Status> retryable_failure;
        std::vector<query::DistributedMutableVectorFragment> remote_fragments;
        remote_fragments.reserve(current.fragments.size());
        std::vector<schema::TabletId> ordered_tablets;
        ordered_tablets.reserve(current.fragments.size());
        for (const query::DistributedMutableVectorFragment& fragment : current.fragments)
          ordered_tablets.push_back(fragment.tablet_id);
        auto coordinator = cluster::DistributedVectorResultCoordinatorV2::create(
            *query_id, std::move(ordered_tablets), logical_identity->result_schema,
            config.execution.coordinator);
        if (!coordinator.has_value())
          return query_error(target, coordinator.error(), limits_.protocol);

        for (query::DistributedMutableVectorFragment& fragment : current.fragments) {
          if (cancellation != nullptr && cancellation->requested()) {
            return query_error(target,
                               {common::StatusCode::kCancelled, "native query was cancelled"},
                               limits_.protocol);
          }
          if (std::chrono::steady_clock::now() >= execution_deadline) {
            return query_error(target,
                               {common::StatusCode::kCancelled,
                                "distributed Native query execution deadline expired"},
                               limits_.protocol);
          }
          if (fragment.serving_node != config.source_node_id) {
            remote_fragments.push_back(std::move(fragment));
            continue;
          }
          if (config.local_worker == nullptr) {
            return query_error(target,
                               invalid("distributed Native local fragment worker is absent"),
                               limits_.protocol);
          }
          auto local_messages = config.local_worker->execute(fragment);
          if (!local_messages.has_value()) {
            if (retryable_authority_failure(local_messages.error().code())) {
              retryable_failure.emplace(std::move(local_messages.error()));
              break;
            }
            return query_error(target, local_messages.error(), limits_.protocol);
          }
          if (cancellation != nullptr && cancellation->requested()) {
            return query_error(target,
                               {common::StatusCode::kCancelled, "native query was cancelled"},
                               limits_.protocol);
          }
          for (const cluster::DistributedVectorResultExchangeMessage& message : *local_messages) {
            const common::Status accepted = coordinator->accept(message);
            if (!accepted.is_ok())
              return query_error(target, accepted, limits_.protocol);
          }
          if (std::chrono::steady_clock::now() >= execution_deadline) {
            return query_error(target,
                               {common::StatusCode::kCancelled,
                                "distributed Native query execution deadline expired"},
                               limits_.protocol);
          }
        }

        if (!retryable_failure.has_value() && !remote_fragments.empty()) {
          std::vector<cluster::DistributedQueryNodeRoute> remote_routes;
          remote_routes.reserve(current.routes.size());
          for (cluster::DistributedQueryNodeRoute& route : current.routes) {
            if (std::ranges::any_of(remote_fragments, [&](const auto& fragment) {
                  return fragment.serving_node == route.node_id;
                })) {
              remote_routes.push_back(std::move(route));
            }
          }
          auto portable = cluster::DistributedMutableVectorQueryExecution::create(
              config.source_node_id, std::move(remote_fragments), config.execution);
          if (!portable.has_value())
            return query_error(target, portable.error(), limits_.protocol);
          auto scheduler = cluster::DistributedMutableVectorQueryTcpExecution::create(
              std::move(*portable), {.authenticator = config.authenticator,
                                     .node_authorizer = config.node_authorizer,
                                     .routes = std::move(remote_routes),
                                     .carrier_limits = config.carrier,
                                     .connect_timeout = config.connect_timeout,
                                     .execution_deadline = execution_deadline,
                                     .maximum_rebindings = 0U});
          if (!scheduler.has_value())
            return query_error(target, scheduler.error(), limits_.protocol);
          while (scheduler->state() ==
                 cluster::DistributedMutableVectorQueryTcpExecutionState::kRunning) {
            if (cancellation != nullptr && cancellation->requested()) {
              static_cast<void>(scheduler->cancel());
              return query_error(target,
                                 {common::StatusCode::kCancelled, "native query was cancelled"},
                                 limits_.protocol);
            }
            const common::Status polled = scheduler->poll_once(config.maximum_poll_wait);
            if (!polled.is_ok()) {
              if (retryable_authority_failure(polled.code()))
                retryable_failure.emplace(polled);
              else
                return query_error(target, polled, limits_.protocol);
              break;
            }
          }
          if (!retryable_failure.has_value()) {
            if (scheduler->state() !=
                cluster::DistributedMutableVectorQueryTcpExecutionState::kComplete) {
              return query_error(target, scheduler->failure(), limits_.protocol);
            }
            auto remote_result = scheduler->take_result();
            if (!remote_result.has_value())
              return query_error(target, remote_result.error(), limits_.protocol);
            for (const cluster::DistributedVectorResultExchangeMessage& message :
                 remote_result->result.messages) {
              const common::Status accepted = coordinator->accept(message);
              if (!accepted.is_ok())
                return query_error(target, accepted, limits_.protocol);
            }
          }
        }

        if (retryable_failure.has_value()) {
          common::Status rebound = install_fresh_authority(std::move(*retryable_failure));
          if (!rebound.is_ok())
            return query_error(target, rebound, limits_.protocol);
          continue;
        }
        auto coordinated = std::move(*coordinator).finish();
        if (!coordinated.has_value())
          return query_error(target, coordinated.error(), limits_.protocol);
        if (lowered_aggregate.has_value()) {
          auto finalized = cluster::finalize_distributed_vector_aggregate_rows_v2(
              {.plan = logical_identity->plan, .result = std::move(*coordinated)},
              lowered_aggregate->intent, std::move(lowered_aggregate->result_schema),
              bounded_aggregate_finalization_limits(config, limits_));
          if (!finalized.has_value())
            return query_error(target, finalized.error(), limits_.protocol);
          return distributed_aggregate_result(target, std::move(*finalized), limits_);
        }
        auto finalized = cluster::finalize_distributed_vector_rows_v2(
            {.plan = logical_identity->plan, .result = std::move(*coordinated)},
            bounded_finalization_limits(config, limits_));
        if (!finalized.has_value())
          return query_error(target, finalized.error(), limits_.protocol);
        return distributed_rows_result(target, std::move(*finalized), limits_);
      }
    }
    auto resources = query::QueryResourceContext::create(limits_.maximum_query_memory_bytes);
    if (!resources.has_value())
      return query_error(target, resources.error(), limits_.protocol);
    std::unique_ptr<query::PhysicalOperator> pipeline;
    if (bound->asof_joins().empty()) {
      auto physical = query::lower_bound_sql_select(*bound, limits_.physical_lowering);
      if (!physical.has_value())
        return query_error(target, physical.error().status(), limits_.protocol);
      if (bound->sources().size() != 1U)
        return query_error(target, invalid("native SELECT requires exactly one table source"),
                           limits_.protocol);
      const std::shared_ptr<const schema::TableSchema>& source_schema =
          bound->sources().front().schema_ptr();
      common::Result<std::unique_ptr<query::PhysicalOperator>> instantiated =
          replicated_snapshot.has_value()
              ? replicated_snapshot->instantiate_table_pipeline(
                    *resources, source_schema->table_id(), source_schema->schema_id(), *physical,
                    limits_.replicated_tablet_pipeline)
              : database_->instantiate_table_pipeline(*resources, source_schema->table_id(),
                                                      source_schema->schema_id(), *physical,
                                                      limits_.tablet_pipeline);
      if (!instantiated.has_value())
        return query_error(target, instantiated.error(), limits_.protocol);
      pipeline = std::move(*instantiated);
    } else {
      if (replicated_snapshot.has_value())
        return query_error(target, unsupported("replicated native ASOF JOIN is not implemented"),
                           limits_.protocol);
      auto physical = query::lower_bound_sql_asof_select(*bound, limits_.physical_lowering);
      if (!physical.has_value())
        return query_error(target, physical.error().status(), limits_.protocol);
      std::vector<SingleNodeAsofSourceBinding> sources;
      sources.reserve(bound->sources().size());
      for (const query::BoundSqlSource& source : bound->sources()) {
        const std::shared_ptr<const schema::TableSchema>& source_schema = source.schema_ptr();
        sources.push_back({.table_id = source_schema->table_id(),
                           .destination_schema_id = source_schema->schema_id(),
                           .limits = limits_.tablet_pipeline});
      }
      auto instantiated = database_->instantiate_asof_pipeline(*resources, sources, *physical);
      if (!instantiated.has_value())
        return query_error(target, instantiated.error(), limits_.protocol);
      pipeline = std::move(*instantiated);
    }

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
      if (cancellation != nullptr && cancellation->requested()) {
        static_cast<void>(resources->request_cancel());
        return query_error(target, {common::StatusCode::kCancelled, "native query was cancelled"},
                           limits_.protocol);
      }
      auto step = pipeline->next(*resources);
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
