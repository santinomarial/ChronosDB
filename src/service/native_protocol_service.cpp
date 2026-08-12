#include "chronos/service/native_protocol_service.hpp"

#include "chronos/ingest/columnar_append_executor.hpp"
#include "chronos/ingest/committed_columnar_append.hpp"
#include "chronos/network/messages.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
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

} // namespace chronos::service
