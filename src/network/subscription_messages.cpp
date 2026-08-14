#include "chronos/network/subscription_messages.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/schema/utf8.hpp"

#include <algorithm>
#include <limits>
#include <new>
#include <optional>
#include <ranges>
#include <string>
#include <utility>

namespace chronos::network {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status corrupt(std::string message) {
  return {common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Status exhausted() {
  return {common::StatusCode::kResourceExhausted,
          "subscription protocol message allocation failed"};
}

[[nodiscard]] bool log_id_is_valid(const SubscriptionLogId& id) noexcept {
  return std::ranges::any_of(id, [](const std::byte value) { return value != std::byte{0}; });
}

[[nodiscard]] bool valid_mode(const SubscriptionStartMode mode) noexcept {
  return mode == SubscriptionStartMode::kNewQuery || mode == SubscriptionStartMode::kResume;
}

[[nodiscard]] bool valid_operation(const SubscriptionChangeOperation operation) noexcept {
  return operation == SubscriptionChangeOperation::kUpsert ||
         operation == SubscriptionChangeOperation::kDelete;
}

[[nodiscard]] bool valid_reason(const SubscriptionEndReason reason) noexcept {
  switch (reason) {
  case SubscriptionEndReason::kCancelled:
  case SubscriptionEndReason::kSchemaChanged:
  case SubscriptionEndReason::kStateExpired:
  case SubscriptionEndReason::kOverflowed:
  case SubscriptionEndReason::kServerShutdown:
    return true;
  }
  return false;
}

[[nodiscard]] common::Result<std::size_t> variable_size(const std::size_t envelope,
                                                        const std::size_t first,
                                                        const std::size_t second,
                                                        const SubscriptionMessageLimits& limits) {
  if (const common::Status status = validate_subscription_message_limits(limits); !status.is_ok())
    return common::make_unexpected(status);
  const auto one = common::checked_add(envelope, first);
  const auto total = one.has_value() ? common::checked_add(*one, second) : std::nullopt;
  if (!total.has_value() || *total > limits.protocol.maximum_payload_size ||
      first > std::numeric_limits<std::uint32_t>::max() ||
      second > std::numeric_limits<std::uint32_t>::max()) {
    return common::make_unexpected(
        invalid("subscription message exceeds the configured payload limit"));
  }
  return *total;
}

[[nodiscard]] common::Result<std::vector<std::byte>> allocated(const std::size_t size) {
  try {
    return std::vector<std::byte>(size);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted());
  }
}

[[nodiscard]] common::Status validate_token(const common::ByteView token,
                                            const SubscriptionMessageLimits& limits) {
  if (token.empty() || token.size() > limits.maximum_resume_token_bytes)
    return invalid("subscription resume token size is invalid");
  return common::Status::ok();
}

[[nodiscard]] common::Result<schema::TabletId> tablet_from(const common::ByteView bytes) {
  common::Uuid::Bytes value{};
  std::ranges::copy(bytes, value.begin());
  return schema::TabletId::from_bytes(value);
}

[[nodiscard]] common::Result<schema::SchemaId> schema_from(const common::ByteView bytes) {
  common::Uuid::Bytes value{};
  std::ranges::copy(bytes, value.begin());
  return schema::SchemaId::from_bytes(value);
}

} // namespace

common::Status validate_subscription_message_limits(const SubscriptionMessageLimits& limits) {
  if (const common::Status status = validate_protocol_limits(limits.protocol); !status.is_ok())
    return status;
  if (limits.maximum_resume_token_bytes == 0U || limits.maximum_result_key_bytes == 0U ||
      limits.maximum_resume_token_bytes > limits.protocol.maximum_payload_size ||
      limits.maximum_result_key_bytes > limits.protocol.maximum_payload_size) {
    return invalid("subscription message limits are inconsistent or zero");
  }
  return common::Status::ok();
}

common::Result<std::vector<std::byte>>
encode_subscription_request(const SubscriptionRequestView& request,
                            const SubscriptionMessageLimits& limits) {
  if (!valid_mode(request.mode) || request.subscription_id.is_nil() || request.body.empty())
    return common::make_unexpected(invalid("subscription request fields are invalid"));
  if (request.mode == SubscriptionStartMode::kNewQuery && !schema::is_valid_utf8(request.body))
    return common::make_unexpected(invalid("subscription query is not valid UTF-8"));
  if (request.mode == SubscriptionStartMode::kResume &&
      !validate_token(request.body, limits).is_ok())
    return common::make_unexpected(invalid("subscription resume token size is invalid"));
  auto size = variable_size(kSubscribeRequestEnvelopeSize, request.body.size(), 0U, limits);
  if (!size.has_value())
    return common::make_unexpected(size.error());
  auto bytes = allocated(*size);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::ByteWriter writer{*bytes};
  if (const common::Status status = writer.write_u16_le(1U); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u8(static_cast<std::uint8_t>(request.mode));
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u8(0U); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_exact(request.subscription_id.bytes());
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status =
          writer.write_u32_le(static_cast<std::uint32_t>(request.body.size()));
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u32_le(0U); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_exact(request.body); !status.is_ok())
    return common::make_unexpected(status);
  return bytes;
}

common::Result<SubscriptionRequestView>
decode_subscription_request(const common::ByteView payload,
                            const SubscriptionMessageLimits& limits) {
  if (const common::Status status = validate_subscription_message_limits(limits); !status.is_ok())
    return common::make_unexpected(status);
  if (payload.size() < kSubscribeRequestEnvelopeSize ||
      payload.size() > limits.protocol.maximum_payload_size)
    return common::make_unexpected(corrupt("SUBSCRIBE_REQUEST payload size is invalid"));
  common::ByteReader reader{payload};
  const auto format = reader.read_u16_le();
  const auto raw_mode = reader.read_u8();
  const auto reserved8 = reader.read_u8();
  const auto subscription = reader.read_exact(common::Uuid::kSize);
  const auto body_size = reader.read_u32_le();
  const auto reserved32 = reader.read_u32_le();
  if (!format.has_value() || !raw_mode.has_value() || !reserved8.has_value() ||
      !subscription.has_value() || !body_size.has_value() || !reserved32.has_value() ||
      *format != 1U || *reserved8 != 0U || *reserved32 != 0U || *body_size == 0U ||
      *body_size != reader.remaining()) {
    return common::make_unexpected(corrupt("SUBSCRIBE_REQUEST envelope is invalid"));
  }
  common::Uuid::Bytes subscription_bytes{};
  std::ranges::copy(*subscription, subscription_bytes.begin());
  const SubscriptionStartMode mode = static_cast<SubscriptionStartMode>(*raw_mode);
  const common::ByteView body = *reader.read_exact(*body_size);
  if (!valid_mode(mode) || common::Uuid{subscription_bytes}.is_nil() ||
      (mode == SubscriptionStartMode::kNewQuery && !schema::is_valid_utf8(body)) ||
      (mode == SubscriptionStartMode::kResume && !validate_token(body, limits).is_ok())) {
    return common::make_unexpected(corrupt("SUBSCRIBE_REQUEST semantics are invalid"));
  }
  return SubscriptionRequestView{mode, common::Uuid{subscription_bytes}, body};
}

common::Result<std::vector<std::byte>>
encode_subscription_ready(const common::ByteView resume_token,
                          const SubscriptionMessageLimits& limits) {
  if (const common::Status status = validate_token(resume_token, limits); !status.is_ok())
    return common::make_unexpected(status);
  auto size = variable_size(kSubscriptionReadyEnvelopeSize, resume_token.size(), 0U, limits);
  if (!size.has_value())
    return common::make_unexpected(size.error());
  auto bytes = allocated(*size);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::ByteWriter writer{*bytes};
  if (const common::Status status = writer.write_u16_le(1U); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u16_le(0U); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status =
          writer.write_u32_le(static_cast<std::uint32_t>(resume_token.size()));
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_exact(resume_token); !status.is_ok())
    return common::make_unexpected(status);
  return bytes;
}

common::Result<SubscriptionReadyView>
decode_subscription_ready(const common::ByteView payload, const SubscriptionMessageLimits& limits) {
  if (const common::Status status = validate_subscription_message_limits(limits); !status.is_ok())
    return common::make_unexpected(status);
  if (payload.size() < kSubscriptionReadyEnvelopeSize ||
      payload.size() > limits.protocol.maximum_payload_size)
    return common::make_unexpected(corrupt("SUBSCRIPTION_READY payload size is invalid"));
  common::ByteReader reader{payload};
  const auto format = reader.read_u16_le();
  const auto reserved = reader.read_u16_le();
  const auto token_size = reader.read_u32_le();
  if (!format.has_value() || !reserved.has_value() || !token_size.has_value() || *format != 1U ||
      *reserved != 0U || *token_size != reader.remaining())
    return common::make_unexpected(corrupt("SUBSCRIPTION_READY envelope is invalid"));
  const common::ByteView token = *reader.read_exact(*token_size);
  if (!validate_token(token, limits).is_ok())
    return common::make_unexpected(corrupt("SUBSCRIPTION_READY token is invalid"));
  return SubscriptionReadyView{token};
}

common::Result<std::vector<std::byte>>
encode_subscription_change(const SubscriptionChangeView& change,
                           const SubscriptionMessageLimits& limits) {
  if (!valid_operation(change.operation) || change.delivery_sequence == 0U ||
      change.tablet_id.uuid().is_nil() || !log_id_is_valid(change.log_id) ||
      change.record_sequence == 0U || change.schema_id.uuid().is_nil() ||
      change.schema_version.value() == 0U || change.result_key.empty() ||
      change.result_key.size() > limits.maximum_result_key_bytes ||
      (change.operation == SubscriptionChangeOperation::kDelete && !change.payload.empty())) {
    return common::make_unexpected(invalid("subscription change fields are invalid"));
  }
  auto size = variable_size(kSubscriptionChangeEnvelopeSize, change.result_key.size(),
                            change.payload.size(), limits);
  if (!size.has_value())
    return common::make_unexpected(size.error());
  auto bytes = allocated(*size);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::ByteWriter writer{*bytes};
  if (const common::Status status = writer.write_u16_le(1U); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u8(static_cast<std::uint8_t>(change.operation));
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u8(0U); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u64_le(change.delivery_sequence); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_exact(change.tablet_id.bytes()); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_exact(change.log_id); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u64_le(change.record_sequence); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_exact(change.schema_id.bytes()); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u64_le(change.schema_version.value());
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status =
          writer.write_u32_le(static_cast<std::uint32_t>(change.result_key.size()));
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status =
          writer.write_u32_le(static_cast<std::uint32_t>(change.payload.size()));
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_exact(change.result_key); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_exact(change.payload); !status.is_ok())
    return common::make_unexpected(status);
  return bytes;
}

common::Result<SubscriptionChangeView>
decode_subscription_change(const common::ByteView payload,
                           const SubscriptionMessageLimits& limits) {
  if (const common::Status status = validate_subscription_message_limits(limits); !status.is_ok())
    return common::make_unexpected(status);
  if (payload.size() < kSubscriptionChangeEnvelopeSize ||
      payload.size() > limits.protocol.maximum_payload_size)
    return common::make_unexpected(corrupt("SUBSCRIPTION_CHANGE payload size is invalid"));
  common::ByteReader reader{payload};
  const auto format = reader.read_u16_le();
  const auto raw_operation = reader.read_u8();
  const auto reserved = reader.read_u8();
  const auto delivery = reader.read_u64_le();
  const auto tablet_bytes = reader.read_exact(common::Uuid::kSize);
  const auto log_bytes = reader.read_exact(SubscriptionLogId{}.size());
  const auto record = reader.read_u64_le();
  const auto schema_bytes = reader.read_exact(common::Uuid::kSize);
  const auto schema_version = reader.read_u64_le();
  const auto key_size = reader.read_u32_le();
  const auto body_size = reader.read_u32_le();
  if (!format.has_value() || !raw_operation.has_value() || !reserved.has_value() ||
      !delivery.has_value() || !tablet_bytes.has_value() || !log_bytes.has_value() ||
      !record.has_value() || !schema_bytes.has_value() || !schema_version.has_value() ||
      !key_size.has_value() || !body_size.has_value() || *format != 1U || *reserved != 0U ||
      *key_size == 0U || *key_size > limits.maximum_result_key_bytes) {
    return common::make_unexpected(corrupt("SUBSCRIPTION_CHANGE envelope is invalid"));
  }
  const auto variable = common::checked_add(static_cast<std::size_t>(*key_size),
                                            static_cast<std::size_t>(*body_size));
  if (!variable.has_value() || *variable != reader.remaining())
    return common::make_unexpected(corrupt("SUBSCRIPTION_CHANGE lengths are invalid"));
  const auto tablet = tablet_from(*tablet_bytes);
  const auto schema_id = schema_from(*schema_bytes);
  const auto version = schema::SchemaVersion::from_value(*schema_version);
  SubscriptionLogId log{};
  std::ranges::copy(*log_bytes, log.begin());
  const SubscriptionChangeOperation operation =
      static_cast<SubscriptionChangeOperation>(*raw_operation);
  const common::ByteView key = *reader.read_exact(*key_size);
  const common::ByteView body = *reader.read_exact(*body_size);
  if (!valid_operation(operation) || *delivery == 0U || !tablet.has_value() ||
      !log_id_is_valid(log) || *record == 0U || !schema_id.has_value() || !version.has_value() ||
      (operation == SubscriptionChangeOperation::kDelete && !body.empty())) {
    return common::make_unexpected(corrupt("SUBSCRIPTION_CHANGE semantics are invalid"));
  }
  return SubscriptionChangeView{operation,  *delivery, *tablet, log, *record,
                                *schema_id, *version,  key,     body};
}

common::Result<std::vector<std::byte>>
encode_subscription_acknowledgement(const SubscriptionAcknowledgement& acknowledgement) {
  if (acknowledgement.delivery_sequence == 0U)
    return common::make_unexpected(invalid("subscription acknowledgement sequence is zero"));
  auto bytes = allocated(kSubscriptionAcknowledgeSize);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::ByteWriter writer{*bytes};
  if (const common::Status status = writer.write_u16_le(1U); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u16_le(0U); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u64_le(acknowledgement.delivery_sequence);
      !status.is_ok())
    return common::make_unexpected(status);
  return bytes;
}

common::Result<SubscriptionAcknowledgement>
decode_subscription_acknowledgement(const common::ByteView payload) {
  if (payload.size() != kSubscriptionAcknowledgeSize)
    return common::make_unexpected(
        corrupt("SUBSCRIPTION_ACKNOWLEDGE payload size is not canonical"));
  common::ByteReader reader{payload};
  const auto format = reader.read_u16_le();
  const auto reserved = reader.read_u16_le();
  const auto sequence = reader.read_u64_le();
  if (!format.has_value() || !reserved.has_value() || !sequence.has_value() || *format != 1U ||
      *reserved != 0U || *sequence == 0U)
    return common::make_unexpected(corrupt("SUBSCRIPTION_ACKNOWLEDGE payload is invalid"));
  return SubscriptionAcknowledgement{*sequence};
}

common::Result<std::vector<std::byte>>
encode_subscription_checkpoint(const SubscriptionCheckpointView& checkpoint,
                               const SubscriptionMessageLimits& limits) {
  if (checkpoint.acknowledged_delivery_sequence == 0U)
    return common::make_unexpected(invalid("subscription checkpoint sequence is zero"));
  if (const common::Status status = validate_token(checkpoint.resume_token, limits);
      !status.is_ok())
    return common::make_unexpected(status);
  auto size = variable_size(kSubscriptionCheckpointEnvelopeSize, checkpoint.resume_token.size(), 0U,
                            limits);
  if (!size.has_value())
    return common::make_unexpected(size.error());
  auto bytes = allocated(*size);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::ByteWriter writer{*bytes};
  if (const common::Status status = writer.write_u16_le(1U); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u16_le(0U); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u64_le(checkpoint.acknowledged_delivery_sequence);
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status =
          writer.write_u32_le(static_cast<std::uint32_t>(checkpoint.resume_token.size()));
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u32_le(0U); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_exact(checkpoint.resume_token); !status.is_ok())
    return common::make_unexpected(status);
  return bytes;
}

common::Result<SubscriptionCheckpointView>
decode_subscription_checkpoint(const common::ByteView payload,
                               const SubscriptionMessageLimits& limits) {
  if (const common::Status status = validate_subscription_message_limits(limits); !status.is_ok())
    return common::make_unexpected(status);
  if (payload.size() < kSubscriptionCheckpointEnvelopeSize ||
      payload.size() > limits.protocol.maximum_payload_size)
    return common::make_unexpected(corrupt("SUBSCRIPTION_CHECKPOINT payload size is invalid"));
  common::ByteReader reader{payload};
  const auto format = reader.read_u16_le();
  const auto reserved16 = reader.read_u16_le();
  const auto sequence = reader.read_u64_le();
  const auto token_size = reader.read_u32_le();
  const auto reserved32 = reader.read_u32_le();
  if (!format.has_value() || !reserved16.has_value() || !sequence.has_value() ||
      !token_size.has_value() || !reserved32.has_value() || *format != 1U || *reserved16 != 0U ||
      *reserved32 != 0U || *sequence == 0U || *token_size != reader.remaining()) {
    return common::make_unexpected(corrupt("SUBSCRIPTION_CHECKPOINT envelope is invalid"));
  }
  const common::ByteView token = *reader.read_exact(*token_size);
  if (!validate_token(token, limits).is_ok())
    return common::make_unexpected(corrupt("SUBSCRIPTION_CHECKPOINT token is invalid"));
  return SubscriptionCheckpointView{*sequence, token};
}

common::Result<std::vector<std::byte>>
encode_subscription_end(const SubscriptionEndView& end, const SubscriptionMessageLimits& limits) {
  if (!valid_reason(end.reason))
    return common::make_unexpected(invalid("subscription end reason is unassigned"));
  if (const common::Status status = validate_token(end.resume_token, limits); !status.is_ok())
    return common::make_unexpected(status);
  auto size = variable_size(kSubscriptionEndEnvelopeSize, end.resume_token.size(), 0U, limits);
  if (!size.has_value())
    return common::make_unexpected(size.error());
  auto bytes = allocated(*size);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  common::ByteWriter writer{*bytes};
  if (const common::Status status = writer.write_u16_le(1U); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u16_le(static_cast<std::uint16_t>(end.reason));
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u32_le(0U); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u64_le(end.safe_delivery_sequence);
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status =
          writer.write_u32_le(static_cast<std::uint32_t>(end.resume_token.size()));
      !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_u32_le(0U); !status.is_ok())
    return common::make_unexpected(status);
  if (const common::Status status = writer.write_exact(end.resume_token); !status.is_ok())
    return common::make_unexpected(status);
  return bytes;
}

common::Result<SubscriptionEndView>
decode_subscription_end(const common::ByteView payload, const SubscriptionMessageLimits& limits) {
  if (const common::Status status = validate_subscription_message_limits(limits); !status.is_ok())
    return common::make_unexpected(status);
  if (payload.size() < kSubscriptionEndEnvelopeSize ||
      payload.size() > limits.protocol.maximum_payload_size)
    return common::make_unexpected(corrupt("SUBSCRIPTION_END payload size is invalid"));
  common::ByteReader reader{payload};
  const auto format = reader.read_u16_le();
  const auto raw_reason = reader.read_u16_le();
  const auto reserved_a = reader.read_u32_le();
  const auto sequence = reader.read_u64_le();
  const auto token_size = reader.read_u32_le();
  const auto reserved_b = reader.read_u32_le();
  if (!format.has_value() || !raw_reason.has_value() || !reserved_a.has_value() ||
      !sequence.has_value() || !token_size.has_value() || !reserved_b.has_value() ||
      *format != 1U || *reserved_a != 0U || *reserved_b != 0U ||
      *token_size != reader.remaining()) {
    return common::make_unexpected(corrupt("SUBSCRIPTION_END envelope is invalid"));
  }
  if (*raw_reason > std::numeric_limits<std::uint8_t>::max())
    return common::make_unexpected(corrupt("SUBSCRIPTION_END reason exceeds its value range"));
  const SubscriptionEndReason reason =
      static_cast<SubscriptionEndReason>(static_cast<std::uint8_t>(*raw_reason));
  const common::ByteView token = *reader.read_exact(*token_size);
  if (!valid_reason(reason) || !validate_token(token, limits).is_ok())
    return common::make_unexpected(corrupt("SUBSCRIPTION_END semantics are invalid"));
  return SubscriptionEndView{reason, *sequence, token};
}

} // namespace chronos::network
