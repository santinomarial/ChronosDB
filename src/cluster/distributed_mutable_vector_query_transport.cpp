#include "chronos/cluster/distributed_mutable_vector_query_transport.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <new>
#include <ranges>
#include <stdexcept>
#include <utility>

namespace chronos::cluster {
namespace {

inline constexpr auto kRequestMagic = kDistributedMutableVectorQueryRequestMagicV1;
inline constexpr std::uint16_t kMajor = 1U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kHeaderCrcOffset = 76U;
inline constexpr std::size_t kMinimumResultSchemaSize =
    query::distributed_vector_result_schema_format::kHeaderLength +
    query::distributed_vector_result_schema_format::kDescriptorFixedLength + 1U +
    query::distributed_vector_result_schema_format::kTrailerLength;
inline constexpr std::size_t kMinimumPlanSize =
    query::distributed_vector_plan_format::kHeaderLength +
    query::distributed_vector_plan_format::kTrailerLength;
inline constexpr std::size_t kMinimumMutableFragmentSize =
    query::distributed_mutable_vector_fragment_format::kHeaderLength + sizeof(std::uint32_t) +
    kMinimumPlanSize + kMinimumResultSchemaSize +
    query::distributed_mutable_vector_fragment_format::kTrailerLength;

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corruption(const char* message) {
  return {common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status unsupported(const char* message) {
  return {common::StatusCode::kNotSupported, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] common::Status unauthenticated(const char* message) {
  return {common::StatusCode::kUnauthenticated, message};
}

[[nodiscard]] common::Status unavailable(const char* message) {
  return {common::StatusCode::kUnavailable, message};
}

[[nodiscard]] bool zero(const common::ByteView bytes) {
  return std::ranges::all_of(bytes, [](const std::byte value) { return value == std::byte{}; });
}

[[nodiscard]] bool retryable_status(const common::StatusCode code) noexcept {
  return code == common::StatusCode::kUnavailable ||
         code == common::StatusCode::kResourceExhausted || code == common::StatusCode::kIoError;
}

[[nodiscard]] DistributedMutableVectorQuerySender::TimePoint
saturating_add(const DistributedMutableVectorQuerySender::TimePoint now,
               const std::chrono::milliseconds delay) noexcept {
  const auto converted =
      std::chrono::duration_cast<DistributedMutableVectorQuerySender::TimePoint::duration>(delay);
  if (now > DistributedMutableVectorQuerySender::TimePoint::max() - converted)
    return DistributedMutableVectorQuerySender::TimePoint::max();
  return now + converted;
}

[[nodiscard]] common::Result<std::size_t> request_frame_length(const common::ByteView header) {
  if (header.size() != kDistributedMutableVectorQueryRequestHeaderSize ||
      !std::ranges::equal(header.first(kRequestMagic.size()), kRequestMagic)) {
    return common::make_unexpected(
        corruption("mutable vector query request streaming header is invalid"));
  }
  common::ByteReader crc_reader{header.last(4U)};
  const auto stored_crc = crc_reader.read_u32_le();
  if (!stored_crc.has_value() || *stored_crc != common::crc32c(header.first(kHeaderCrcOffset))) {
    return common::make_unexpected(
        corruption("mutable vector query request streaming header checksum differs"));
  }
  common::ByteReader reader{header.subspan(kRequestMagic.size())};
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto total_length = reader.read_u64_le();
  const auto source = reader.read_u64_le();
  const auto target = reader.read_u64_le();
  const auto payload_length = reader.read_u64_le();
  static_cast<void>(reader.skip(4U));
  const auto reserved = reader.read_exact(24U);
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !total_length.has_value() || !source.has_value() || !target.has_value() ||
      !payload_length.has_value() || !reserved.has_value()) {
    return common::make_unexpected(
        corruption("mutable vector query request streaming header is truncated"));
  }
  if (*major != kMajor || *minor != kMinor) {
    return common::make_unexpected(
        unsupported("mutable vector query request version is unsupported"));
  }
  if (*header_length != kDistributedMutableVectorQueryRequestHeaderSize || *source == 0U ||
      *target == 0U || *source == *target || !zero(*reserved) ||
      *payload_length < kMinimumMutableFragmentSize ||
      *payload_length > query::distributed_mutable_vector_fragment_format::kMaximumFrameLength ||
      *total_length != kDistributedMutableVectorQueryRequestHeaderSize + *payload_length +
                           kDistributedMutableVectorQueryRequestTrailerSize ||
      *total_length > kMaximumDistributedMutableVectorQueryRequestSize) {
    return common::make_unexpected(
        corruption("mutable vector query request streaming header is invalid"));
  }
  return static_cast<std::size_t>(*total_length);
}

[[nodiscard]] common::Result<std::vector<DistributedVectorResultExchangeMessage>>
execute_worker(DistributedMutableVectorQueryWorkerService& worker,
               const query::DistributedMutableVectorFragment& fragment) noexcept {
  try {
    return worker.execute(fragment);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("mutable vector query worker allocation failed"));
  } catch (...) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "mutable vector query worker threw"});
  }
}

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_sender_request(const raft::NodeId source_node_id,
                      const query::DistributedMutableVectorFragment& fragment) {
  return encode_distributed_mutable_vector_query_request({.source_node_id = source_node_id,
                                                          .target_node_id = fragment.serving_node,
                                                          .fragment = fragment});
}

} // namespace

common::Result<std::vector<std::byte>> encode_distributed_mutable_vector_query_request(
    const DistributedMutableVectorQueryRequest& request) {
  if (request.source_node_id == 0U || request.target_node_id == 0U ||
      request.source_node_id == request.target_node_id ||
      request.target_node_id != request.fragment.serving_node) {
    return common::make_unexpected(invalid("mutable vector query request route is invalid"));
  }
  auto payload = query::encode_distributed_mutable_vector_fragment(request.fragment);
  if (!payload.has_value())
    return common::make_unexpected(payload.error());
  try {
    const std::size_t total = kDistributedMutableVectorQueryRequestHeaderSize +
                              payload->bytes().size() +
                              kDistributedMutableVectorQueryRequestTrailerSize;
    std::vector<std::byte> bytes(total);
    common::ByteWriter writer{bytes};
    common::Status write = writer.write_exact(kRequestMagic);
    if (write.is_ok())
      write = writer.write_u16_le(kMajor);
    if (write.is_ok())
      write = writer.write_u16_le(kMinor);
    if (write.is_ok())
      write = writer.write_u32_le(kDistributedMutableVectorQueryRequestHeaderSize);
    if (write.is_ok())
      write = writer.write_u64_le(total);
    if (write.is_ok())
      write = writer.write_u64_le(request.source_node_id);
    if (write.is_ok())
      write = writer.write_u64_le(request.target_node_id);
    if (write.is_ok())
      write = writer.write_u64_le(payload->bytes().size());
    if (write.is_ok())
      write = writer.write_u32_le(common::crc32c(payload->bytes()));
    if (write.is_ok())
      write = writer.zero_fill(24U);
    if (!write.is_ok() || writer.offset() != kHeaderCrcOffset)
      return common::make_unexpected(
          invalid("mutable vector query request header is inconsistent"));
    write = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(kHeaderCrcOffset)));
    if (write.is_ok())
      write = writer.write_exact(payload->bytes());
    if (write.is_ok())
      write = writer.write_u32_le(common::crc32c(common::ByteView{bytes}.first(total - 4U)));
    if (!write.is_ok() || !writer.full())
      return common::make_unexpected(invalid("mutable vector query request frame is inconsistent"));
    return bytes;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("mutable vector query request allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(
        exhausted("mutable vector query request exceeds container limits"));
  }
}

common::Result<DistributedMutableVectorQueryRequest>
decode_distributed_mutable_vector_query_request_exact(const common::ByteView bytes) {
  if (bytes.size() < kDistributedMutableVectorQueryRequestHeaderSize + kMinimumMutableFragmentSize +
                         kDistributedMutableVectorQueryRequestTrailerSize ||
      bytes.size() > kMaximumDistributedMutableVectorQueryRequestSize) {
    return common::make_unexpected(corruption("mutable vector query request length is invalid"));
  }
  if (!std::ranges::equal(bytes.first(kRequestMagic.size()), kRequestMagic))
    return common::make_unexpected(corruption("mutable vector query request magic is invalid"));
  common::ByteReader header_crc_reader{bytes.subspan(kHeaderCrcOffset, 4U)};
  const auto stored_header_crc = header_crc_reader.read_u32_le();
  if (!stored_header_crc.has_value() ||
      *stored_header_crc != common::crc32c(bytes.first(kHeaderCrcOffset))) {
    return common::make_unexpected(
        corruption("mutable vector query request header checksum differs"));
  }
  common::ByteReader reader{bytes};
  static_cast<void>(reader.skip(kRequestMagic.size()));
  const auto major = reader.read_u16_le();
  const auto minor = reader.read_u16_le();
  const auto header_length = reader.read_u32_le();
  const auto total_length = reader.read_u64_le();
  const auto source = reader.read_u64_le();
  const auto target = reader.read_u64_le();
  const auto payload_length = reader.read_u64_le();
  const auto payload_crc = reader.read_u32_le();
  const auto reserved = reader.read_exact(24U);
  static_cast<void>(reader.skip(4U));
  if (!major.has_value() || !minor.has_value() || !header_length.has_value() ||
      !total_length.has_value() || !source.has_value() || !target.has_value() ||
      !payload_length.has_value() || !payload_crc.has_value() || !reserved.has_value()) {
    return common::make_unexpected(corruption("mutable vector query request header is truncated"));
  }
  if (*major != kMajor || *minor != kMinor)
    return common::make_unexpected(
        unsupported("mutable vector query request version is unsupported"));
  if (*header_length != kDistributedMutableVectorQueryRequestHeaderSize ||
      *total_length != bytes.size() || *source == 0U || *target == 0U || *source == *target ||
      !zero(*reserved) || *payload_length < kMinimumMutableFragmentSize ||
      *payload_length > query::distributed_mutable_vector_fragment_format::kMaximumFrameLength ||
      *payload_length != bytes.size() - kDistributedMutableVectorQueryRequestHeaderSize -
                             kDistributedMutableVectorQueryRequestTrailerSize) {
    return common::make_unexpected(corruption("mutable vector query request header is invalid"));
  }
  common::ByteReader trailer{bytes.last(4U)};
  const auto stored_crc = trailer.read_u32_le();
  if (!stored_crc.has_value() || *stored_crc != common::crc32c(bytes.first(bytes.size() - 4U)))
    return common::make_unexpected(corruption("mutable vector query request checksum differs"));
  const common::ByteView payload = bytes.subspan(kDistributedMutableVectorQueryRequestHeaderSize,
                                                 static_cast<std::size_t>(*payload_length));
  if (*payload_crc != common::crc32c(payload))
    return common::make_unexpected(
        corruption("mutable vector query request payload checksum differs"));
  auto fragment = query::decode_distributed_mutable_vector_fragment_exact(payload);
  if (!fragment.has_value())
    return common::make_unexpected(fragment.error());
  if (fragment->serving_node != *target)
    return common::make_unexpected(corruption("mutable vector query target differs from fragment"));
  return DistributedMutableVectorQueryRequest{*source, *target, std::move(*fragment)};
}

DistributedMutableVectorQueryRequestReader::DistributedMutableVectorQueryRequestReader(
    const std::size_t maximum_frame_length) noexcept
    : maximum_frame_length_(maximum_frame_length) {}

common::Result<DistributedMutableVectorQueryRequestReadStep>
DistributedMutableVectorQueryRequestReader::consume(const common::ByteView bytes) {
  if (failure_.has_value())
    return common::make_unexpected(*failure_);
  constexpr std::size_t kMinimum = kDistributedMutableVectorQueryRequestHeaderSize +
                                   kMinimumMutableFragmentSize +
                                   kDistributedMutableVectorQueryRequestTrailerSize;
  if (maximum_frame_length_ < kMinimum ||
      maximum_frame_length_ > kMaximumDistributedMutableVectorQueryRequestSize) {
    return common::make_unexpected(invalid("mutable vector query request reader limit is invalid"));
  }
  std::size_t consumed{};
  if (frame_.empty()) {
    const std::size_t copied = std::min(bytes.size(), header_.size() - header_bytes_);
    std::ranges::copy(bytes.first(copied),
                      header_.begin() + static_cast<std::ptrdiff_t>(header_bytes_));
    header_bytes_ += copied;
    consumed += copied;
    if (header_bytes_ != header_.size())
      return DistributedMutableVectorQueryRequestReadStep{.consumed_bytes = consumed};
    const auto frame_length = request_frame_length(header_);
    if (!frame_length.has_value()) {
      failure_ = frame_length.error();
      return common::make_unexpected(*failure_);
    }
    if (*frame_length > maximum_frame_length_) {
      failure_ = exhausted("mutable vector query request exceeds reader frame limit");
      return common::make_unexpected(*failure_);
    }
    try {
      frame_.resize(*frame_length);
    } catch (const std::bad_alloc&) {
      failure_ = exhausted("mutable vector query request reader allocation failed");
      return common::make_unexpected(*failure_);
    } catch (const std::length_error&) {
      failure_ = exhausted("mutable vector query request reader exceeds container limits");
      return common::make_unexpected(*failure_);
    }
    std::ranges::copy(header_, frame_.begin());
    frame_bytes_ = header_.size();
  }
  const common::ByteView remaining = bytes.subspan(consumed);
  const std::size_t copied = std::min(remaining.size(), frame_.size() - frame_bytes_);
  std::ranges::copy(remaining.first(copied),
                    frame_.begin() + static_cast<std::ptrdiff_t>(frame_bytes_));
  frame_bytes_ += copied;
  consumed += copied;
  if (frame_bytes_ != frame_.size())
    return DistributedMutableVectorQueryRequestReadStep{.consumed_bytes = consumed};
  auto decoded = decode_distributed_mutable_vector_query_request_exact(frame_);
  if (!decoded.has_value()) {
    failure_ = decoded.error();
    return common::make_unexpected(*failure_);
  }
  DistributedMutableVectorQueryRequest result = std::move(*decoded);
  frame_.clear();
  frame_bytes_ = 0U;
  header_bytes_ = 0U;
  return DistributedMutableVectorQueryRequestReadStep{.consumed_bytes = consumed,
                                                      .request = std::move(result)};
}

std::size_t DistributedMutableVectorQueryRequestReader::buffered_bytes() const noexcept {
  return frame_.empty() ? header_bytes_ : frame_bytes_;
}

bool DistributedMutableVectorQueryRequestReader::failed() const noexcept {
  return failure_.has_value();
}

DistributedMutableVectorQueryRequestWriteCursor::DistributedMutableVectorQueryRequestWriteCursor(
    std::vector<std::byte> encoded_frame) noexcept
    : encoded_frame_(std::move(encoded_frame)) {}

DistributedMutableVectorQueryRequestWriteCursor::DistributedMutableVectorQueryRequestWriteCursor(
    DistributedMutableVectorQueryRequestWriteCursor&& other) noexcept
    : encoded_frame_(std::move(other.encoded_frame_)), written_bytes_(other.written_bytes_) {
  other.written_bytes_ = other.encoded_frame_.size();
}

DistributedMutableVectorQueryRequestWriteCursor&
DistributedMutableVectorQueryRequestWriteCursor::operator=(
    DistributedMutableVectorQueryRequestWriteCursor&& other) noexcept {
  if (this != &other) {
    encoded_frame_ = std::move(other.encoded_frame_);
    written_bytes_ = other.written_bytes_;
    other.written_bytes_ = other.encoded_frame_.size();
  }
  return *this;
}

common::Result<DistributedMutableVectorQueryRequestWriteCursor>
DistributedMutableVectorQueryRequestWriteCursor::create(
    const DistributedMutableVectorQueryRequest& request) {
  auto encoded = encode_distributed_mutable_vector_query_request(request);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  return DistributedMutableVectorQueryRequestWriteCursor{std::move(*encoded)};
}

common::ByteView DistributedMutableVectorQueryRequestWriteCursor::pending_write() const noexcept {
  return common::ByteView{encoded_frame_}.subspan(written_bytes_);
}

common::Status
DistributedMutableVectorQueryRequestWriteCursor::consume_written(const std::size_t bytes) noexcept {
  if (bytes > encoded_frame_.size() - written_bytes_)
    return invalid("written byte count exceeds mutable vector query request frame");
  written_bytes_ += bytes;
  return common::Status::ok();
}

std::size_t DistributedMutableVectorQueryRequestWriteCursor::written_bytes() const noexcept {
  return written_bytes_;
}

bool DistributedMutableVectorQueryRequestWriteCursor::complete() const noexcept {
  return written_bytes_ == encoded_frame_.size();
}

DistributedMutableVectorQueryReceiver::DistributedMutableVectorQueryReceiver(
    DistributedMutableVectorQueryReceiverConfig config) noexcept
    : config_(config) {}

common::Result<DistributedMutableVectorQueryReceiver> DistributedMutableVectorQueryReceiver::create(
    const DistributedMutableVectorQueryReceiverConfig config) {
  constexpr std::size_t kMinimumResponseBytes =
      kDistributedVectorQueryResponseV2HeaderSize + kDistributedVectorQueryResponseV2TrailerSize;
  if (config.local_node_id == 0U || config.authorizer == nullptr || config.worker == nullptr ||
      config.maximum_response_frames == 0U ||
      config.maximum_response_frames > query::kMaximumDistributedCoordinatorMessages ||
      config.maximum_response_bytes < kMinimumResponseBytes ||
      config.maximum_response_bytes > kMaximumDistributedVectorQueryV2ResponseBytes) {
    return common::make_unexpected(
        invalid("mutable vector query receiver configuration is invalid"));
  }
  return DistributedMutableVectorQueryReceiver{config};
}

common::Result<std::vector<std::vector<std::byte>>> DistributedMutableVectorQueryReceiver::receive(
    const common::ByteView request_bytes,
    const network::PeerAuthenticationResult& authenticated_peer) {
  if (!authenticated_peer.authorized || authenticated_peer.principal_id == 0U) {
    return common::make_unexpected(
        unauthenticated("mutable vector query requires an authenticated principal"));
  }
  auto request = decode_distributed_mutable_vector_query_request_exact(request_bytes);
  if (!request.has_value())
    return common::make_unexpected(request.error());
  auto authorized =
      config_.authorizer->authorize_node(authenticated_peer.principal_id, request->source_node_id);
  if (!authorized.has_value())
    return common::make_unexpected(authorized.error());
  if (!*authorized) {
    return common::make_unexpected(
        unauthenticated("authenticated principal cannot claim mutable vector query source node"));
  }
  if (request->target_node_id != config_.local_node_id) {
    return common::make_unexpected(unavailable("mutable vector query targets a different node"));
  }

  const query::DistributedMutableVectorFragment& identity = request->fragment;
  auto result = execute_worker(*config_.worker, request->fragment);
  if (!result.has_value()) {
    std::optional<DistributedQueryLeaderHint> leader_hint;
    if (result.error().code() == common::StatusCode::kUnavailable &&
        config_.leader_hint_provider != nullptr) {
      auto resolved = config_.leader_hint_provider->current_leader_hint(identity.tablet_id,
                                                                        identity.raft_group_id);
      if (!resolved.has_value())
        return common::make_unexpected(resolved.error());
      leader_hint = *resolved;
    }
    auto encoded =
        encode_distributed_vector_query_response_v2({.source_node_id = config_.local_node_id,
                                                     .target_node_id = request->source_node_id,
                                                     .query_id = identity.query_id,
                                                     .tablet_id = identity.tablet_id,
                                                     .status_code = result.error().code(),
                                                     .leader_hint = leader_hint},
                                                    identity.result_schema);
    if (!encoded.has_value())
      return common::make_unexpected(encoded.error());
    try {
      std::vector<std::vector<std::byte>> frames;
      frames.push_back(std::move(*encoded));
      return frames;
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("mutable vector query response allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(exhausted("mutable vector query response exceeds limits"));
    }
  }

  constexpr std::size_t kEncodedSuccessOverhead =
      kDistributedVectorQueryResponseV2HeaderSize +
      distributed_vector_result_exchange_v2_format::kHeaderLength +
      distributed_vector_result_exchange_v2_format::kTrailerLength +
      kDistributedVectorQueryResponseV2TrailerSize;
  if (result->empty())
    return common::make_unexpected(invalid("mutable vector query worker returned an empty stream"));
  bool over_limit = result->size() > config_.maximum_response_frames ||
                    config_.maximum_response_bytes < kEncodedSuccessOverhead;
  std::size_t total_response_bytes{};
  for (std::size_t index = 0U; !over_limit && index < result->size(); ++index) {
    const auto& message = (*result)[index];
    const bool last = index + 1U == result->size();
    if (message.query_id != identity.query_id || message.tablet_id != identity.tablet_id ||
        message.sequence != index + 1U || message.terminal != last) {
      return common::make_unexpected(
          invalid("mutable vector query worker stream is not correlated and terminally closed"));
    }
    if (total_response_bytes > config_.maximum_response_bytes - kEncodedSuccessOverhead ||
        message.encoded_result_batch.size() >
            config_.maximum_response_bytes - kEncodedSuccessOverhead - total_response_bytes) {
      over_limit = true;
      continue;
    }
    total_response_bytes += kEncodedSuccessOverhead + message.encoded_result_batch.size();
  }
  if (over_limit) {
    auto encoded = encode_distributed_vector_query_response_v2(
        {.source_node_id = config_.local_node_id,
         .target_node_id = request->source_node_id,
         .query_id = identity.query_id,
         .tablet_id = identity.tablet_id,
         .status_code = common::StatusCode::kResourceExhausted},
        identity.result_schema);
    if (!encoded.has_value())
      return common::make_unexpected(encoded.error());
    try {
      std::vector<std::vector<std::byte>> frames;
      frames.push_back(std::move(*encoded));
      return frames;
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("mutable vector query response allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(exhausted("mutable vector query response exceeds limits"));
    }
  }

  try {
    std::vector<std::vector<std::byte>> frames;
    frames.reserve(result->size());
    for (auto& message : *result) {
      auto encoded =
          encode_distributed_vector_query_response_v2({.source_node_id = config_.local_node_id,
                                                       .target_node_id = request->source_node_id,
                                                       .query_id = identity.query_id,
                                                       .tablet_id = identity.tablet_id,
                                                       .status_code = common::StatusCode::kOk,
                                                       .payload = std::move(message)},
                                                      identity.result_schema);
      if (!encoded.has_value())
        return common::make_unexpected(encoded.error());
      frames.push_back(std::move(*encoded));
    }
    return frames;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("mutable vector query response allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("mutable vector query response exceeds limits"));
  }
}

DistributedMutableVectorQuerySender::DistributedMutableVectorQuerySender(
    const raft::NodeId source_node_id, query::DistributedMutableVectorFragment fragment,
    const DistributedMutableVectorQuerySenderLimits limits) noexcept
    : source_node_id_(source_node_id), fragment_(std::move(fragment)), limits_(limits),
      next_backoff_(limits.retry.initial_backoff) {}

common::Result<DistributedMutableVectorQuerySender> DistributedMutableVectorQuerySender::create(
    const raft::NodeId source_node_id, query::DistributedMutableVectorFragment fragment,
    const DistributedMutableVectorQuerySenderLimits limits) {
  const auto maximum_supported_backoff =
      std::chrono::duration_cast<std::chrono::milliseconds>(TimePoint::duration::max());
  constexpr std::size_t kMinimumResponseBytes =
      kDistributedVectorQueryResponseV2HeaderSize + kDistributedVectorQueryResponseV2TrailerSize;
  if (source_node_id == 0U || limits.retry.maximum_attempts == 0U ||
      limits.retry.maximum_attempts > 1024U || limits.retry.initial_backoff.count() <= 0 ||
      limits.retry.maximum_backoff < limits.retry.initial_backoff ||
      limits.retry.maximum_backoff > maximum_supported_backoff ||
      limits.maximum_response_frames == 0U ||
      limits.maximum_response_frames > query::kMaximumDistributedCoordinatorMessages ||
      limits.maximum_response_bytes < kMinimumResponseBytes ||
      limits.maximum_response_bytes > kMaximumDistributedVectorQueryV2ResponseBytes ||
      fragment.serving_node == source_node_id) {
    return common::make_unexpected(invalid("mutable vector query sender configuration is invalid"));
  }
  auto encoded = encode_sender_request(source_node_id, fragment);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  return DistributedMutableVectorQuerySender{source_node_id, std::move(fragment), limits};
}

common::Result<DistributedMutableVectorQueryAttempt>
DistributedMutableVectorQuerySender::begin_attempt(const TimePoint now) {
  if (state_ == DistributedQuerySenderState::kSucceeded ||
      state_ == DistributedQuerySenderState::kFailed) {
    return common::make_unexpected(invalid("mutable vector query sender is terminal"));
  }
  if (state_ == DistributedQuerySenderState::kWaitingForResponse)
    return common::make_unexpected(unavailable("mutable vector query response is pending"));
  if (state_ == DistributedQuerySenderState::kBackoff && now < *next_attempt_not_before_)
    return common::make_unexpected(unavailable("mutable vector query retry backoff is active"));
  if (attempts_started_ >= limits_.retry.maximum_attempts)
    return common::make_unexpected(invalid("mutable vector query retry budget is exhausted"));
  auto bytes = encode_sender_request(source_node_id_, fragment_);
  if (!bytes.has_value())
    return common::make_unexpected(bytes.error());
  ++attempts_started_;
  state_ = DistributedQuerySenderState::kWaitingForResponse;
  suggested_leader_.reset();
  next_attempt_not_before_.reset();
  return DistributedMutableVectorQueryAttempt{attempts_started_, fragment_.serving_node,
                                              std::move(*bytes)};
}

common::Status DistributedMutableVectorQuerySender::accept_responses(
    const std::span<const DistributedVectorQueryResponseV2> responses, const TimePoint now) {
  if (state_ != DistributedQuerySenderState::kWaitingForResponse)
    return invalid("mutable vector query sender has no pending response");
  if (responses.empty())
    return invalid("mutable vector query response stream is empty");
  if (responses.size() > limits_.maximum_response_frames)
    return exhausted("mutable vector query response stream exceeds sender frame limit");

  std::size_t total_response_bytes{};
  for (const auto& response : responses) {
    if (response.source_node_id != fragment_.serving_node ||
        response.target_node_id != source_node_id_ || response.query_id != fragment_.query_id ||
        response.tablet_id != fragment_.tablet_id) {
      return invalid("mutable vector query sender response correlation mismatch");
    }
    auto encoded = encode_distributed_vector_query_response_v2(response, fragment_.result_schema);
    if (!encoded.has_value())
      return encoded.error();
    if (encoded->size() > limits_.maximum_response_bytes - total_response_bytes)
      return exhausted("mutable vector query response stream exceeds sender byte limit");
    total_response_bytes += encoded->size();
  }

  if (responses.front().status_code != common::StatusCode::kOk) {
    if (responses.size() != 1U || responses.front().payload.has_value())
      return invalid("mutable vector query failure response stream is invalid");
    suggested_leader_ = responses.front().leader_hint;
    return schedule(responses.front().status_code, now);
  }

  try {
    std::vector<DistributedVectorResultExchangeMessage> accepted;
    accepted.reserve(responses.size());
    for (std::size_t index = 0U; index < responses.size(); ++index) {
      const auto& response = responses[index];
      if (response.status_code != common::StatusCode::kOk || !response.payload.has_value() ||
          response.leader_hint.has_value()) {
        return invalid("mutable vector query success response stream is invalid");
      }
      const auto& message = *response.payload;
      const bool last = index + 1U == responses.size();
      if (message.query_id != fragment_.query_id || message.tablet_id != fragment_.tablet_id ||
          message.sequence != index + 1U || message.terminal != last ||
          (message.encoded_result_batch.empty() && (!last || responses.size() != 1U))) {
        return invalid("mutable vector query success response sequence is invalid");
      }
      accepted.push_back(message);
    }
    result_ = std::move(accepted);
  } catch (const std::bad_alloc&) {
    return exhausted("mutable vector query sender result allocation failed");
  } catch (const std::length_error&) {
    return exhausted("mutable vector query sender result exceeds limits");
  }
  last_status_code_ = common::StatusCode::kOk;
  suggested_leader_.reset();
  state_ = DistributedQuerySenderState::kSucceeded;
  next_attempt_not_before_.reset();
  return common::Status::ok();
}

common::Status
DistributedMutableVectorQuerySender::record_transport_failure(const common::StatusCode code,
                                                              const TimePoint now) {
  if (state_ != DistributedQuerySenderState::kWaitingForResponse)
    return invalid("mutable vector query sender has no active transport attempt");
  if (code == common::StatusCode::kOk)
    return invalid("mutable vector query transport failure cannot be OK");
  suggested_leader_.reset();
  return schedule(code, now);
}

common::Status DistributedMutableVectorQuerySender::schedule(const common::StatusCode code,
                                                             const TimePoint now) {
  last_status_code_ = code;
  if (!retryable_status(code) || attempts_started_ >= limits_.retry.maximum_attempts) {
    state_ = DistributedQuerySenderState::kFailed;
    next_attempt_not_before_.reset();
    return common::Status::ok();
  }
  state_ = DistributedQuerySenderState::kBackoff;
  next_attempt_not_before_ = saturating_add(now, next_backoff_);
  if (next_backoff_ < limits_.retry.maximum_backoff) {
    const auto current = next_backoff_.count();
    const auto maximum = limits_.retry.maximum_backoff.count();
    next_backoff_ = current > maximum / 2
                        ? limits_.retry.maximum_backoff
                        : std::min(next_backoff_ * 2, limits_.retry.maximum_backoff);
  }
  return common::Status::ok();
}

DistributedQuerySenderState DistributedMutableVectorQuerySender::state() const noexcept {
  return state_;
}

std::size_t DistributedMutableVectorQuerySender::attempts_started() const noexcept {
  return attempts_started_;
}

std::optional<DistributedMutableVectorQuerySender::TimePoint>
DistributedMutableVectorQuerySender::next_attempt_not_before() const noexcept {
  return next_attempt_not_before_;
}

std::optional<common::StatusCode>
DistributedMutableVectorQuerySender::last_status_code() const noexcept {
  return last_status_code_;
}

std::optional<DistributedQueryLeaderHint>
DistributedMutableVectorQuerySender::suggested_leader() const noexcept {
  return suggested_leader_;
}

const std::optional<std::vector<DistributedVectorResultExchangeMessage>>&
DistributedMutableVectorQuerySender::result() const noexcept {
  return result_;
}

} // namespace chronos::cluster
