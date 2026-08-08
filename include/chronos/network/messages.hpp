#ifndef CHRONOS_NETWORK_MESSAGES_HPP_
#define CHRONOS_NETWORK_MESSAGES_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/protocol.hpp"

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

namespace chronos::network {

inline constexpr std::uint16_t kMessagePayloadFormat = 1U;
inline constexpr std::uint64_t kProtocolV1FeatureBits = 0U;
inline constexpr std::size_t kHelloPayloadSize = 24U;
inline constexpr std::size_t kIngestEnvelopeSize = 8U;
inline constexpr std::size_t kIngestAcknowledgementSize = 32U;
inline constexpr std::size_t kQueryEnvelopeSize = 8U;
inline constexpr std::size_t kErrorEnvelopeSize = 8U;

enum class DurabilityMode : std::uint8_t { kAsync = 1, kLocalSync = 2 };
enum class IngestOutcome : std::uint8_t { kApplied = 1, kMatchingRetry = 2 };
enum class ProtocolErrorCode : std::uint8_t {
  kMalformedFrame = 1,
  kUnsupportedVersion = 2,
  kInvalidState = 3,
  kDuplicateRequest = 4,
  kUnknownRequest = 5,
  kOverloaded = 6,
  kCancelled = 7,
  kInvalidRequest = 8,
  kExecutionFailure = 9,
  kUnauthorized = 10,
  kInternal = 11,
};

struct ClientHello {
  std::uint16_t minimum_major{kProtocolMajor};
  std::uint16_t maximum_major{kProtocolMajor};
  std::uint16_t maximum_minor{kProtocolMinor};
  std::uint64_t feature_bits{kProtocolV1FeatureBits};
  std::uint32_t maximum_payload_size{kDefaultMaximumPayloadSize};
};

struct ServerHello {
  std::uint16_t selected_major{kProtocolMajor};
  std::uint16_t selected_minor{kProtocolMinor};
  std::uint64_t feature_bits{kProtocolV1FeatureBits};
  std::uint32_t maximum_payload_size{kDefaultMaximumPayloadSize};
};

struct IngestRequestView {
  DurabilityMode durability{DurabilityMode::kAsync};
  common::ByteView encoded_columnar_append;
};

struct IngestAcknowledgement {
  DurabilityMode requested_durability{DurabilityMode::kAsync};
  DurabilityMode effective_durability{DurabilityMode::kAsync};
  IngestOutcome outcome{IngestOutcome::kApplied};
  std::uint64_t record_sequence{};
  std::uint64_t segment_number{};
  std::uint64_t byte_offset{};

  friend bool operator==(const IngestAcknowledgement&, const IngestAcknowledgement&) = default;
};

struct ErrorMessageView {
  ProtocolErrorCode code{ProtocolErrorCode::kInternal};
  common::ByteView message;
};

[[nodiscard]] common::Result<std::vector<std::byte>> encode_client_hello(const ClientHello& hello);
[[nodiscard]] common::Result<ClientHello> decode_client_hello(common::ByteView payload);
[[nodiscard]] common::Result<std::vector<std::byte>> encode_server_hello(const ServerHello& hello);
[[nodiscard]] common::Result<ServerHello> decode_server_hello(common::ByteView payload);
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_ingest_request(DurabilityMode durability, common::ByteView encoded_columnar_append,
                      const ProtocolLimits& limits = {});
[[nodiscard]] common::Result<IngestRequestView>
decode_ingest_request(common::ByteView payload, const ProtocolLimits& limits = {});
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_ingest_acknowledgement(const IngestAcknowledgement& acknowledgement);
[[nodiscard]] common::Result<IngestAcknowledgement>
decode_ingest_acknowledgement(common::ByteView payload);
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_query_request(std::string_view sql, const ProtocolLimits& limits = {});
[[nodiscard]] common::Result<common::ByteView>
decode_query_request(common::ByteView payload, const ProtocolLimits& limits = {});
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_error_message(ProtocolErrorCode code, std::string_view message,
                     const ProtocolLimits& limits = {});
[[nodiscard]] common::Result<ErrorMessageView>
decode_error_message(common::ByteView payload, const ProtocolLimits& limits = {});

} // namespace chronos::network

#endif // CHRONOS_NETWORK_MESSAGES_HPP_
