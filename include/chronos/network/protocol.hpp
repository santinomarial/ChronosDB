#ifndef CHRONOS_NETWORK_PROTOCOL_HPP_
#define CHRONOS_NETWORK_PROTOCOL_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::network {

inline constexpr std::uint32_t kProtocolMagic = 0x31424443U;
// Protocol 1 remains the default compatibility baseline. Protocol 2 is selected only through the
// v1-framed hello exchange and adds no implicit capabilities by itself.
inline constexpr std::uint16_t kProtocolMajor = 1U;
inline constexpr std::uint16_t kProtocolV2Major = 2U;
inline constexpr std::uint16_t kProtocolLatestMajor = kProtocolV2Major;
// Minor 0 is the frozen baseline emitted by callers that do not explicitly negotiate an
// extension. Minor 1 adds the feature-gated subscription message family.
inline constexpr std::uint16_t kProtocolMinor = 0U;
inline constexpr std::uint16_t kProtocolLatestMinor = 1U;
inline constexpr std::uint16_t kProtocolV2LatestMinor = 0U;
inline constexpr std::uint64_t kProtocolV1SubscriptionFeature = std::uint64_t{1U} << 0U;
inline constexpr std::uint64_t kProtocolV1SupportedFeatureBits = kProtocolV1SubscriptionFeature;
inline constexpr std::size_t kFrameHeaderSize = 40U;
inline constexpr std::uint32_t kDefaultMaximumPayloadSize = 16U * 1024U * 1024U;

enum class MessageType : std::uint16_t {
  kClientHello = 1,
  kServerHello = 2,
  kIngestRequest = 10,
  kIngestAcknowledgement = 11,
  kQuorumSyncIngestAcknowledgement = 12,
  kLeaderRedirect = 13,
  kQueryRequest = 20,
  kQueryResult = 21,
  kQueryEnd = 22,
  kSubscribeRequest = 23,
  kSubscriptionReady = 24,
  kSubscriptionChange = 25,
  kSubscriptionAcknowledge = 26,
  kSubscriptionCheckpoint = 27,
  kSubscriptionEnd = 28,
  kCancel = 30,
  kError = 31,
  kPing = 40,
  kPong = 41,
};

inline constexpr std::uint32_t kFrameFlagEndStream = 1U << 0U;
inline constexpr std::uint32_t kKnownFrameFlags = kFrameFlagEndStream;

struct ProtocolLimits {
  std::uint32_t maximum_payload_size{kDefaultMaximumPayloadSize};
};

struct FrameHeader {
  std::uint16_t protocol_major{kProtocolMajor};
  std::uint16_t protocol_minor{kProtocolMinor};
  MessageType message_type{MessageType::kPing};
  std::uint32_t flags{};
  std::uint64_t request_id{};
  std::uint32_t payload_size{};
  std::uint32_t payload_crc32c{};

  friend bool operator==(const FrameHeader&, const FrameHeader&) = default;
};

struct Frame {
  FrameHeader header;
  std::vector<std::byte> payload;

  friend bool operator==(const Frame&, const Frame&) = default;
};

struct FrameDescriptor {
  std::uint16_t protocol_major{kProtocolMajor};
  std::uint16_t protocol_minor{kProtocolMinor};
  MessageType message_type{MessageType::kPing};
  std::uint32_t flags{};
  std::uint64_t request_id{};
};

[[nodiscard]] common::Status validate_protocol_limits(const ProtocolLimits& limits);
[[nodiscard]] common::Result<std::size_t> encoded_frame_size(std::size_t payload_size,
                                                             const ProtocolLimits& limits = {});
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_frame(const FrameDescriptor& descriptor, common::ByteView payload,
             const ProtocolLimits& limits = {});
[[nodiscard]] common::Result<FrameHeader> decode_frame_header(common::ByteView bytes,
                                                              const ProtocolLimits& limits = {});
[[nodiscard]] common::Result<Frame> decode_frame(common::ByteView bytes,
                                                 const ProtocolLimits& limits = {});

} // namespace chronos::network

#endif // CHRONOS_NETWORK_PROTOCOL_HPP_
