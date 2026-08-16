#ifndef CHRONOS_NETWORK_SUBSCRIPTION_MESSAGES_HPP_
#define CHRONOS_NETWORK_SUBSCRIPTION_MESSAGES_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/network/protocol.hpp"
#include "chronos/schema/identity.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::network {

inline constexpr std::size_t kSubscribeRequestEnvelopeSize = 28U;
inline constexpr std::size_t kSubscriptionReadyEnvelopeSize = 8U;
inline constexpr std::size_t kSubscriptionChangeEnvelopeSize = 84U;
inline constexpr std::size_t kSubscriptionAcknowledgeSize = 12U;
inline constexpr std::size_t kSubscriptionCheckpointEnvelopeSize = 20U;
inline constexpr std::size_t kSubscriptionEndEnvelopeSize = 24U;

using SubscriptionSourceId = std::array<std::byte, 16U>;

enum class SubscriptionStartMode : std::uint8_t { kNewQuery = 1, kResume = 2 };
enum class SubscriptionChangeOperation : std::uint8_t { kUpsert = 1, kDelete = 2 };
enum class SubscriptionSourceKind : std::uint8_t { kWal = 1, kRaft = 2 };
enum class SubscriptionEndReason : std::uint8_t {
  kCancelled = 1,
  kSchemaChanged = 2,
  kStateExpired = 3,
  kOverflowed = 4,
  kServerShutdown = 5,
};

struct SubscriptionMessageLimits {
  ProtocolLimits protocol;
  std::uint32_t maximum_resume_token_bytes{1024U * 1024U};
  std::uint32_t maximum_result_key_bytes{1024U * 1024U};
};

// Protocol 1.1 and 2.0 retain the frozen WAL-only change payload. Protocol 1.2 selects the
// source-tagged payload. The subscription feature must have been negotiated in every context.
struct SubscriptionProtocolContext {
  std::uint16_t protocol_major{kProtocolMajor};
  std::uint16_t protocol_minor{1U};
  std::uint64_t feature_bits{kProtocolV1SubscriptionFeature};
};

// Every decoded *View borrows the payload passed to its decoder; that payload must outlive the view
// and all copied ByteViews. Encoders copy borrowed fields into one owned canonical payload.
// NEW_QUERY carries UTF-8 SQL in body. RESUME carries one opaque authenticated Resume Token.
struct SubscriptionRequestView {
  SubscriptionStartMode mode{SubscriptionStartMode::kNewQuery};
  common::Uuid subscription_id;
  common::ByteView body;
};

struct SubscriptionReadyView {
  common::ByteView resume_token;
};

struct SubscriptionChangeView {
  SubscriptionChangeOperation operation{SubscriptionChangeOperation::kUpsert};
  std::uint64_t delivery_sequence{};
  schema::TabletId tablet_id;
  SubscriptionSourceKind source_kind{SubscriptionSourceKind::kWal};
  SubscriptionSourceId source_id{};
  std::uint64_t source_sequence{};
  schema::SchemaId schema_id;
  schema::SchemaVersion schema_version;
  common::ByteView result_key;
  common::ByteView payload;
};

struct SubscriptionAcknowledgement {
  std::uint64_t delivery_sequence{};

  friend bool operator==(const SubscriptionAcknowledgement&,
                         const SubscriptionAcknowledgement&) = default;
};

struct SubscriptionCheckpointView {
  std::uint64_t acknowledged_delivery_sequence{};
  common::ByteView resume_token;
};

struct SubscriptionEndView {
  SubscriptionEndReason reason{SubscriptionEndReason::kCancelled};
  std::uint64_t safe_delivery_sequence{};
  common::ByteView resume_token;
};

[[nodiscard]] common::Status
validate_subscription_message_limits(const SubscriptionMessageLimits& limits);

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_subscription_request(const SubscriptionRequestView& request,
                            const SubscriptionMessageLimits& limits = {});
[[nodiscard]] common::Result<SubscriptionRequestView>
decode_subscription_request(common::ByteView payload, const SubscriptionMessageLimits& limits = {});

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_subscription_ready(common::ByteView resume_token,
                          const SubscriptionMessageLimits& limits = {});
[[nodiscard]] common::Result<SubscriptionReadyView>
decode_subscription_ready(common::ByteView payload, const SubscriptionMessageLimits& limits = {});

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_subscription_change(const SubscriptionChangeView& change,
                           const SubscriptionMessageLimits& limits = {});
[[nodiscard]] common::Result<std::vector<std::byte>>
encode_subscription_change(const SubscriptionChangeView& change,
                           const SubscriptionProtocolContext& context,
                           const SubscriptionMessageLimits& limits = {});
[[nodiscard]] common::Result<SubscriptionChangeView>
decode_subscription_change(common::ByteView payload, const SubscriptionMessageLimits& limits = {});
[[nodiscard]] common::Result<SubscriptionChangeView>
decode_subscription_change(common::ByteView payload, const SubscriptionProtocolContext& context,
                           const SubscriptionMessageLimits& limits = {});

[[nodiscard]] bool
supports_source_tagged_subscription_changes(const SubscriptionProtocolContext& context) noexcept;

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_subscription_acknowledgement(const SubscriptionAcknowledgement& acknowledgement);
[[nodiscard]] common::Result<SubscriptionAcknowledgement>
decode_subscription_acknowledgement(common::ByteView payload);

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_subscription_checkpoint(const SubscriptionCheckpointView& checkpoint,
                               const SubscriptionMessageLimits& limits = {});
[[nodiscard]] common::Result<SubscriptionCheckpointView>
decode_subscription_checkpoint(common::ByteView payload,
                               const SubscriptionMessageLimits& limits = {});

[[nodiscard]] common::Result<std::vector<std::byte>>
encode_subscription_end(const SubscriptionEndView& end,
                        const SubscriptionMessageLimits& limits = {});
[[nodiscard]] common::Result<SubscriptionEndView>
decode_subscription_end(common::ByteView payload, const SubscriptionMessageLimits& limits = {});

} // namespace chronos::network

#endif // CHRONOS_NETWORK_SUBSCRIPTION_MESSAGES_HPP_
