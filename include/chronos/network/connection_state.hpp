#ifndef CHRONOS_NETWORK_CONNECTION_STATE_HPP_
#define CHRONOS_NETWORK_CONNECTION_STATE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/network/protocol.hpp"
#include "chronos/network/subscription_messages.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

namespace chronos::network {

enum class ConnectionPhase : std::uint8_t { kAwaitingHello, kActive, kClosed };
enum class InboundActionKind : std::uint8_t {
  kHandshake,
  kIngest,
  kQuery,
  kSubscribe,
  kSubscriptionAcknowledge,
  kCancel,
  kPing,
};

struct ConnectionStateConfig {
  ProtocolLimits limits;
  std::size_t maximum_in_flight_requests{64U};
  std::uint16_t maximum_protocol_major{kProtocolMajor};
  std::uint64_t supported_feature_bits{kProtocolV1SupportedFeatureBits};
};

struct InboundAction {
  InboundActionKind kind{InboundActionKind::kPing};
  std::uint64_t request_id{};
  std::uint32_t negotiated_maximum_payload_size{};
  std::uint16_t negotiated_major{kProtocolMajor};
  std::uint16_t negotiated_minor{};
  std::uint64_t negotiated_feature_bits{};
  std::uint64_t acknowledged_delivery_sequence{};
  bool cancellation_was_active{};
};

class ServerConnectionState {
public:
  ServerConnectionState() = delete;

  [[nodiscard]] static common::Result<ServerConnectionState>
  create(const ConnectionStateConfig& config = {});

  [[nodiscard]] common::Result<InboundAction> accept(const Frame& frame);
  [[nodiscard]] common::Status accept_response(const Frame& frame);
  [[nodiscard]] bool complete(std::uint64_t request_id) noexcept;
  void close() noexcept;

  [[nodiscard]] ConnectionPhase phase() const noexcept;
  [[nodiscard]] std::size_t in_flight_requests() const noexcept;
  [[nodiscard]] std::uint64_t last_request_id() const noexcept;
  [[nodiscard]] std::uint32_t negotiated_maximum_payload_size() const noexcept;
  [[nodiscard]] std::uint16_t negotiated_major() const noexcept;
  [[nodiscard]] std::uint16_t negotiated_minor() const noexcept;
  [[nodiscard]] std::uint64_t negotiated_feature_bits() const noexcept;
  [[nodiscard]] std::span<const std::uint64_t> active_request_ids() const noexcept;
  [[nodiscard]] std::optional<MessageType>
  active_request_type(std::uint64_t request_id) const noexcept;

private:
  explicit ServerConnectionState(ConnectionStateConfig config,
                                 std::vector<std::uint64_t> active_requests,
                                 std::vector<MessageType> active_request_types,
                                 std::vector<DurabilityMode> active_request_durabilities,
                                 std::vector<bool> query_result_started,
                                 std::vector<bool> query_result_ended,
                                 std::vector<bool> subscription_ready,
                                 std::vector<bool> cancellation_requested,
                                 std::vector<std::uint64_t> subscription_last_delivery,
                                 std::vector<std::uint64_t> subscription_last_acknowledged,
                                 std::vector<std::uint64_t> subscription_last_checkpoint) noexcept;
  void erase_active(std::size_t offset) noexcept;

  ConnectionStateConfig config_;
  std::vector<std::uint64_t> active_requests_;
  std::vector<MessageType> active_request_types_;
  std::vector<DurabilityMode> active_request_durabilities_;
  std::vector<bool> query_result_started_;
  std::vector<bool> query_result_ended_;
  std::vector<bool> subscription_ready_;
  std::vector<bool> cancellation_requested_;
  std::vector<std::uint64_t> subscription_last_delivery_;
  std::vector<std::uint64_t> subscription_last_acknowledged_;
  std::vector<std::uint64_t> subscription_last_checkpoint_;
  ConnectionPhase phase_{ConnectionPhase::kAwaitingHello};
  std::uint64_t last_request_id_{};
  std::uint32_t negotiated_maximum_payload_size_{};
  std::uint16_t negotiated_major_{kProtocolMajor};
  std::uint16_t negotiated_minor_{};
  std::uint64_t negotiated_feature_bits_{};
};

} // namespace chronos::network

#endif // CHRONOS_NETWORK_CONNECTION_STATE_HPP_
