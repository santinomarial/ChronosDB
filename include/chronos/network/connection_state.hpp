#ifndef CHRONOS_NETWORK_CONNECTION_STATE_HPP_
#define CHRONOS_NETWORK_CONNECTION_STATE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/network/protocol.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

namespace chronos::network {

enum class ConnectionPhase : std::uint8_t { kAwaitingHello, kActive, kClosed };
enum class InboundActionKind : std::uint8_t { kHandshake, kIngest, kQuery, kCancel, kPing };

struct ConnectionStateConfig {
  ProtocolLimits limits;
  std::size_t maximum_in_flight_requests{64U};
};

struct InboundAction {
  InboundActionKind kind{InboundActionKind::kPing};
  std::uint64_t request_id{};
  std::uint32_t negotiated_maximum_payload_size{};
  bool cancellation_was_active{};
};

class ServerConnectionState {
public:
  ServerConnectionState() = delete;

  [[nodiscard]] static common::Result<ServerConnectionState>
  create(const ConnectionStateConfig& config = {});

  [[nodiscard]] common::Result<InboundAction> accept(const Frame& frame);
  [[nodiscard]] bool complete(std::uint64_t request_id) noexcept;
  void close() noexcept;

  [[nodiscard]] ConnectionPhase phase() const noexcept;
  [[nodiscard]] std::size_t in_flight_requests() const noexcept;
  [[nodiscard]] std::uint64_t last_request_id() const noexcept;
  [[nodiscard]] std::uint32_t negotiated_maximum_payload_size() const noexcept;
  [[nodiscard]] std::span<const std::uint64_t> active_request_ids() const noexcept;
  [[nodiscard]] std::optional<MessageType>
  active_request_type(std::uint64_t request_id) const noexcept;

private:
  explicit ServerConnectionState(ConnectionStateConfig config,
                                 std::vector<std::uint64_t> active_requests,
                                 std::vector<MessageType> active_request_types) noexcept;

  ConnectionStateConfig config_;
  std::vector<std::uint64_t> active_requests_;
  std::vector<MessageType> active_request_types_;
  ConnectionPhase phase_{ConnectionPhase::kAwaitingHello};
  std::uint64_t last_request_id_{};
  std::uint32_t negotiated_maximum_payload_size_{};
};

} // namespace chronos::network

#endif // CHRONOS_NETWORK_CONNECTION_STATE_HPP_
