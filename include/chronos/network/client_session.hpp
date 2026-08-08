#ifndef CHRONOS_NETWORK_CLIENT_SESSION_HPP_
#define CHRONOS_NETWORK_CLIENT_SESSION_HPP_

#include "chronos/network/connection_buffers.hpp"
#include "chronos/network/messages.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::network {

enum class ClientSessionPhase : std::uint8_t { kCreated, kAwaitingServerHello, kActive, kClosed };

struct NativeClientConfig {
  ConnectionBufferConfig buffers;
  std::size_t maximum_in_flight_requests{64U};
};

class NativeClientSession {
public:
  NativeClientSession() = delete;
  NativeClientSession(const NativeClientSession&) = delete;
  NativeClientSession& operator=(const NativeClientSession&) = delete;
  NativeClientSession(NativeClientSession&&) noexcept = default;
  NativeClientSession& operator=(NativeClientSession&&) noexcept = default;

  [[nodiscard]] static common::Result<NativeClientSession>
  create(const NativeClientConfig& config = {});

  [[nodiscard]] common::Status queue_handshake();
  [[nodiscard]] common::Result<std::uint64_t> queue_query(std::string_view sql);
  [[nodiscard]] common::Result<std::uint64_t>
  queue_ingest(DurabilityMode durability, common::ByteView encoded_columnar_append);
  [[nodiscard]] common::Status queue_cancel(std::uint64_t request_id);
  [[nodiscard]] common::Status queue_ping();
  [[nodiscard]] common::Result<std::vector<Frame>> receive(common::ByteView bytes);

  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  void close() noexcept;
  [[nodiscard]] ClientSessionPhase phase() const noexcept;
  [[nodiscard]] std::size_t in_flight_requests() const noexcept;
  [[nodiscard]] std::uint32_t negotiated_maximum_payload_size() const noexcept;

private:
  struct ActiveRequest {
    std::uint64_t id{};
    MessageType type{MessageType::kQueryRequest};
    DurabilityMode durability{DurabilityMode::kAsync};
    bool query_result_ended{};
  };

  NativeClientSession(NativeClientConfig config, ConnectionBuffers buffers,
                      std::vector<ActiveRequest> active) noexcept;
  [[nodiscard]] common::Status queue_frame(MessageType type, std::uint64_t request_id,
                                           common::ByteView payload, std::uint32_t flags = 0U);
  [[nodiscard]] common::Status accept_server_frame(const Frame& frame);
  void erase_active(std::size_t offset) noexcept;

  NativeClientConfig config_;
  ConnectionBuffers buffers_;
  std::vector<ActiveRequest> active_;
  ClientSessionPhase phase_{ClientSessionPhase::kCreated};
  std::uint64_t last_request_id_{};
  std::uint32_t negotiated_maximum_payload_size_{};
};

} // namespace chronos::network

#endif // CHRONOS_NETWORK_CLIENT_SESSION_HPP_
