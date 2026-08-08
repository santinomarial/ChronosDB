#ifndef CHRONOS_NETWORK_CONNECTION_BUFFERS_HPP_
#define CHRONOS_NETWORK_CONNECTION_BUFFERS_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/network/protocol.hpp"

#include <cstddef>
#include <cstdint>
#include <deque>
#include <vector>

namespace chronos::network {

struct ConnectionBufferConfig {
  ProtocolLimits protocol;
  std::size_t maximum_inbound_buffer_bytes{kFrameHeaderSize + kDefaultMaximumPayloadSize};
  std::size_t maximum_outbound_buffer_bytes{std::size_t{32U} * 1024U * 1024U};
  std::size_t maximum_outbound_frames{128U};
};

class ConnectionBuffers {
public:
  ConnectionBuffers() = delete;
  [[nodiscard]] static common::Result<ConnectionBuffers>
  create(const ConnectionBufferConfig& config = {});

  [[nodiscard]] common::Result<std::vector<Frame>> receive(common::ByteView bytes);
  [[nodiscard]] common::Status enqueue(std::vector<std::byte> encoded_frame);
  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes) noexcept;
  void clear() noexcept;

  [[nodiscard]] std::size_t inbound_buffer_bytes() const noexcept;
  [[nodiscard]] std::size_t outbound_buffer_bytes() const noexcept;
  [[nodiscard]] std::size_t outbound_frames() const noexcept;

private:
  explicit ConnectionBuffers(ConnectionBufferConfig config) noexcept;

  ConnectionBufferConfig config_;
  std::vector<std::byte> inbound_;
  std::deque<std::vector<std::byte>> outbound_;
  std::size_t outbound_offset_{};
  std::size_t outbound_bytes_{};
};

} // namespace chronos::network

#endif // CHRONOS_NETWORK_CONNECTION_BUFFERS_HPP_
