#ifndef CHRONOS_NETWORK_NATIVE_QUORUM_INGEST_TCP_EXECUTION_HPP_
#define CHRONOS_NETWORK_NATIVE_QUORUM_INGEST_TCP_EXECUTION_HPP_

#include "chronos/common/result.hpp"
#include "chronos/network/native_quorum_ingest_tcp_client.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::network {

struct NativeQuorumIngestTcpExecutionConfig {
  NativeQuorumIngestTcpClientConfig client;
  std::optional<std::chrono::steady_clock::time_point> operation_deadline;
};

enum class NativeQuorumIngestTcpExecutionState : std::uint8_t {
  kRunning = 1,
  kComplete = 2,
  kFailed = 3,
  kCancelled = 4,
};

struct NativeQuorumIngestTcpExecutionMetrics {
  std::uint64_t poll_calls{};
  std::uint64_t readiness_events{};
  std::size_t attempts_started{};
  std::size_t redirects_followed{};
  bool active_client{};
};

// Single-threaded poll owner for one exact native QUORUM_SYNC operation. It owns the TCP client and
// bounds every kernel wait by the caller's maximum, the active carrier deadline, and an optional
// whole-operation deadline. TLS contexts and authorization policies remain borrowed through the
// client configuration and must outlive this execution.
class NativeQuorumIngestTcpExecution {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  NativeQuorumIngestTcpExecution() = delete;
  ~NativeQuorumIngestTcpExecution();
  NativeQuorumIngestTcpExecution(const NativeQuorumIngestTcpExecution&) = delete;
  NativeQuorumIngestTcpExecution& operator=(const NativeQuorumIngestTcpExecution&) = delete;
  NativeQuorumIngestTcpExecution(NativeQuorumIngestTcpExecution&&) noexcept;
  NativeQuorumIngestTcpExecution& operator=(NativeQuorumIngestTcpExecution&&) noexcept;

  [[nodiscard]] static common::Result<NativeQuorumIngestTcpExecution>
  begin(NativeQuorumIngestTcpExecutionConfig config,
        std::vector<std::byte> encoded_columnar_append);

  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status cancel();

  [[nodiscard]] NativeQuorumIngestTcpExecutionState state() const noexcept;
  [[nodiscard]] NativeQuorumIngestTcpExecutionMetrics metrics() const noexcept;
  [[nodiscard]] std::optional<TimePoint> next_deadline() const noexcept;
  [[nodiscard]] NativeLeaderRoute current_route() const noexcept;
  [[nodiscard]] common::Result<QuorumSyncIngestAcknowledgement> result() const;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit NativeQuorumIngestTcpExecution(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::network

#endif // CHRONOS_NETWORK_NATIVE_QUORUM_INGEST_TCP_EXECUTION_HPP_
