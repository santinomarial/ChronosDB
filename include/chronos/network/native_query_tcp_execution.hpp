#ifndef CHRONOS_NETWORK_NATIVE_QUERY_TCP_EXECUTION_HPP_
#define CHRONOS_NETWORK_NATIVE_QUERY_TCP_EXECUTION_HPP_

#include "chronos/common/result.hpp"
#include "chronos/network/native_query_tcp_client.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

namespace chronos::network {

struct NativeQueryTcpExecutionConfig {
  NativeQueryTcpClientConfig client;
  std::optional<std::chrono::steady_clock::time_point> operation_deadline;
};

enum class NativeQueryTcpExecutionState : std::uint8_t {
  kRunning = 1,
  kComplete = 2,
  kFailed = 3,
  kCancelled = 4,
};

struct NativeQueryTcpExecutionMetrics {
  std::uint64_t poll_calls{};
  std::uint64_t readiness_events{};
  std::size_t attempts_started{};
  std::size_t redirects_followed{};
  bool active_client{};
};

// Single-threaded poll owner for one exact native finite query. It owns the TCP client and bounds
// every kernel wait by the caller maximum, active carrier deadline, and optional whole-operation
// deadline. TLS contexts and authorization policies remain borrowed through client configuration.
class NativeQueryTcpExecution {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  NativeQueryTcpExecution() = delete;
  ~NativeQueryTcpExecution();
  NativeQueryTcpExecution(const NativeQueryTcpExecution&) = delete;
  NativeQueryTcpExecution& operator=(const NativeQueryTcpExecution&) = delete;
  NativeQueryTcpExecution(NativeQueryTcpExecution&&) noexcept;
  NativeQueryTcpExecution& operator=(NativeQueryTcpExecution&&) noexcept;

  [[nodiscard]] static common::Result<NativeQueryTcpExecution>
  begin(NativeQueryTcpExecutionConfig config, std::string sql);

  [[nodiscard]] common::Status poll_once(std::chrono::milliseconds maximum_wait);
  [[nodiscard]] common::Status cancel();

  [[nodiscard]] NativeQueryTcpExecutionState state() const noexcept;
  [[nodiscard]] NativeQueryTcpExecutionMetrics metrics() const noexcept;
  [[nodiscard]] std::optional<TimePoint> next_deadline() const noexcept;
  [[nodiscard]] NativeLeaderRoute current_route() const noexcept;
  [[nodiscard]] const std::optional<NativeQueryResult>& result() const noexcept;
  [[nodiscard]] const common::Status& failure() const noexcept;

private:
  class Impl;
  explicit NativeQueryTcpExecution(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::network

#endif // CHRONOS_NETWORK_NATIVE_QUERY_TCP_EXECUTION_HPP_
