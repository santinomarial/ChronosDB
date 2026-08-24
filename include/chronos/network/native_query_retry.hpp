#ifndef CHRONOS_NETWORK_NATIVE_QUERY_RETRY_HPP_
#define CHRONOS_NETWORK_NATIVE_QUERY_RETRY_HPP_

#include "chronos/common/result.hpp"
#include "chronos/network/client_session.hpp"
#include "chronos/network/native_leader_redirect_router.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace chronos::network {

struct NativeQueryRetryLimits {
  QueryResultLimits query_result;
  std::uint64_t maximum_result_rows{1'048'576U};
  std::size_t maximum_result_batches{1024U};
  std::size_t maximum_result_payload_bytes{std::size_t{64U} * 1024U * 1024U};
};

struct NativeQueryRetryConfig {
  NativeLeaderRedirectRouterConfig routing;
  ConnectionBufferConfig buffers;
  NativeQueryRetryLimits limits;
};

struct NativeQueryResult {
  std::vector<std::vector<std::byte>> encoded_batches;
  std::uint64_t row_count{};
  std::size_t payload_bytes{};
};

enum class NativeQueryRetryState : std::uint8_t {
  kRunning = 1,
  kComplete = 2,
  kFailed = 3,
};

struct NativeQueryRetryProgress {
  bool reconnect_required{};
  bool completed{};
  std::size_t attempt_number{};
  std::size_t result_batches_received{};
};

// Portable, single-threaded Protocol 2 replay owner for one exact finite SQL query. A carrier
// writes pending bytes to current_route(), feeds response bytes to receive(), and replaces its
// connection only when reconnect_required is returned. Complete result batches remain owned and
// unpublished through result() until QUERY_END validates the stream. Borrowed route TLS contexts
// must outlive this owner and every carrier attempt.
class NativeQueryRetry {
public:
  NativeQueryRetry() = delete;
  ~NativeQueryRetry();
  NativeQueryRetry(const NativeQueryRetry&) = delete;
  NativeQueryRetry& operator=(const NativeQueryRetry&) = delete;
  NativeQueryRetry(NativeQueryRetry&&) noexcept;
  NativeQueryRetry& operator=(NativeQueryRetry&&) noexcept;

  [[nodiscard]] static common::Result<NativeQueryRetry> create(NativeQueryRetryConfig config,
                                                               std::string sql);

  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes);
  [[nodiscard]] common::Result<NativeQueryRetryProgress> receive(common::ByteView bytes);

  [[nodiscard]] NativeQueryRetryState state() const noexcept;
  [[nodiscard]] NativeLeaderRoute current_route() const noexcept;
  [[nodiscard]] std::size_t attempts_started() const noexcept;
  [[nodiscard]] const std::optional<NativeQueryResult>& result() const noexcept;
  [[nodiscard]] const common::Status& failure() const;

private:
  class Impl;
  explicit NativeQueryRetry(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::network

#endif // CHRONOS_NETWORK_NATIVE_QUERY_RETRY_HPP_
