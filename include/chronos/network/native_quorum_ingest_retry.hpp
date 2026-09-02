#ifndef CHRONOS_NETWORK_NATIVE_QUORUM_INGEST_RETRY_HPP_
#define CHRONOS_NETWORK_NATIVE_QUORUM_INGEST_RETRY_HPP_

#include "chronos/common/result.hpp"
#include "chronos/network/client_session.hpp"
#include "chronos/network/native_leader_redirect_router.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace chronos::network {

struct NativeQuorumIngestRetryConfig {
  NativeLeaderRedirectRouterConfig routing{};
  ConnectionBufferConfig buffers{};
};

enum class NativeQuorumIngestRetryState : std::uint8_t {
  kRunning = 1,
  kComplete = 2,
  kFailed = 3,
};

struct NativeQuorumIngestRetryProgress {
  bool reconnect_required{};
  std::size_t attempt_number{};
  std::optional<QuorumSyncIngestAcknowledgement> acknowledgement{std::nullopt};
};

// Portable, single-threaded Protocol 2 replay owner for one exact caller-supplied encoded
// QUORUM_SYNC append.
// A carrier writes pending bytes to current_route(), feeds response bytes to receive(), and must
// replace its connection when reconnect_required is returned. TLS contexts are borrowed through
// the immutable route map and must outlive this owner and every carrier attempt.
class NativeQuorumIngestRetry {
public:
  NativeQuorumIngestRetry() = delete;
  ~NativeQuorumIngestRetry();
  NativeQuorumIngestRetry(const NativeQuorumIngestRetry&) = delete;
  NativeQuorumIngestRetry& operator=(const NativeQuorumIngestRetry&) = delete;
  NativeQuorumIngestRetry(NativeQuorumIngestRetry&&) noexcept;
  NativeQuorumIngestRetry& operator=(NativeQuorumIngestRetry&&) noexcept;

  [[nodiscard]] static common::Result<NativeQuorumIngestRetry>
  create(NativeQuorumIngestRetryConfig config, std::vector<std::byte> encoded_columnar_append);

  [[nodiscard]] common::ByteView pending_write() const noexcept;
  [[nodiscard]] common::Status consume_written(std::size_t bytes);
  [[nodiscard]] common::Result<NativeQuorumIngestRetryProgress> receive(common::ByteView bytes);

  [[nodiscard]] NativeQuorumIngestRetryState state() const noexcept;
  [[nodiscard]] NativeLeaderRoute current_route() const noexcept;
  [[nodiscard]] std::size_t attempts_started() const noexcept;
  [[nodiscard]] common::Result<QuorumSyncIngestAcknowledgement> result() const;
  [[nodiscard]] const common::Status& failure() const;

private:
  class Impl;
  explicit NativeQuorumIngestRetry(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::network

#endif // CHRONOS_NETWORK_NATIVE_QUORUM_INGEST_RETRY_HPP_
