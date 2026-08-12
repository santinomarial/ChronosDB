#ifndef CHRONOS_SERVICE_REPLICATED_INGEST_COORDINATOR_HPP_
#define CHRONOS_SERVICE_REPLICATED_INGEST_COORDINATOR_HPP_

#include "chronos/common/result.hpp"
#include "chronos/ingest/async_raft_tablet_application.hpp"
#include "chronos/network/spsc_queue.hpp"
#include "chronos/raft/async_durable_runtime.hpp"
#include "chronos/raft/async_metadata_application.hpp"

#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>

namespace chronos::service {

struct ReplicatedIngestCoordinatorLimits {
  std::size_t maximum_pending_requests{1024U};
  std::chrono::milliseconds request_timeout{30'000};
  network::ProtocolLimits protocol;
  ingest::ColumnarAppendDecodeLimits columnar_append;
};

struct ReplicatedIngestCoordinatorMetrics {
  std::size_t pending_requests{};
  std::size_t high_water_pending_requests{};
  std::uint64_t admitted_requests{};
  std::uint64_t completed_requests{};
  std::uint64_t cancelled_requests{};
  std::uint64_t timed_out_requests{};
  std::uint64_t rejected_requests{};
};

// Thread-affine bounded owner for multiple reactor-routed QUORUM_SYNC requests. poll() performs no
// blocking wait and returns at most one owning response for caller-managed queue backpressure. The
// borrowed runtime and application owners must outlive this coordinator and all admitted
// operations. Admission derives the tablet group from committed metadata; poll() obtains an
// ordered current-role observation before submitting under that exact leader term.
class ReplicatedIngestCoordinator {
public:
  ReplicatedIngestCoordinator() = delete;
  ~ReplicatedIngestCoordinator();
  ReplicatedIngestCoordinator(const ReplicatedIngestCoordinator&) = delete;
  ReplicatedIngestCoordinator& operator=(const ReplicatedIngestCoordinator&) = delete;
  ReplicatedIngestCoordinator(ReplicatedIngestCoordinator&&) noexcept;
  ReplicatedIngestCoordinator& operator=(ReplicatedIngestCoordinator&&) noexcept;

  [[nodiscard]] static common::Result<ReplicatedIngestCoordinator> create(
      raft::AsyncDurableMultiRaftRuntime& runtime, ingest::AsyncRaftTabletApplication& application,
      raft::AsyncRaftMetadataApplication& metadata, ReplicatedIngestCoordinatorLimits limits = {});

  [[nodiscard]] common::Status
  admit(network::NetworkTask request,
        std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now());
  [[nodiscard]] bool cancel(std::uint64_t connection_id, std::uint64_t request_id) noexcept;
  [[nodiscard]] common::Result<std::optional<network::NetworkTask>>
  poll(std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now());
  [[nodiscard]] ReplicatedIngestCoordinatorMetrics metrics() const noexcept;

private:
  class Impl;
  explicit ReplicatedIngestCoordinator(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_REPLICATED_INGEST_COORDINATOR_HPP_
