#ifndef CHRONOS_SERVICE_REPLICATED_INGEST_SERVICE_HPP_
#define CHRONOS_SERVICE_REPLICATED_INGEST_SERVICE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/network/spsc_queue.hpp"
#include "chronos/service/replicated_ingest_coordinator.hpp"

#include <chrono>
#include <cstdint>
#include <memory>

namespace chronos::service {

struct ReplicatedIngestServiceConfig {
  ReplicatedIngestCoordinator* coordinator{};
  network::SpscNetworkTaskQueue* requests{};
  network::SpscNetworkTaskQueue* responses{};
  network::ProtocolLimits protocol;
};

struct ReplicatedIngestServicePoll {
  bool response_enqueued{};
};

struct ReplicatedIngestServiceMetrics {
  std::uint64_t consumed_requests{};
  std::uint64_t admitted_requests{};
  std::uint64_t cancelled_requests{};
  std::uint64_t request_errors{};
  std::uint64_t shutdown_rejections{};
  std::uint64_t response_backpressure{};
  bool accepting{};
  bool response_retained{};
};

// Thread-affine queue adapter between one native reactor shard and one replicated-ingest
// coordinator. Each poll retries one retained response, consumes at most one request, and advances
// the coordinator by at most one response. The configured owners and queues must outlive this
// service. The response producer must be joined before either queue is destroyed.
class ReplicatedIngestService {
public:
  ReplicatedIngestService() = delete;
  ~ReplicatedIngestService();
  ReplicatedIngestService(const ReplicatedIngestService&) = delete;
  ReplicatedIngestService& operator=(const ReplicatedIngestService&) = delete;
  ReplicatedIngestService(ReplicatedIngestService&&) noexcept;
  ReplicatedIngestService& operator=(ReplicatedIngestService&&) noexcept;

  [[nodiscard]] static common::Result<ReplicatedIngestService>
  create(ReplicatedIngestServiceConfig config);

  [[nodiscard]] common::Result<ReplicatedIngestServicePoll>
  poll_once(std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now());
  void begin_shutdown() noexcept;
  [[nodiscard]] bool drained() const noexcept;
  [[nodiscard]] bool accepting() const noexcept;
  [[nodiscard]] ReplicatedIngestServiceMetrics metrics() const noexcept;

private:
  class Impl;
  explicit ReplicatedIngestService(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::service

#endif // CHRONOS_SERVICE_REPLICATED_INGEST_SERVICE_HPP_
