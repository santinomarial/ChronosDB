#ifndef CHRONOS_SERVICE_REPLICATED_INGEST_SERVICE_HPP_
#define CHRONOS_SERVICE_REPLICATED_INGEST_SERVICE_HPP_

#include "chronos/common/result.hpp"
#include "chronos/network/spsc_queue.hpp"
#include "chronos/service/replicated_ingest_coordinator.hpp"

#include <chrono>
#include <cstdint>
#include <memory>

namespace chronos::service {

class NativeQueryDispatcher;

struct ReplicatedIngestServiceConfig {
  ReplicatedIngestCoordinator* coordinator{};
  NativeQueryDispatcher* queries{};
  network::SpscNetworkTaskQueue* requests{};
  network::SpscNetworkTaskQueue* responses{};
  network::ProtocolLimits protocol{};
};

struct ReplicatedIngestServicePoll {
  bool response_enqueued{};
};

struct ReplicatedIngestServiceMetrics {
  std::uint64_t consumed_requests{};
  std::uint64_t admitted_requests{};
  std::uint64_t query_requests{};
  std::uint64_t cancelled_requests{};
  std::uint64_t request_errors{};
  std::uint64_t shutdown_rejections{};
  std::uint64_t response_backpressure{};
  bool accepting{};
  bool response_retained{};
  bool query_active{};
};

// Thread-affine queue adapter between one native reactor shard, one replicated-ingest coordinator,
// and an optional query dispatcher. It owns at most one joined query thread so the queue owner can
// consume exact CANCEL tasks while synchronous query work advances. Each poll retries one retained
// response, advances one finite query sequence, consumes at most one request, harvests one query,
// or advances the coordinator by at most one response. The configured owners and queues must
// outlive this service. The response producer must be joined before either queue is destroyed.
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
