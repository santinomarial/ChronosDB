#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RETRY_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RETRY_HPP_

#include "chronos/cluster/distributed_query_transport.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_stream.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <vector>

namespace chronos::cluster {

struct DistributedVectorGroupedAggregateShuffleRetryLimits {
  DistributedQueryRetryLimits retry;
  DistributedVectorGroupedAggregateShuffleStreamLimits stream;
};

enum class DistributedVectorGroupedAggregateShuffleRetryState : std::uint8_t {
  kReady = 1,
  kAttemptActive = 2,
  kBackoff = 3,
  kSucceeded = 4,
  kFailed = 5,
};

struct DistributedVectorGroupedAggregateShuffleAttempt {
  std::size_t attempt_number{};
  raft::NodeId target_node_id{};
  DistributedVectorGroupedAggregateShuffleStreamSender stream;
};

// Retains one immutable remote edge and canonical nested stream across finite whole-attempt
// retries. The authority is borrowed and must outlive this single-thread-affine owner. A caller
// reports success only after the attempt's TLS carrier validates the exact acknowledgment.
class DistributedVectorGroupedAggregateShuffleRetry {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedVectorGroupedAggregateShuffleRetry() = delete;
  DistributedVectorGroupedAggregateShuffleRetry(
      const DistributedVectorGroupedAggregateShuffleRetry&) = delete;
  DistributedVectorGroupedAggregateShuffleRetry&
  operator=(const DistributedVectorGroupedAggregateShuffleRetry&) = delete;
  DistributedVectorGroupedAggregateShuffleRetry(
      DistributedVectorGroupedAggregateShuffleRetry&&) noexcept = default;
  DistributedVectorGroupedAggregateShuffleRetry&
  operator=(DistributedVectorGroupedAggregateShuffleRetry&&) noexcept = default;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleRetry>
  create(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
         DistributedVectorGroupedAggregateShuffleEdge edge,
         std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> messages,
         query::QueryResourceContext resources,
         DistributedVectorGroupedAggregateShuffleRetryLimits limits = {});

  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleAttempt>
  begin_attempt(TimePoint now);
  [[nodiscard]] common::Status record_acknowledged();
  [[nodiscard]] common::Status record_attempt_failure(common::StatusCode code, TimePoint now);

  [[nodiscard]] DistributedVectorGroupedAggregateShuffleRetryState state() const noexcept;
  [[nodiscard]] std::size_t attempts_started() const noexcept;
  [[nodiscard]] std::optional<TimePoint> next_attempt_not_before() const noexcept;
  [[nodiscard]] std::optional<common::StatusCode> last_status_code() const noexcept;
  [[nodiscard]] const DistributedVectorGroupedAggregateShuffleEdge& edge() const noexcept;

private:
  DistributedVectorGroupedAggregateShuffleRetry(
      const DistributedVectorGroupedAggregateShuffleAuthority& authority,
      DistributedVectorGroupedAggregateShuffleEdge edge,
      std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> messages,
      query::QueryResourceContext resources,
      DistributedVectorGroupedAggregateShuffleRetryLimits limits) noexcept;
  [[nodiscard]] common::Status schedule(common::StatusCode code, TimePoint now);

  std::reference_wrapper<const DistributedVectorGroupedAggregateShuffleAuthority> authority_;
  DistributedVectorGroupedAggregateShuffleEdge edge_;
  std::vector<query::EncodedDistributedVectorGroupedAggregateExchangeMessage> messages_;
  query::QueryResourceContext resources_;
  DistributedVectorGroupedAggregateShuffleRetryLimits limits_;
  DistributedVectorGroupedAggregateShuffleRetryState state_{
      DistributedVectorGroupedAggregateShuffleRetryState::kReady};
  std::size_t attempts_started_{};
  std::chrono::milliseconds next_backoff_{};
  std::optional<TimePoint> next_attempt_not_before_;
  std::optional<common::StatusCode> last_status_code_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RETRY_HPP_
