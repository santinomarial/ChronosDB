#ifndef CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_RETRY_HPP_
#define CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_RETRY_HPP_

#include "chronos/cluster/distributed_query_transport.hpp"
#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_result_stream.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <vector>

namespace chronos::cluster {

struct DistributedVectorGroupedAggregateShuffleResultRetryLimits {
  DistributedQueryRetryLimits retry;
  DistributedVectorGroupedAggregateShuffleResultStreamLimits stream;
};

struct DistributedVectorGroupedAggregateShuffleResultRoute {
  std::uint32_t partition_id{};
  raft::NodeId source_node_id{};
  raft::NodeId coordinator_node_id{};
};

enum class DistributedVectorGroupedAggregateShuffleResultRetryState : std::uint8_t {
  kReady = 1,
  kAttemptActive = 2,
  kBackoff = 3,
  kSucceeded = 4,
  kFailed = 5,
};

struct DistributedVectorGroupedAggregateShuffleResultAttempt {
  std::size_t attempt_number{};
  raft::NodeId target_node_id{};
  DistributedVectorGroupedAggregateShuffleResultStreamSender stream;
};

// Retains one immutable reduced partition across finite whole-attempt retries. Authority and raw
// schema are borrowed and outlive this single-thread-affine owner. Success may be reported only
// after the connected carrier validates the exact result receipt.
class DistributedVectorGroupedAggregateShuffleResultRetry {
public:
  using TimePoint = std::chrono::steady_clock::time_point;

  DistributedVectorGroupedAggregateShuffleResultRetry() = delete;
  DistributedVectorGroupedAggregateShuffleResultRetry(
      const DistributedVectorGroupedAggregateShuffleResultRetry&) = delete;
  DistributedVectorGroupedAggregateShuffleResultRetry&
  operator=(const DistributedVectorGroupedAggregateShuffleResultRetry&) = delete;
  DistributedVectorGroupedAggregateShuffleResultRetry(
      DistributedVectorGroupedAggregateShuffleResultRetry&&) noexcept = default;
  DistributedVectorGroupedAggregateShuffleResultRetry&
  operator=(DistributedVectorGroupedAggregateShuffleResultRetry&&) noexcept = default;

  [[nodiscard]] static common::Result<DistributedVectorGroupedAggregateShuffleResultRetry>
  create(const DistributedVectorGroupedAggregateShuffleAuthority& authority,
         const query::DistributedVectorResultSchema& result_schema,
         DistributedVectorGroupedAggregateShuffleResultRoute route,
         std::vector<std::vector<std::byte>> encoded_result_batches,
         DistributedVectorGroupedAggregateShuffleResultRetryLimits limits = {});

  [[nodiscard]] common::Result<DistributedVectorGroupedAggregateShuffleResultAttempt>
  begin_attempt(TimePoint now);
  [[nodiscard]] common::Status record_acknowledged();
  [[nodiscard]] common::Status record_attempt_failure(common::StatusCode code, TimePoint now);

  [[nodiscard]] DistributedVectorGroupedAggregateShuffleResultRetryState state() const noexcept;
  [[nodiscard]] std::size_t attempts_started() const noexcept;
  [[nodiscard]] std::optional<TimePoint> next_attempt_not_before() const noexcept;
  [[nodiscard]] std::optional<common::StatusCode> last_status_code() const noexcept;
  [[nodiscard]] std::uint32_t partition_id() const noexcept;
  [[nodiscard]] raft::NodeId source_node_id() const noexcept;
  [[nodiscard]] raft::NodeId target_node_id() const noexcept;

private:
  DistributedVectorGroupedAggregateShuffleResultRetry(
      const DistributedVectorGroupedAggregateShuffleAuthority& authority,
      const query::DistributedVectorResultSchema& result_schema,
      DistributedVectorGroupedAggregateShuffleResultRoute route,
      std::vector<std::vector<std::byte>> encoded_result_batches,
      DistributedVectorGroupedAggregateShuffleResultRetryLimits limits) noexcept;
  [[nodiscard]] common::Status schedule(common::StatusCode code, TimePoint now);

  std::reference_wrapper<const DistributedVectorGroupedAggregateShuffleAuthority> authority_;
  std::reference_wrapper<const query::DistributedVectorResultSchema> result_schema_;
  std::uint32_t partition_id_{};
  raft::NodeId source_node_id_{};
  raft::NodeId coordinator_node_id_{};
  std::vector<std::vector<std::byte>> encoded_result_batches_;
  DistributedVectorGroupedAggregateShuffleResultRetryLimits limits_;
  DistributedVectorGroupedAggregateShuffleResultRetryState state_{
      DistributedVectorGroupedAggregateShuffleResultRetryState::kReady};
  std::size_t attempts_started_{};
  std::chrono::milliseconds next_backoff_{};
  std::optional<TimePoint> next_attempt_not_before_;
  std::optional<common::StatusCode> last_status_code_;
};

} // namespace chronos::cluster

#endif // CHRONOS_CLUSTER_DISTRIBUTED_VECTOR_GROUPED_AGGREGATE_SHUFFLE_RESULT_RETRY_HPP_
