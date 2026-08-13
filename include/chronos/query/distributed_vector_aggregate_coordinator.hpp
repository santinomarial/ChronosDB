#ifndef CHRONOS_QUERY_DISTRIBUTED_VECTOR_AGGREGATE_COORDINATOR_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_VECTOR_AGGREGATE_COORDINATOR_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/query/distributed.hpp"
#include "chronos/query/distributed_vector_aggregate_exchange.hpp"
#include "chronos/query/distributed_vector_result_schema.hpp"
#include "chronos/query/value.hpp"
#include "chronos/schema/identity.hpp"

#include <cstddef>
#include <memory>
#include <vector>

namespace chronos::query {

inline constexpr std::size_t kDefaultDistributedVectorAggregateCoordinatorBytesV2 =
    std::size_t{64U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumDistributedVectorAggregateCoordinatorBytesV2 =
    std::size_t{1024U} * 1024U * 1024U;
inline constexpr std::size_t kDefaultDistributedVectorAggregateCoordinatorMemoryBytesV2 =
    std::size_t{64U} * 1024U * 1024U;
inline constexpr std::size_t kMaximumDistributedVectorAggregateCoordinatorMemoryBytesV2 =
    std::size_t{1024U} * 1024U * 1024U;

struct DistributedVectorAggregateCoordinatorLimitsV2 {
  DistributedCoordinatorLimits messages{
      .maximum_messages_per_fragment = kMaximumUngroupedAggregateWidth,
      .maximum_total_messages = kMaximumDistributedCoordinatorMessages};
  std::size_t maximum_total_encoded_bytes{kDefaultDistributedVectorAggregateCoordinatorBytesV2};
  std::size_t maximum_query_memory_bytes{
      kDefaultDistributedVectorAggregateCoordinatorMemoryBytesV2};
  std::size_t maximum_retained_configuration_bytes{
      kDefaultUngroupedAggregateConfigurationByteLimit};
  DistributedVectorAggregateExchangeDecodeLimits decode;
};

struct DistributedVectorAggregateQueryResultV2 {
  // The exact definition vector remains attached through scalar finalization so later public
  // boundaries do not reconstruct input-type authority from output descriptors.
  std::vector<VectorAggregateDefinition> definitions;
  DistributedVectorResultSchema result_schema;
  std::vector<ScalarValue> values;
  std::size_t retained_encoded_bytes{};
};

// Single-threaded owner for one ungrouped aggregate query. Every accepted in-memory message is
// canonically encoded before retention, making exact bytes the retry identity. Successful finish
// merges complete tablet vectors in plan-tablet order and finalizes each sufficient state once.
class DistributedVectorAggregateCoordinatorV2 {
public:
  DistributedVectorAggregateCoordinatorV2() = delete;
  ~DistributedVectorAggregateCoordinatorV2();
  DistributedVectorAggregateCoordinatorV2(const DistributedVectorAggregateCoordinatorV2&) = delete;
  DistributedVectorAggregateCoordinatorV2&
  operator=(const DistributedVectorAggregateCoordinatorV2&) = delete;
  DistributedVectorAggregateCoordinatorV2(DistributedVectorAggregateCoordinatorV2&&) noexcept;
  DistributedVectorAggregateCoordinatorV2&
  operator=(DistributedVectorAggregateCoordinatorV2&&) noexcept;

  [[nodiscard]] static common::Result<DistributedVectorAggregateCoordinatorV2>
  create(common::Uuid query_id, std::vector<schema::TabletId> tablets,
         std::vector<VectorAggregateDefinition> definitions,
         DistributedVectorResultSchema result_schema,
         DistributedVectorAggregateCoordinatorLimitsV2 limits = {});
  [[nodiscard]] common::Status accept(const DistributedVectorAggregateExchangeMessage& message);
  [[nodiscard]] common::Status worker_failed(const schema::TabletId& tablet_id,
                                             common::Status failure);
  [[nodiscard]] common::Result<DistributedVectorAggregateQueryResultV2> finish() &&;

private:
  class Impl;
  explicit DistributedVectorAggregateCoordinatorV2(std::unique_ptr<Impl> implementation) noexcept;
  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_VECTOR_AGGREGATE_COORDINATOR_HPP_
