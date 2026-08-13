#ifndef CHRONOS_QUERY_DISTRIBUTED_VECTOR_FRAGMENT_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_VECTOR_FRAGMENT_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/pruning.hpp"
#include "chronos/manifest/types.hpp"
#include "chronos/query/distributed.hpp"
#include "chronos/query/distributed_vector_plan.hpp"
#include "chronos/raft/types.hpp"
#include "chronos/schema/identity.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace chronos::query {

namespace distributed_vector_fragment_format {
inline constexpr std::uint16_t kMajor = 1U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kHeaderLength = 232U;
inline constexpr std::size_t kTrailerLength = 4U;
inline constexpr std::uint32_t kMaximumProjectionColumns = 4096U;
inline constexpr std::size_t kMaximumFrameLength =
    kHeaderLength + static_cast<std::size_t>(kMaximumProjectionColumns) * 4U +
    distributed_vector_plan_format::kMaximumFrameLength + kTrailerLength;
} // namespace distributed_vector_fragment_format

// One group-scoped, snapshot/route/proof-shaped request. Runtime construction and worker execution
// must still independently bind these owned values to committed metadata and local authority.
struct DistributedVectorFragmentDispatch {
  common::Uuid query_id;
  manifest::DatabaseId database_id;
  schema::TableId table_id;
  schema::TabletId tablet_id;
  schema::SchemaId destination_schema_id;
  common::Uuid raft_group_id;
  std::uint64_t snapshot_generation{};
  std::uint64_t serving_node{};
  std::uint64_t applied_position{};
  std::uint64_t observed_leader_commit_position{};
  std::uint64_t placement_epoch{};
  DistributedReadPolicy read_policy;
  std::optional<raft::ReadBarrier> linearizable_barrier;
  std::vector<std::uint32_t> destination_column_ordinals;
  std::optional<cseg::EventTimePredicate> event_time_predicate;
  DistributedVectorPlanIntent plan;

  friend bool operator==(const DistributedVectorFragmentDispatch&,
                         const DistributedVectorFragmentDispatch&) = default;
};

struct DistributedVectorFragmentDecodeLimits {
  std::uint32_t maximum_projection_columns{
      distributed_vector_fragment_format::kMaximumProjectionColumns};
  DistributedVectorPlanDecodeLimits plan;
};

class EncodedDistributedVectorFragmentDispatch {
public:
  EncodedDistributedVectorFragmentDispatch() = delete;
  EncodedDistributedVectorFragmentDispatch(const EncodedDistributedVectorFragmentDispatch&) =
      delete;
  EncodedDistributedVectorFragmentDispatch&
  operator=(const EncodedDistributedVectorFragmentDispatch&) = delete;
  EncodedDistributedVectorFragmentDispatch(EncodedDistributedVectorFragmentDispatch&&) noexcept =
      default;
  EncodedDistributedVectorFragmentDispatch&
  operator=(EncodedDistributedVectorFragmentDispatch&&) noexcept = default;

  [[nodiscard]] common::ByteView bytes() const noexcept;

private:
  explicit EncodedDistributedVectorFragmentDispatch(std::vector<std::byte> bytes) noexcept;
  std::vector<std::byte> bytes_;

  friend common::Result<EncodedDistributedVectorFragmentDispatch>
  encode_distributed_vector_fragment_dispatch(const DistributedVectorFragmentDispatch&);
};

[[nodiscard]] common::Result<EncodedDistributedVectorFragmentDispatch>
encode_distributed_vector_fragment_dispatch(const DistributedVectorFragmentDispatch& dispatch);

[[nodiscard]] common::Result<DistributedVectorFragmentDispatch>
decode_distributed_vector_fragment_dispatch_exact(
    common::ByteView bytes, DistributedVectorFragmentDecodeLimits limits = {});

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_VECTOR_FRAGMENT_HPP_
