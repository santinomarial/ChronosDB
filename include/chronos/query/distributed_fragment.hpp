#ifndef CHRONOS_QUERY_DISTRIBUTED_FRAGMENT_HPP_
#define CHRONOS_QUERY_DISTRIBUTED_FRAGMENT_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"
#include "chronos/cseg/pruning.hpp"
#include "chronos/manifest/types.hpp"
#include "chronos/query/distributed.hpp"
#include "chronos/raft/types.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/table_schema.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace chronos::query {

namespace distributed_fragment_format {
inline constexpr std::uint16_t kMajor = 1U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kHeaderLength = 216U;
inline constexpr std::size_t kTrailerLength = 4U;
inline constexpr std::uint32_t kMaximumProjectionColumns =
    static_cast<std::uint32_t>(schema::kMaximumSchemaColumnCount);
inline constexpr std::size_t kMaximumFrameLength =
    kHeaderLength + static_cast<std::size_t>(kMaximumProjectionColumns) * sizeof(std::uint32_t) +
    kTrailerLength;
} // namespace distributed_fragment_format

// One executable worker request for the current single-tablet, projected Float64 aggregate path.
// The destination ordinals are unique schema ordinals; aggregate_input_index selects one projected
// column. Schema/type and local snapshot provenance are revalidated by the worker executor.
struct DistributedAggregateFragment {
  common::Uuid query_id;
  manifest::DatabaseId database_id;
  schema::TableId table_id;
  schema::TabletId tablet_id;
  schema::SchemaId destination_schema_id;
  std::uint64_t snapshot_generation{};
  std::uint64_t serving_node{};
  std::uint64_t applied_position{};
  std::uint64_t observed_leader_commit_position{};
  std::uint64_t placement_epoch{};
  DistributedReadPolicy read_policy;
  std::optional<raft::ReadBarrier> linearizable_barrier;
  std::vector<std::uint32_t> destination_column_ordinals;
  std::uint32_t aggregate_input_index{};
  std::optional<cseg::EventTimePredicate> event_time_predicate;

  friend bool operator==(const DistributedAggregateFragment&,
                         const DistributedAggregateFragment&) = default;
};

struct DistributedFragmentDecodeLimits {
  std::uint32_t maximum_projection_columns{distributed_fragment_format::kMaximumProjectionColumns};
};

class EncodedDistributedAggregateFragment {
public:
  [[nodiscard]] common::ByteView bytes() const noexcept;

private:
  explicit EncodedDistributedAggregateFragment(std::vector<std::byte> bytes) noexcept;
  std::vector<std::byte> bytes_;

  friend common::Result<EncodedDistributedAggregateFragment>
  encode_distributed_aggregate_fragment(const DistributedAggregateFragment&);
};

[[nodiscard]] common::Result<EncodedDistributedAggregateFragment>
encode_distributed_aggregate_fragment(const DistributedAggregateFragment& fragment);

[[nodiscard]] common::Result<DistributedAggregateFragment>
decode_distributed_aggregate_fragment_exact(common::ByteView bytes,
                                            DistributedFragmentDecodeLimits limits = {});

namespace distributed_grouped_float64_fragment_format {
inline constexpr std::uint16_t kMajor = 1U;
inline constexpr std::uint16_t kMinor = 0U;
inline constexpr std::size_t kHeaderLength = 40U;
inline constexpr std::size_t kTrailerLength = 4U;
inline constexpr std::size_t kMaximumFrameLength =
    kHeaderLength + distributed_fragment_format::kMaximumFrameLength + kTrailerLength;
} // namespace distributed_grouped_float64_fragment_format

// Distinct grouping intent around one exact aggregate fragment. The key index selects a projected
// column; a later authority binder must prove that destination column has supported FLOAT64 type.
struct DistributedGroupedFloat64Fragment {
  DistributedAggregateFragment aggregate;
  std::uint32_t group_key_input_index{};

  friend bool operator==(const DistributedGroupedFloat64Fragment&,
                         const DistributedGroupedFloat64Fragment&) = default;
};

class EncodedDistributedGroupedFloat64Fragment {
public:
  [[nodiscard]] common::ByteView bytes() const noexcept;

private:
  explicit EncodedDistributedGroupedFloat64Fragment(std::vector<std::byte> bytes) noexcept;
  std::vector<std::byte> bytes_;

  friend common::Result<EncodedDistributedGroupedFloat64Fragment>
  encode_distributed_grouped_float64_fragment(const DistributedGroupedFloat64Fragment&);
};

[[nodiscard]] common::Result<EncodedDistributedGroupedFloat64Fragment>
encode_distributed_grouped_float64_fragment(const DistributedGroupedFloat64Fragment& fragment);

[[nodiscard]] common::Result<DistributedGroupedFloat64Fragment>
decode_distributed_grouped_float64_fragment_exact(common::ByteView bytes,
                                                  DistributedFragmentDecodeLimits limits = {});

} // namespace chronos::query

#endif // CHRONOS_QUERY_DISTRIBUTED_FRAGMENT_HPP_
