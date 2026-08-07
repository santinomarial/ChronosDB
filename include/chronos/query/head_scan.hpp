#ifndef CHRONOS_QUERY_HEAD_SCAN_HPP_
#define CHRONOS_QUERY_HEAD_SCAN_HPP_

#include "chronos/common/result.hpp"
#include "chronos/head/mutable_head.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/schema_lineage.hpp"

#include <cstdint>
#include <memory>
#include <vector>

namespace chronos::query {

struct HeadScanLimits {
  VectorChunkLimits chunk{};
};

// A thread-affine source over one acquire-observed immutable mutable-head boundary. Head storage
// uses race-safe byte-per-row validity/BOOL state and native offsets, so each pull materializes one
// bounded canonical query chunk. Hidden row-version metadata is deliberately not exposed here.
class HeadScanOperator final : public PhysicalOperator {
public:
  ~HeadScanOperator() override;

  [[nodiscard]] static common::Result<std::unique_ptr<PhysicalOperator>>
  create(const QueryResourceContext& resources, head::HeadSnapshot snapshot,
         const schema::SchemaLineage& lineage, schema::SchemaId destination_schema_id,
         const schema::TabletId& target_tablet,
         std::vector<std::uint32_t> destination_column_ordinals, HeadScanLimits limits = {});

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override;

private:
  class State;

  explicit HeadScanOperator(std::unique_ptr<State> state) noexcept;

  std::unique_ptr<State> state_;
  bool ended_{};
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_HEAD_SCAN_HPP_
