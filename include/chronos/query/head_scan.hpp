#ifndef CHRONOS_QUERY_HEAD_SCAN_HPP_
#define CHRONOS_QUERY_HEAD_SCAN_HPP_

#include "chronos/common/result.hpp"
#include "chronos/head/mutable_head.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/query/row_version.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/schema_lineage.hpp"

#include <cstdint>
#include <memory>
#include <vector>

namespace chronos::query {

struct HeadScanLimits {
  VectorChunkLimits chunk{};
  RowVersionScanMode row_version_columns{RowVersionScanMode::kOmit};
};

// A thread-affine source over one acquire-observed immutable mutable-head boundary. Head storage
// uses race-safe byte-per-row validity/BOOL state and native offsets, so each pull materializes one
// bounded canonical query chunk. Callers may opt into the shared fixed row-version suffix.
class HeadScanOperator final : public PhysicalOperator {
public:
  ~HeadScanOperator() override;

  [[nodiscard]] static common::Result<std::unique_ptr<PhysicalOperator>>
  create(const QueryResourceContext& resources, head::HeadSnapshot snapshot,
         const schema::SchemaLineage& lineage, schema::SchemaId destination_schema_id,
         const schema::TabletId& target_tablet,
         std::vector<std::uint32_t> destination_column_ordinals, HeadScanLimits limits = {});

  // Materializes the event-time column if necessary, applies exact open/closed row truth, and
  // removes an unrequested final helper before returning caller-visible chunks. Unlike CSEG range
  // scans, mutable heads have no zone-map pruning stage.
  [[nodiscard]] static common::Result<std::unique_ptr<PhysicalOperator>> create_event_time_filtered(
      const QueryResourceContext& resources, head::HeadSnapshot snapshot,
      const schema::SchemaLineage& lineage, schema::SchemaId destination_schema_id,
      const schema::TabletId& target_tablet, std::vector<std::uint32_t> destination_column_ordinals,
      TimestampRangePredicate predicate, HeadScanLimits limits = {});

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
