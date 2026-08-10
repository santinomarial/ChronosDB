#ifndef CHRONOS_QUERY_SCALAR_SNAPSHOT_SCAN_HPP_
#define CHRONOS_QUERY_SCALAR_SNAPSHOT_SCAN_HPP_

#include "chronos/common/result.hpp"
#include "chronos/query/physical_operator.hpp"
#include "chronos/query/snapshot.hpp"

#include <cstdint>
#include <memory>

namespace chronos::query {

struct ScalarSnapshotScanLimits {
  std::uint32_t maximum_rows_per_chunk{kDefaultVectorChunkRowLimit};
  VectorChunkLimits chunk;
};

// Copies one immutable scalar snapshot into canonical, query-accounted vector chunks. The operator
// owns the snapshot for its complete pull lifetime; emitted chunks own their physical buffers and
// remain valid after the operator is destroyed. Rows and schema columns retain their input order.
class ScalarSnapshotScanOperator final : public PhysicalOperator {
public:
  [[nodiscard]] static common::Result<std::unique_ptr<PhysicalOperator>>
  create(std::shared_ptr<const ScalarTableSnapshot> snapshot, ScalarSnapshotScanLimits limits = {});

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override;

private:
  ScalarSnapshotScanOperator(std::shared_ptr<const ScalarTableSnapshot> snapshot,
                             ScalarSnapshotScanLimits limits) noexcept;

  std::shared_ptr<const ScalarTableSnapshot> snapshot_;
  ScalarSnapshotScanLimits limits_;
  std::size_t next_row_{};
  bool ended_{};
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_SCALAR_SNAPSHOT_SCAN_HPP_
