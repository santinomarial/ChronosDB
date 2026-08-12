#ifndef CHRONOS_QUERY_COLUMNAR_BATCH_SCAN_HPP_
#define CHRONOS_QUERY_COLUMNAR_BATCH_SCAN_HPP_

#include "chronos/columnar/columnar_batch.hpp"
#include "chronos/common/result.hpp"
#include "chronos/query/physical_operator.hpp"

#include <cstdint>
#include <memory>

namespace chronos::query {

struct ColumnarBatchScanLimits {
  std::uint32_t maximum_rows_per_chunk{kDefaultVectorChunkRowLimit};
  VectorChunkLimits chunk{};
};

// Copies one immutable schema-shaped ingest batch into bounded query-accounted physical chunks.
// The source owns the batch until successful end; emitted chunks own their canonical buffers and
// remain valid independently. This source exposes schema columns only and never fabricates row-
// version identity, so callers may instantiate only a physical plan with that exact input shape.
class ColumnarBatchScanOperator final : public PhysicalOperator {
public:
  [[nodiscard]] static common::Result<std::unique_ptr<PhysicalOperator>>
  create(std::shared_ptr<const columnar::OwnedColumnarBatch> batch,
         ColumnarBatchScanLimits limits = {});

  [[nodiscard]] common::Result<PhysicalOperatorStep>
  next(const QueryResourceContext& resources) override;

private:
  ColumnarBatchScanOperator(std::shared_ptr<const columnar::OwnedColumnarBatch> batch,
                            ColumnarBatchScanLimits limits) noexcept;

  std::shared_ptr<const columnar::OwnedColumnarBatch> batch_;
  QuerySharedMemoryReservation source_reservation_;
  ColumnarBatchScanLimits limits_;
  std::uint32_t next_row_{};
  bool ended_{};
};

} // namespace chronos::query

#endif // CHRONOS_QUERY_COLUMNAR_BATCH_SCAN_HPP_
