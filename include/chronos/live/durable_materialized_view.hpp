#ifndef CHRONOS_LIVE_DURABLE_MATERIALIZED_VIEW_HPP_
#define CHRONOS_LIVE_DURABLE_MATERIALIZED_VIEW_HPP_

#include "chronos/common/result.hpp"
#include "chronos/live/materialized_view.hpp"
#include "chronos/live/materialized_view_checkpoint_storage.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace chronos::live {

struct DurableWindowedMaterializedViewConfig {
  MaterializedViewCheckpointStorageConfig storage;
  schema::TabletId tablet_id;
  wal::WalId wal_id;
  WindowDefinition definition;
};

// Single-thread-affine composition of incremental window state and its immutable checkpoint owner.
// apply_committed() remains committed-only and consecutive. checkpoint() advances the durable
// source retention frontier only after complete file and directory synchronization.
class DurableWindowedMaterializedView {
public:
  DurableWindowedMaterializedView() = delete;
  ~DurableWindowedMaterializedView();
  DurableWindowedMaterializedView(const DurableWindowedMaterializedView&) = delete;
  DurableWindowedMaterializedView& operator=(const DurableWindowedMaterializedView&) = delete;
  DurableWindowedMaterializedView(DurableWindowedMaterializedView&&) noexcept;
  DurableWindowedMaterializedView& operator=(DurableWindowedMaterializedView&&) noexcept;

  [[nodiscard]] static common::Result<DurableWindowedMaterializedView>
  create_new(DurableWindowedMaterializedViewConfig config);
  [[nodiscard]] static common::Result<DurableWindowedMaterializedView>
  open_existing(DurableWindowedMaterializedViewConfig config);

  [[nodiscard]] common::Result<std::vector<MaterializedViewChange>>
  apply_committed(SourcePosition position, MaterializedViewInput input);
  [[nodiscard]] common::Result<std::vector<MaterializedViewChange>>
  advance_watermark(std::int64_t watermark);
  [[nodiscard]] common::Result<InstalledMaterializedViewCheckpoint> checkpoint();

  [[nodiscard]] SourcePosition applied_position() const;
  [[nodiscard]] std::uint64_t durable_record_sequence() const noexcept;
  [[nodiscard]] std::uint64_t checkpoint_generation() const noexcept;
  [[nodiscard]] std::int64_t watermark() const noexcept;
  [[nodiscard]] std::size_t retained_rows() const noexcept;
  [[nodiscard]] std::size_t open_windows() const noexcept;

private:
  class Impl;
  explicit DurableWindowedMaterializedView(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::live

#endif // CHRONOS_LIVE_DURABLE_MATERIALIZED_VIEW_HPP_
