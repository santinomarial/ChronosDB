#ifndef CHRONOS_LIVE_MATERIALIZED_VIEW_HPP_
#define CHRONOS_LIVE_MATERIALIZED_VIEW_HPP_

#include "chronos/common/result.hpp"
#include "chronos/live/incremental_aggregate.hpp"
#include "chronos/live/resume_token.hpp"
#include "chronos/live/subscription.hpp"

#include <compare>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace chronos::live {

struct WindowDefinition {
  std::int64_t width{};
  std::int64_t slide{};
  std::int64_t allowed_lateness{};
  std::size_t maximum_windows{4096U};
  std::size_t maximum_rows{65'536U};

  friend bool operator==(const WindowDefinition&, const WindowDefinition&) = default;
};

struct WindowKey {
  std::int64_t start{};
  std::int64_t end{};

  friend auto operator<=>(const WindowKey&, const WindowKey&) = default;
};

enum class WindowResultStatus : std::uint8_t {
  kProvisional = 1,
  kFinalized = 2,
  kCorrected = 3,
};

struct MaterializedViewInput {
  AggregateInput aggregate;
  bool tombstone{};
};

struct MaterializedViewChange {
  LogicalChangeOperation operation{LogicalChangeOperation::kUpsert};
  WindowKey window;
  std::uint64_t revision{};
  WindowResultStatus status{WindowResultStatus::kProvisional};
  AggregateSnapshot value;

  friend bool operator==(const MaterializedViewChange&, const MaterializedViewChange&) = default;
};

struct MaterializedWindowCheckpoint {
  WindowKey window;
  std::uint64_t revision{};
  bool emitted{};
  bool finalized{};
  IncrementalAggregateCheckpoint aggregate;

  friend bool operator==(const MaterializedWindowCheckpoint&,
                         const MaterializedWindowCheckpoint&) = default;
};

struct WindowedMaterializedViewCheckpoint {
  WindowDefinition definition;
  SourcePosition position;
  std::int64_t watermark{};
  std::vector<AggregateInput> rows;
  std::vector<MaterializedWindowCheckpoint> windows;

  friend bool operator==(const WindowedMaterializedViewCheckpoint&,
                         const WindowedMaterializedViewCheckpoint&) = default;
};

// Deterministic single-owner window state driven only by consecutive committed source positions.
// width == slide is tumbling; width > slide is sliding. Corrections use the same row identity and
// replace the prior visible contribution, including movement between windows.
class WindowedMaterializedView {
public:
  WindowedMaterializedView() = delete;
  ~WindowedMaterializedView();
  WindowedMaterializedView(const WindowedMaterializedView&) = delete;
  WindowedMaterializedView& operator=(const WindowedMaterializedView&) = delete;
  WindowedMaterializedView(WindowedMaterializedView&&) noexcept;
  WindowedMaterializedView& operator=(WindowedMaterializedView&&) noexcept;

  [[nodiscard]] static common::Result<WindowedMaterializedView>
  create(schema::TabletId tablet_id, wal::WalId wal_id, WindowDefinition definition);
  // Borrows the canonical checkpoint while reconstructing independently owned view state.
  [[nodiscard]] static common::Result<WindowedMaterializedView>
  restore(const WindowedMaterializedViewCheckpoint& checkpoint);

  [[nodiscard]] common::Result<std::vector<MaterializedViewChange>>
  apply_committed(SourcePosition position, MaterializedViewInput input);

  [[nodiscard]] common::Result<std::vector<MaterializedViewChange>>
  advance_watermark(std::int64_t watermark);

  [[nodiscard]] SourcePosition applied_position() const;
  [[nodiscard]] std::int64_t watermark() const noexcept;
  [[nodiscard]] std::size_t retained_rows() const noexcept;
  [[nodiscard]] std::size_t open_windows() const noexcept;
  [[nodiscard]] common::Result<WindowedMaterializedViewCheckpoint> checkpoint() const;

private:
  class Impl;
  explicit WindowedMaterializedView(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

} // namespace chronos::live

#endif // CHRONOS_LIVE_MATERIALIZED_VIEW_HPP_
