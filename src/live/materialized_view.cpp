#include "chronos/live/materialized_view.hpp"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <utility>
#include <vector>

namespace chronos::live {
namespace {

[[nodiscard]] std::optional<std::int64_t> add_nonnegative(const std::int64_t value,
                                                          const std::int64_t amount) noexcept {
  if (amount < 0 || value > std::numeric_limits<std::int64_t>::max() - amount) {
    return std::nullopt;
  }
  return value + amount;
}

[[nodiscard]] std::optional<std::int64_t> subtract_nonnegative(const std::int64_t value,
                                                               const std::int64_t amount) noexcept {
  if (amount < 0 || value < std::numeric_limits<std::int64_t>::min() + amount) {
    return std::nullopt;
  }
  return value - amount;
}

[[nodiscard]] std::optional<std::int64_t> multiply_nonnegative(const std::int64_t left,
                                                               const std::int64_t right) noexcept {
  if (left < 0 || right < 0 ||
      (left != 0 && right > std::numeric_limits<std::int64_t>::max() / left)) {
    return std::nullopt;
  }
  return left * right;
}

[[nodiscard]] common::Result<std::vector<WindowKey>>
windows_for(const std::int64_t event_time, const WindowDefinition& definition) {
  std::int64_t last_start = (event_time / definition.slide) * definition.slide;
  if (event_time < 0 && event_time % definition.slide != 0) {
    const auto adjusted = subtract_nonnegative(last_start, definition.slide);
    if (!adjusted.has_value()) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kOutOfRange, "event time cannot be aligned safely"});
    }
    last_start = *adjusted;
  }
  const std::int64_t window_count = definition.width / definition.slide;
  std::vector<WindowKey> windows;
  windows.reserve(static_cast<std::size_t>(window_count));
  for (std::int64_t index = 0; index < window_count; ++index) {
    const auto offset = multiply_nonnegative(index, definition.slide);
    const auto start =
        offset.has_value() ? subtract_nonnegative(last_start, *offset) : std::nullopt;
    const auto end = start.has_value() ? add_nonnegative(*start, definition.width) : std::nullopt;
    if (!start.has_value() || !end.has_value()) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kOutOfRange, "window boundary overflows int64"});
    }
    windows.push_back(WindowKey{*start, *end});
  }
  return windows;
}

[[nodiscard]] bool final_at(const WindowKey window, const std::int64_t watermark,
                            const std::int64_t allowed_lateness) noexcept {
  const auto final_boundary = add_nonnegative(window.end, allowed_lateness);
  return final_boundary.has_value() && watermark >= *final_boundary;
}

} // namespace

class WindowedMaterializedView::Impl {
public:
  struct WindowState {
    IncrementalAggregateSet aggregate;
    std::uint64_t revision{};
    bool emitted{};
    bool finalized{};
  };

  Impl(schema::TabletId tablet, wal::WalId wal, WindowDefinition configured)
      : definition(configured), position{tablet, wal, 0U} {}

  WindowDefinition definition;
  SourcePosition position;
  std::int64_t current_watermark{std::numeric_limits<std::int64_t>::min()};
  std::map<WindowKey, WindowState> windows;
  std::map<std::uint64_t, AggregateInput> rows;
};

WindowedMaterializedView::WindowedMaterializedView(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
WindowedMaterializedView::~WindowedMaterializedView() = default;
WindowedMaterializedView::WindowedMaterializedView(WindowedMaterializedView&&) noexcept = default;
WindowedMaterializedView&
WindowedMaterializedView::operator=(WindowedMaterializedView&&) noexcept = default;

common::Result<WindowedMaterializedView>
WindowedMaterializedView::create(schema::TabletId tablet_id, wal::WalId wal_id,
                                 const WindowDefinition definition) {
  if (tablet_id.uuid().is_nil() || !wal_id.is_valid()) {
    return common::make_unexpected(common::Status{common::StatusCode::kInvalidArgument,
                                                  "materialized-view source identity is invalid"});
  }
  if (definition.width <= 0 || definition.slide <= 0 || definition.allowed_lateness < 0 ||
      definition.width < definition.slide || definition.width % definition.slide != 0 ||
      definition.maximum_windows == 0U || definition.maximum_rows == 0U) {
    return common::make_unexpected(common::Status{common::StatusCode::kInvalidArgument,
                                                  "window definition is invalid or unbounded"});
  }
  const auto windows_per_row = static_cast<std::uint64_t>(definition.width / definition.slide);
  if (windows_per_row > definition.maximum_windows) {
    return common::make_unexpected(common::Status{common::StatusCode::kInvalidArgument,
                                                  "one row exceeds the configured window bound"});
  }
  return WindowedMaterializedView{std::make_unique<Impl>(tablet_id, wal_id, definition)};
}

common::Result<WindowedMaterializedView>
WindowedMaterializedView::restore(WindowedMaterializedViewCheckpoint checkpoint) {
  auto restored =
      create(checkpoint.position.tablet_id, checkpoint.position.wal_id, checkpoint.definition);
  if (!restored.has_value()) {
    return common::make_unexpected(
        restored.error().code() == common::StatusCode::kInvalidArgument
            ? common::Status{common::StatusCode::kCorruption,
                             "materialized-view checkpoint configuration is invalid"}
            : restored.error());
  }
  if (checkpoint.rows.size() > checkpoint.definition.maximum_rows ||
      checkpoint.windows.size() > checkpoint.definition.maximum_windows) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kCorruption, "materialized-view checkpoint exceeds declared bounds"});
  }

  std::map<std::uint64_t, AggregateInput> rows;
  std::map<WindowKey, std::vector<AggregateInput>> expected_window_rows;
  try {
    std::uint64_t prior_identity = 0U;
    for (const AggregateInput& input : checkpoint.rows) {
      if (input.row_identity <= prior_identity || input.source_order == 0U ||
          !std::isfinite(input.value) || !std::isfinite(input.weight) ||
          !std::isfinite(input.value * input.weight)) {
        return common::make_unexpected(
            common::Status{common::StatusCode::kCorruption,
                           "materialized-view checkpoint rows are not canonical"});
      }
      prior_identity = input.row_identity;
      rows.emplace(input.row_identity, input);
      auto row_windows = windows_for(input.event_time, checkpoint.definition);
      if (!row_windows.has_value()) {
        return common::make_unexpected(
            common::Status{common::StatusCode::kCorruption,
                           "materialized-view checkpoint row has invalid window coordinates"});
      }
      for (const WindowKey window : *row_windows) {
        expected_window_rows[window].push_back(input);
      }
    }

    std::optional<WindowKey> prior_window;
    for (MaterializedWindowCheckpoint& window : checkpoint.windows) {
      const auto expected_end = add_nonnegative(window.window.start, checkpoint.definition.width);
      if ((prior_window.has_value() && window.window <= *prior_window) ||
          !expected_end.has_value() || *expected_end != window.window.end ||
          window.window.start % checkpoint.definition.slide != 0 || window.revision == 0U ||
          !window.emitted ||
          window.finalized != final_at(window.window, checkpoint.watermark,
                                       checkpoint.definition.allowed_lateness)) {
        return common::make_unexpected(
            common::Status{common::StatusCode::kCorruption,
                           "materialized-view checkpoint window metadata is inconsistent"});
      }
      prior_window = window.window;
      const auto expected = expected_window_rows.find(window.window);
      const std::span<const AggregateInput> expected_rows =
          expected == expected_window_rows.end()
              ? std::span<const AggregateInput>{}
              : std::span<const AggregateInput>{expected->second};
      if (!std::ranges::equal(window.aggregate.rows, expected_rows)) {
        return common::make_unexpected(
            common::Status{common::StatusCode::kCorruption,
                           "materialized-view checkpoint window rows disagree with visible rows"});
      }
      auto aggregate = IncrementalAggregateSet::restore(std::move(window.aggregate));
      if (!aggregate.has_value()) {
        return common::make_unexpected(aggregate.error());
      }
      restored->impl_->windows.emplace(window.window,
                                       Impl::WindowState{std::move(*aggregate), window.revision,
                                                         window.emitted, window.finalized});
      expected_window_rows.erase(window.window);
    }
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "materialized-view checkpoint restore allocation failed"});
  }
  if (!expected_window_rows.empty()) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kCorruption, "materialized-view checkpoint omits a populated window"});
  }
  restored->impl_->position = checkpoint.position;
  restored->impl_->current_watermark = checkpoint.watermark;
  restored->impl_->rows = std::move(rows);
  return restored;
}

common::Result<std::vector<MaterializedViewChange>>
WindowedMaterializedView::apply_committed(const SourcePosition position,
                                          MaterializedViewInput input) {
  if (position.tablet_id != impl_->position.tablet_id ||
      position.wal_id != impl_->position.wal_id ||
      position.record_sequence != impl_->position.record_sequence + 1U) {
    return common::make_unexpected(common::Status{common::StatusCode::kInvalidArgument,
                                                  "materialized-view input is not consecutive"});
  }
  if (input.aggregate.row_identity == 0U ||
      (!input.tombstone &&
       (input.aggregate.source_order == 0U || !std::isfinite(input.aggregate.value) ||
        !std::isfinite(input.aggregate.weight) ||
        !std::isfinite(input.aggregate.value * input.aggregate.weight)))) {
    return common::make_unexpected(common::Status{common::StatusCode::kInvalidArgument,
                                                  "materialized-view row input is invalid"});
  }

  const auto existing = impl_->rows.find(input.aggregate.row_identity);
  if (!input.tombstone && existing == impl_->rows.end() &&
      impl_->rows.size() >= impl_->definition.maximum_rows) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "materialized-view row bound is exhausted"});
  }

  std::vector<WindowKey> old_windows;
  if (existing != impl_->rows.end()) {
    auto computed = windows_for(existing->second.event_time, impl_->definition);
    if (!computed.has_value()) {
      return common::make_unexpected(computed.error());
    }
    old_windows = std::move(*computed);
  }
  std::vector<WindowKey> new_windows;
  if (!input.tombstone) {
    auto computed = windows_for(input.aggregate.event_time, impl_->definition);
    if (!computed.has_value()) {
      return common::make_unexpected(computed.error());
    }
    new_windows = std::move(*computed);
  }

  std::set<WindowKey> affected(old_windows.begin(), old_windows.end());
  affected.insert(new_windows.begin(), new_windows.end());
  std::size_t missing = 0U;
  for (const WindowKey window : affected) {
    missing += impl_->windows.contains(window) ? 0U : 1U;
  }
  if (missing > impl_->definition.maximum_windows - impl_->windows.size()) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "materialized-view window bound is exhausted"});
  }

  for (const WindowKey window : affected) {
    const auto current = impl_->windows.find(window);
    if (current == impl_->windows.end()) {
      continue;
    }
    common::Status status = common::Status::ok();
    if (std::ranges::find(new_windows, window) != new_windows.end()) {
      status = current->second.aggregate.validate_upsert(input.aggregate);
    } else if (std::ranges::find(old_windows, window) != old_windows.end()) {
      status = current->second.aggregate.validate_erase(input.aggregate.row_identity);
    }
    if (!status.is_ok()) {
      return common::make_unexpected(status);
    }
  }

  std::map<WindowKey, bool> previously_emitted;
  for (const WindowKey window : affected) {
    auto [iterator, inserted] = impl_->windows.try_emplace(window);
    static_cast<void>(inserted);
    previously_emitted.emplace(window, iterator->second.emitted);
    iterator->second.finalized =
        iterator->second.finalized ||
        final_at(window, impl_->current_watermark, impl_->definition.allowed_lateness);
  }
  if (existing != impl_->rows.end()) {
    for (const WindowKey window : old_windows) {
      const auto status = impl_->windows.at(window).aggregate.erase(input.aggregate.row_identity);
      if (!status.is_ok()) {
        return common::make_unexpected(status);
      }
    }
  }
  if (!input.tombstone) {
    for (const WindowKey window : new_windows) {
      const auto status = impl_->windows.at(window).aggregate.upsert(input.aggregate);
      if (!status.is_ok()) {
        return common::make_unexpected(status);
      }
    }
    impl_->rows.insert_or_assign(input.aggregate.row_identity, input.aggregate);
  } else if (existing != impl_->rows.end()) {
    impl_->rows.erase(existing);
  }

  std::vector<MaterializedViewChange> changes;
  changes.reserve(affected.size());
  for (const WindowKey window : affected) {
    Impl::WindowState& state = impl_->windows.at(window);
    ++state.revision;
    const AggregateSnapshot value = state.aggregate.snapshot();
    const bool was_emitted = previously_emitted.at(window);
    const WindowResultStatus status =
        was_emitted
            ? WindowResultStatus::kCorrected
            : (state.finalized ? WindowResultStatus::kFinalized : WindowResultStatus::kProvisional);
    changes.push_back(MaterializedViewChange{value.count == 0U ? LogicalChangeOperation::kDelete
                                                               : LogicalChangeOperation::kUpsert,
                                             window, state.revision, status, value});
    state.emitted = true;
  }
  impl_->position = position;
  return changes;
}

common::Result<std::vector<MaterializedViewChange>>
WindowedMaterializedView::advance_watermark(const std::int64_t watermark) {
  if (watermark < impl_->current_watermark) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument, "watermark cannot move backward"});
  }
  impl_->current_watermark = watermark;
  std::vector<MaterializedViewChange> changes;
  for (auto& [window, state] : impl_->windows) {
    if (state.finalized || !final_at(window, watermark, impl_->definition.allowed_lateness)) {
      continue;
    }
    state.finalized = true;
    const AggregateSnapshot value = state.aggregate.snapshot();
    if (value.count == 0U) {
      continue;
    }
    ++state.revision;
    state.emitted = true;
    changes.push_back(MaterializedViewChange{LogicalChangeOperation::kUpsert, window,
                                             state.revision, WindowResultStatus::kFinalized,
                                             value});
  }
  return changes;
}

SourcePosition WindowedMaterializedView::applied_position() const {
  return impl_->position;
}

std::int64_t WindowedMaterializedView::watermark() const noexcept {
  return impl_->current_watermark;
}

std::size_t WindowedMaterializedView::retained_rows() const noexcept {
  return impl_->rows.size();
}

std::size_t WindowedMaterializedView::open_windows() const noexcept {
  return impl_->windows.size();
}

common::Result<WindowedMaterializedViewCheckpoint> WindowedMaterializedView::checkpoint() const {
  try {
    WindowedMaterializedViewCheckpoint checkpoint{.definition = impl_->definition,
                                                  .position = impl_->position,
                                                  .watermark = impl_->current_watermark};
    checkpoint.rows.reserve(impl_->rows.size());
    for (const auto& [identity, input] : impl_->rows) {
      static_cast<void>(identity);
      checkpoint.rows.push_back(input);
    }
    checkpoint.windows.reserve(impl_->windows.size());
    for (const auto& [window, state] : impl_->windows) {
      auto aggregate = state.aggregate.checkpoint();
      if (!aggregate.has_value()) {
        return common::make_unexpected(aggregate.error());
      }
      checkpoint.windows.push_back(MaterializedWindowCheckpoint{
          window, state.revision, state.emitted, state.finalized, std::move(*aggregate)});
    }
    return checkpoint;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kResourceExhausted, "materialized-view checkpoint allocation failed"});
  }
}

} // namespace chronos::live
