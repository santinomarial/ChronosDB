#include "chronos/live/durable_materialized_view.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <utility>
#include <vector>

namespace chronos::live {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status corruption(std::string message) {
  return common::Status{common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Status exhausted(std::string message) {
  return common::Status{common::StatusCode::kResourceExhausted, std::move(message)};
}

} // namespace

class DurableWindowedMaterializedView::Impl {
public:
  struct DurabilityState {
    std::uint64_t generation{};
    std::uint64_t durable_sequence{};
    bool dirty{true};
  };

  Impl(MaterializedViewCheckpointIdentity configured_identity,
       MaterializedViewCheckpointStorage configured_storage,
       WindowedMaterializedView configured_view, const DurabilityState configured_state) noexcept
      : identity(configured_identity), storage(std::move(configured_storage)),
        view(std::move(configured_view)), generation(configured_state.generation),
        durable_sequence(configured_state.durable_sequence), dirty(configured_state.dirty) {}

  MaterializedViewCheckpointIdentity identity;
  MaterializedViewCheckpointStorage storage;
  WindowedMaterializedView view;
  std::uint64_t generation{};
  std::uint64_t durable_sequence{};
  bool dirty{true};
};

DurableWindowedMaterializedView::DurableWindowedMaterializedView(
    std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
DurableWindowedMaterializedView::~DurableWindowedMaterializedView() = default;
DurableWindowedMaterializedView::DurableWindowedMaterializedView(
    DurableWindowedMaterializedView&&) noexcept = default;
DurableWindowedMaterializedView&
DurableWindowedMaterializedView::operator=(DurableWindowedMaterializedView&&) noexcept = default;

common::Result<DurableWindowedMaterializedView>
DurableWindowedMaterializedView::create_new(DurableWindowedMaterializedViewConfig config) {
  try {
    const MaterializedViewCheckpointIdentity identity = config.storage.identity;
    auto view =
        WindowedMaterializedView::create(config.tablet_id, config.wal_id, config.definition);
    if (!view.has_value()) {
      return common::make_unexpected(view.error());
    }
    auto storage = MaterializedViewCheckpointStorage::create(std::move(config.storage));
    if (!storage.has_value()) {
      return common::make_unexpected(storage.error());
    }
    auto existing = storage->load_latest();
    if (!existing.has_value()) {
      return common::make_unexpected(existing.error());
    }
    if (existing->has_value()) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kAlreadyExists,
                         "new durable materialized-view directory already contains a checkpoint"});
    }
    return DurableWindowedMaterializedView{std::make_unique<Impl>(
        identity, std::move(*storage), std::move(*view), Impl::DurabilityState{})};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("durable materialized-view allocation failed"));
  }
}

common::Result<DurableWindowedMaterializedView>
DurableWindowedMaterializedView::open_existing(DurableWindowedMaterializedViewConfig config) {
  try {
    const MaterializedViewCheckpointIdentity identity = config.storage.identity;
    auto storage = MaterializedViewCheckpointStorage::open_existing(std::move(config.storage));
    if (!storage.has_value()) {
      return common::make_unexpected(storage.error());
    }
    auto latest = storage->load_latest();
    if (!latest.has_value()) {
      return common::make_unexpected(latest.error());
    }
    auto loaded = std::move(*latest);
    if (!loaded.has_value()) {
      auto view =
          WindowedMaterializedView::create(config.tablet_id, config.wal_id, config.definition);
      if (!view.has_value()) {
        return common::make_unexpected(view.error());
      }
      return DurableWindowedMaterializedView{std::make_unique<Impl>(
          identity, std::move(*storage), std::move(*view), Impl::DurabilityState{})};
    }

    BoundMaterializedViewCheckpoint checkpoint = std::move(loaded).value().checkpoint;
    if (checkpoint.identity != identity ||
        checkpoint.state.position.tablet_id != config.tablet_id ||
        checkpoint.state.position.wal_id != config.wal_id ||
        checkpoint.state.definition != config.definition) {
      return common::make_unexpected(
          corruption("durable materialized-view checkpoint disagrees with configured owner"));
    }
    const std::uint64_t durable_sequence = checkpoint.state.position.record_sequence;
    const std::uint64_t generation = checkpoint.checkpoint_generation;
    auto view = WindowedMaterializedView::restore(checkpoint.state);
    if (!view.has_value()) {
      return common::make_unexpected(view.error());
    }
    return DurableWindowedMaterializedView{
        std::make_unique<Impl>(identity, std::move(*storage), std::move(*view),
                               Impl::DurabilityState{.generation = generation,
                                                     .durable_sequence = durable_sequence,
                                                     .dirty = generation == 0U})};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(
        exhausted("durable materialized-view recovery allocation failed"));
  }
}

common::Result<std::vector<MaterializedViewChange>>
DurableWindowedMaterializedView::apply_committed(const SourcePosition position,
                                                 MaterializedViewInput input) {
  auto applied = impl_->view.apply_committed(position, input);
  if (applied.has_value()) {
    impl_->dirty = true;
  }
  return applied;
}

common::Result<std::vector<MaterializedViewChange>>
DurableWindowedMaterializedView::advance_watermark(const std::int64_t watermark) {
  const std::int64_t previous = impl_->view.watermark();
  auto advanced = impl_->view.advance_watermark(watermark);
  if (advanced.has_value() && watermark != previous) {
    impl_->dirty = true;
  }
  return advanced;
}

common::Result<InstalledMaterializedViewCheckpoint> DurableWindowedMaterializedView::checkpoint() {
  if (impl_->dirty && impl_->generation == std::numeric_limits<std::uint64_t>::max()) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kOutOfRange, "materialized-view checkpoint generation is exhausted"});
  }
  auto state = impl_->view.checkpoint();
  if (!state.has_value()) {
    return common::make_unexpected(state.error());
  }
  const std::uint64_t target_generation = impl_->dirty ? impl_->generation + 1U : impl_->generation;
  if (target_generation == 0U) {
    return common::make_unexpected(invalid("durable materialized-view generation is invalid"));
  }
  BoundMaterializedViewCheckpoint checkpoint{.identity = impl_->identity,
                                             .checkpoint_generation = target_generation,
                                             .state = std::move(*state)};
  auto installed = impl_->storage.install(checkpoint);
  if (!installed.has_value()) {
    return common::make_unexpected(installed.error());
  }
  impl_->generation = target_generation;
  impl_->durable_sequence = checkpoint.state.position.record_sequence;
  impl_->dirty = false;
  return installed;
}

SourcePosition DurableWindowedMaterializedView::applied_position() const {
  return impl_->view.applied_position();
}

std::uint64_t DurableWindowedMaterializedView::durable_record_sequence() const noexcept {
  return impl_->durable_sequence;
}

std::uint64_t DurableWindowedMaterializedView::checkpoint_generation() const noexcept {
  return impl_->generation;
}

std::int64_t DurableWindowedMaterializedView::watermark() const noexcept {
  return impl_->view.watermark();
}

std::size_t DurableWindowedMaterializedView::retained_rows() const noexcept {
  return impl_->view.retained_rows();
}

std::size_t DurableWindowedMaterializedView::open_windows() const noexcept {
  return impl_->view.open_windows();
}

} // namespace chronos::live
