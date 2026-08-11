#include "chronos/manifest/temporal_publication.hpp"

#include "chronos/manifest/raft_tablet_physical_snapshot.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/manifest/temporal_validation.hpp"

#include <algorithm>
#include <atomic>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace {

[[nodiscard]] common::Status invalid(std::string message) {
  return {common::StatusCode::kInvalidArgument, std::move(message)};
}
[[nodiscard]] common::Status exhausted(std::string message) {
  return {common::StatusCode::kResourceExhausted, std::move(message)};
}
[[nodiscard]] common::Status unavailable(std::string message) {
  return {common::StatusCode::kUnavailable, std::move(message)};
}

[[nodiscard]] common::Status decode_status(const ManifestDecodeError& error) {
  return error.status();
}

[[nodiscard]] common::Status
validate_selected(const LoadedTemporalManifestGeneration& selected,
                  const std::span<const TabletSchemaBinding> schema_bindings,
                  const ManifestDecodeLimits limits) {
  auto decoded = decode_manifest_v2_temporal_exact(selected.encoded_bytes(), limits);
  if (!decoded.has_value())
    return decode_status(decoded.error());
  if (decoded->generation() != selected.generation() ||
      decoded->database_id() != selected.database_id()) {
    return invalid("loaded temporal Manifest owner disagrees with its encoded bytes");
  }
  return validate_manifest_v2_temporal_schema_binding(*decoded, schema_bindings);
}

} // namespace

namespace detail {

class TemporalDatabaseStoragePublisherImpl {
public:
  explicit TemporalDatabaseStoragePublisherImpl(
      std::shared_ptr<const LoadedTemporalManifestGeneration> selected)
      : selected_(std::move(selected)), published_generations_{selected_} {}

  [[nodiscard]] common::Result<TemporalDatabaseStorageSnapshot> snapshot() const {
    if (failed_.load(std::memory_order_acquire))
      return common::make_unexpected(unavailable("temporal storage publisher is failed closed"));
    return TemporalDatabaseStorageSnapshot{
        std::atomic_load_explicit(&selected_, std::memory_order_acquire)};
  }

  [[nodiscard]] common::Result<TemporalDatabaseStorageSnapshot>
  publish(const DurableTemporalManifestPublicationRequest& request) {
    if (failed_.load(std::memory_order_acquire))
      return common::make_unexpected(unavailable("temporal storage publisher is failed closed"));
    if (request.selected_manifest == nullptr)
      return common::make_unexpected(invalid("temporal Manifest publication requires an owner"));
    auto fail = [&](common::Status status) -> common::Result<TemporalDatabaseStorageSnapshot> {
      failed_.store(true, std::memory_order_release);
      return common::make_unexpected(std::move(status));
    };
    try {
      const std::shared_ptr<const LoadedTemporalManifestGeneration> current =
          std::atomic_load_explicit(&selected_, std::memory_order_acquire);
      auto predecessor =
          decode_manifest_v2_temporal_exact(current->encoded_bytes(), request.decode_limits);
      if (!predecessor.has_value())
        return fail(decode_status(predecessor.error()));
      auto successor = decode_manifest_v2_temporal_exact(request.selected_manifest->encoded_bytes(),
                                                         request.decode_limits);
      if (!successor.has_value())
        return fail(decode_status(successor.error()));
      common::Status transition = validate_manifest_v2_temporal_transition(*predecessor, *successor,
                                                                           request.schema_bindings);
      if (!transition.is_ok())
        return fail(std::move(transition));
      if (successor->generation() != request.selected_manifest->generation() ||
          successor->database_id() != request.selected_manifest->database_id()) {
        return fail(invalid("durable temporal Manifest owner disagrees with its encoded bytes"));
      }
      std::erase_if(published_generations_,
                    [](const auto& generation) { return generation.expired(); });
      published_generations_.reserve(published_generations_.size() + 1U);
      published_generations_.push_back(request.selected_manifest);
      std::atomic_store_explicit(&selected_, request.selected_manifest, std::memory_order_release);
      return TemporalDatabaseStorageSnapshot{request.selected_manifest};
    } catch (const std::bad_alloc&) {
      return fail(exhausted("temporal Manifest publication allocation failed"));
    } catch (const std::length_error&) {
      return fail(exhausted("temporal Manifest publication exceeded limits"));
    }
  }

  [[nodiscard]] common::Result<PublishedTemporalSourceRetirement>
  publish_source_retirement(const DurableTemporalSourceRetirementPublicationRequest& request) {
    if (failed_.load(std::memory_order_acquire))
      return common::make_unexpected(unavailable("temporal storage publisher is failed closed"));
    if (request.selected_manifest == nullptr)
      return common::make_unexpected(
          invalid("temporal source-retirement publication requires an owner"));
    auto fail = [&](common::Status status) -> common::Result<PublishedTemporalSourceRetirement> {
      failed_.store(true, std::memory_order_release);
      return common::make_unexpected(std::move(status));
    };
    if (request.source_retirement == nullptr)
      return fail(invalid("temporal source-retirement publication requires its authority"));
    try {
      const std::shared_ptr<const LoadedTemporalManifestGeneration> current =
          std::atomic_load_explicit(&selected_, std::memory_order_acquire);
      auto predecessor =
          decode_manifest_v2_temporal_exact(current->encoded_bytes(), request.decode_limits);
      if (!predecessor.has_value())
        return fail(decode_status(predecessor.error()));
      auto successor = decode_manifest_v2_temporal_exact(request.selected_manifest->encoded_bytes(),
                                                         request.decode_limits);
      if (!successor.has_value())
        return fail(decode_status(successor.error()));
      common::Result<BuiltRaftTabletSourceRetirementManifest> rebuilt =
          build_raft_tablet_source_retirement_manifest(*predecessor, *request.source_retirement);
      if (!rebuilt.has_value())
        return fail(rebuilt.error());
      if (!std::ranges::equal(rebuilt->manifest.bytes(),
                              request.selected_manifest->encoded_bytes())) {
        return fail(invalid(
            "durable source-retirement Manifest differs from the authorized exact successor"));
      }
      common::Status binding =
          validate_manifest_v2_temporal_schema_binding(*successor, request.schema_bindings);
      if (!binding.is_ok())
        return fail(std::move(binding));
      if (successor->generation() != request.selected_manifest->generation() ||
          successor->database_id() != request.selected_manifest->database_id()) {
        return fail(
            invalid("durable source-retirement Manifest owner disagrees with its encoded bytes"));
      }
      std::erase_if(published_generations_,
                    [](const auto& generation) { return generation.expired(); });
      std::vector<std::weak_ptr<const LoadedTemporalManifestGeneration>> retirement_pins;
      retirement_pins.reserve(published_generations_.size());
      for (const auto& generation_pin : published_generations_) {
        const std::shared_ptr<const LoadedTemporalManifestGeneration> generation =
            generation_pin.lock();
        if (generation == nullptr)
          continue;
        const bool names_retired_part =
            std::ranges::any_of(rebuilt->retired_parts, [&](const TemporalPartDescriptor& retired) {
              return std::ranges::find(generation->parts(), retired.part_id,
                                       &TemporalPartDescriptor::part_id) !=
                     generation->parts().end();
            });
        if (names_retired_part)
          retirement_pins.push_back(generation_pin);
      }
      published_generations_.reserve(published_generations_.size() + 1U);
      published_generations_.push_back(request.selected_manifest);
      PublishedTemporalSourceRetirement published{
          .snapshot = TemporalDatabaseStorageSnapshot{request.selected_manifest},
          .retirement = TemporalRetiredPartSet{rebuilt->predecessor_generation,
                                               std::move(rebuilt->retired_parts),
                                               std::move(retirement_pins)}};
      std::atomic_store_explicit(&selected_, request.selected_manifest, std::memory_order_release);
      return published;
    } catch (const std::bad_alloc&) {
      return fail(exhausted("temporal source-retirement publication allocation failed"));
    } catch (const std::length_error&) {
      return fail(exhausted("temporal source-retirement publication exceeded limits"));
    }
  }

  [[nodiscard]] bool is_usable() const noexcept {
    return !failed_.load(std::memory_order_acquire);
  }

  void fail_closed() noexcept {
    failed_.store(true, std::memory_order_release);
  }

  [[nodiscard]] common::Status poison_status() const {
    return is_usable() ? common::Status::ok() : poison_status_;
  }

private:
  std::shared_ptr<const LoadedTemporalManifestGeneration> selected_;
  // Weak ownership never extends an epoch lifetime. Expired entries are pruned before every
  // publication; live entries let retirement cover older readers of parts retained across epochs.
  std::vector<std::weak_ptr<const LoadedTemporalManifestGeneration>> published_generations_;
  std::atomic<bool> failed_{false};
  common::Status poison_status_{
      unavailable("temporal storage publisher failed after durable successor")};
};

} // namespace detail

TemporalRetiredPartSet::TemporalRetiredPartSet(
    const std::uint64_t predecessor_generation, std::vector<TemporalPartDescriptor> parts,
    std::vector<std::weak_ptr<const LoadedTemporalManifestGeneration>> generation_pins) noexcept
    : predecessor_generation_(predecessor_generation), parts_(std::move(parts)),
      generation_pins_(std::move(generation_pins)) {}

std::uint64_t TemporalRetiredPartSet::predecessor_generation() const noexcept {
  return predecessor_generation_;
}

std::span<const TemporalPartDescriptor> TemporalRetiredPartSet::parts() const noexcept {
  return parts_;
}

bool TemporalRetiredPartSet::is_pinned() const noexcept {
  return std::ranges::any_of(generation_pins_,
                             [](const auto& generation) { return !generation.expired(); });
}

TemporalDatabaseStorageSnapshot::TemporalDatabaseStorageSnapshot(
    std::shared_ptr<const LoadedTemporalManifestGeneration> selected) noexcept
    : selected_(std::move(selected)) {}

std::uint64_t TemporalDatabaseStorageSnapshot::generation() const noexcept {
  return selected_->generation();
}
const DatabaseId& TemporalDatabaseStorageSnapshot::database_id() const noexcept {
  return selected_->database_id();
}
common::ByteView TemporalDatabaseStorageSnapshot::manifest_bytes() const noexcept {
  return selected_->encoded_bytes();
}
std::span<const TemporalTabletDescriptor>
TemporalDatabaseStorageSnapshot::tablets() const noexcept {
  return selected_->tablets();
}
std::span<const TemporalPartDescriptor> TemporalDatabaseStorageSnapshot::parts() const noexcept {
  return selected_->parts();
}
std::span<const TemporalRetryDescriptor> TemporalDatabaseStorageSnapshot::retries() const noexcept {
  return selected_->retries();
}
std::shared_ptr<const LoadedTemporalManifestGeneration>
TemporalDatabaseStorageSnapshot::selected_manifest() const noexcept {
  return selected_;
}

TemporalDatabaseStoragePublisher::TemporalDatabaseStoragePublisher(
    std::unique_ptr<detail::TemporalDatabaseStoragePublisherImpl> implementation) noexcept
    : implementation_(std::move(implementation)) {}
TemporalDatabaseStoragePublisher::~TemporalDatabaseStoragePublisher() = default;
TemporalDatabaseStoragePublisher::TemporalDatabaseStoragePublisher(
    TemporalDatabaseStoragePublisher&&) noexcept = default;
TemporalDatabaseStoragePublisher&
TemporalDatabaseStoragePublisher::operator=(TemporalDatabaseStoragePublisher&&) noexcept = default;

common::Result<TemporalDatabaseStoragePublisher> TemporalDatabaseStoragePublisher::create(
    std::shared_ptr<const LoadedTemporalManifestGeneration> selected,
    const std::span<const TabletSchemaBinding> schema_bindings, const ManifestDecodeLimits limits) {
  if (selected == nullptr)
    return common::make_unexpected(invalid("temporal storage publisher requires an owner"));
  common::Status validation = validate_selected(*selected, schema_bindings, limits);
  if (!validation.is_ok())
    return common::make_unexpected(std::move(validation));
  try {
    return TemporalDatabaseStoragePublisher{
        std::make_unique<detail::TemporalDatabaseStoragePublisherImpl>(std::move(selected))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("temporal storage publisher allocation failed"));
  }
}

common::Result<TemporalDatabaseStorageSnapshot> TemporalDatabaseStoragePublisher::snapshot() const {
  if (implementation_ == nullptr)
    return common::make_unexpected(invalid("temporal storage publisher was moved from"));
  return implementation_->snapshot();
}
common::Result<TemporalDatabaseStorageSnapshot> TemporalDatabaseStoragePublisher::publish_manifest(
    const DurableTemporalManifestPublicationRequest& request) {
  if (implementation_ == nullptr)
    return common::make_unexpected(invalid("temporal storage publisher was moved from"));
  return implementation_->publish(request);
}
common::Result<PublishedTemporalSourceRetirement>
TemporalDatabaseStoragePublisher::publish_source_retirement_manifest(
    const DurableTemporalSourceRetirementPublicationRequest& request) {
  if (implementation_ == nullptr)
    return common::make_unexpected(invalid("temporal storage publisher was moved from"));
  return implementation_->publish_source_retirement(request);
}
void TemporalDatabaseStoragePublisher::fail_closed_after_durable_successor() noexcept {
  if (implementation_ != nullptr)
    implementation_->fail_closed();
}
bool TemporalDatabaseStoragePublisher::is_usable() const noexcept {
  return implementation_ != nullptr && implementation_->is_usable();
}
common::Status TemporalDatabaseStoragePublisher::poison_status() const {
  return implementation_ == nullptr ? invalid("temporal storage publisher was moved from")
                                    : implementation_->poison_status();
}

} // namespace chronos::manifest
