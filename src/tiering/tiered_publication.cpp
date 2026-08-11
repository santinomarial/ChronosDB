#include "chronos/tiering/tiered_publication.hpp"

#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/manifest/temporal_validation.hpp"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <ranges>
#include <stdexcept>
#include <string>
#include <utility>

namespace chronos::tiering {
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

[[nodiscard]] common::Status
validate_loaded_cold(const LoadedColdLocationManifest& loaded,
                     const manifest::DecodedTemporalManifestView& decoded_manifest,
                     const ColdLocationManifestDecodeLimits limits) {
  auto decoded = decode_cold_location_manifest_v1_exact(loaded.encoded_bytes(), limits);
  if (!decoded.has_value())
    return decoded.error().status();
  const DecodedColdLocationManifest& retained = loaded.manifest();
  if (decoded->generation() != retained.generation() ||
      decoded->previous_generation() != retained.previous_generation() ||
      decoded->base_manifest_generation() != retained.base_manifest_generation() ||
      decoded->database_id() != retained.database_id() ||
      decoded->object_store_id() != retained.object_store_id() ||
      !std::ranges::equal(decoded->locations(), retained.locations())) {
    return invalid("loaded cold manifest owner disagrees with its encoded bytes");
  }
  return validate_cold_location_manifest_binding(*decoded, decoded_manifest);
}

[[nodiscard]] common::Result<manifest::DecodedTemporalManifestView>
decode_manifest_snapshot(const manifest::TemporalDatabaseStorageSnapshot& snapshot,
                         const manifest::ManifestDecodeLimits limits) {
  auto decoded = manifest::decode_manifest_v2_temporal_exact(snapshot.manifest_bytes(), limits);
  if (!decoded.has_value())
    return common::make_unexpected(decoded.error().status());
  if (decoded->generation() != snapshot.generation() ||
      decoded->database_id() != snapshot.database_id()) {
    return common::make_unexpected(
        invalid("temporal Manifest snapshot disagrees with its encoded bytes"));
  }
  return std::move(*decoded);
}

} // namespace

namespace detail {

class TieredDatabaseStorageEpoch {
public:
  TieredDatabaseStorageEpoch(
      manifest::TemporalDatabaseStorageSnapshot manifest_snapshot,
      std::shared_ptr<const LoadedColdLocationManifest> cold_manifest) noexcept
      : manifest_snapshot_(std::move(manifest_snapshot)), cold_manifest_(std::move(cold_manifest)) {
  }

  manifest::TemporalDatabaseStorageSnapshot manifest_snapshot_;
  std::shared_ptr<const LoadedColdLocationManifest> cold_manifest_;
};

class TieredDatabaseStoragePublisherImpl {
public:
  explicit TieredDatabaseStoragePublisherImpl(
      std::shared_ptr<const TieredDatabaseStorageEpoch> epoch)
      : epoch_(std::move(epoch)) {}

  [[nodiscard]] common::Result<TieredDatabaseStorageSnapshot> snapshot() const {
    if (failed_.load(std::memory_order_acquire))
      return common::make_unexpected(unavailable("tiered storage publisher is failed closed"));
    return TieredDatabaseStorageSnapshot{
        std::atomic_load_explicit(&epoch_, std::memory_order_acquire)};
  }

  [[nodiscard]] common::Result<TieredDatabaseStorageSnapshot>
  publish(const DurableTieredDatabaseStoragePublicationRequest& request) {
    if (failed_.load(std::memory_order_acquire))
      return common::make_unexpected(unavailable("tiered storage publisher is failed closed"));
    auto fail = [&](common::Status status) -> common::Result<TieredDatabaseStorageSnapshot> {
      failed_.store(true, std::memory_order_release);
      return common::make_unexpected(std::move(status));
    };
    try {
      const std::shared_ptr<const TieredDatabaseStorageEpoch> current =
          std::atomic_load_explicit(&epoch_, std::memory_order_acquire);
      auto predecessor =
          decode_manifest_snapshot(current->manifest_snapshot_, request.manifest_decode_limits);
      if (!predecessor.has_value())
        return fail(predecessor.error());
      auto successor =
          decode_manifest_snapshot(request.manifest_snapshot, request.manifest_decode_limits);
      if (!successor.has_value())
        return fail(successor.error());

      if (successor->generation() == predecessor->generation()) {
        if (!std::ranges::equal(request.manifest_snapshot.manifest_bytes(),
                                current->manifest_snapshot_.manifest_bytes())) {
          return fail(invalid("equal Manifest v2 generation has different published bytes"));
        }
      } else {
        if (predecessor->generation() == std::numeric_limits<std::uint64_t>::max() ||
            successor->generation() != predecessor->generation() + 1U) {
          return fail(invalid("tiered publication skips the live Manifest v2 generation"));
        }
        common::Status transition = manifest::validate_manifest_v2_temporal_transition(
            *predecessor, *successor, request.schema_bindings);
        if (!transition.is_ok())
          return fail(std::move(transition));
      }

      if (request.cold_manifest != nullptr) {
        common::Status binding =
            validate_loaded_cold(*request.cold_manifest, *successor, request.cold_decode_limits);
        if (!binding.is_ok())
          return fail(std::move(binding));
      }
      if (current->cold_manifest_ == nullptr) {
        if (request.cold_manifest != nullptr &&
            request.cold_manifest->manifest().generation() != 1U) {
          return fail(invalid("first published cold manifest is not generation one"));
        }
      } else {
        if (request.cold_manifest == nullptr)
          return fail(invalid("tiered publication cannot remove cold authority"));
        const auto current_generation = current->cold_manifest_->manifest().generation();
        const auto candidate_generation = request.cold_manifest->manifest().generation();
        if (candidate_generation == current_generation) {
          if (!std::ranges::equal(request.cold_manifest->encoded_bytes(),
                                  current->cold_manifest_->encoded_bytes())) {
            return fail(invalid("equal cold generation has different published bytes"));
          }
        } else {
          common::Status transition = validate_cold_location_manifest_transition(
              current->cold_manifest_->manifest(), request.cold_manifest->manifest());
          if (!transition.is_ok())
            return fail(std::move(transition));
        }
      }

      auto next = std::make_shared<const TieredDatabaseStorageEpoch>(request.manifest_snapshot,
                                                                     request.cold_manifest);
      // All epoch state is fully initialized above. Release publication synchronizes with acquire
      // snapshots; shared ownership keeps the exact old pair alive until its last reader exits.
      std::atomic_store_explicit(&epoch_, next, std::memory_order_release);
      return TieredDatabaseStorageSnapshot{std::move(next)};
    } catch (const std::bad_alloc&) {
      return fail(exhausted("tiered storage publication allocation failed"));
    } catch (const std::length_error&) {
      return fail(exhausted("tiered storage publication exceeded limits"));
    }
  }

  void fail_closed() noexcept {
    failed_.store(true, std::memory_order_release);
  }

  [[nodiscard]] bool is_usable() const noexcept {
    return !failed_.load(std::memory_order_acquire);
  }

  [[nodiscard]] common::Status poison_status() const {
    return is_usable() ? common::Status::ok()
                       : unavailable("tiered storage publisher failed after durable successor");
  }

private:
  std::shared_ptr<const TieredDatabaseStorageEpoch> epoch_;
  std::atomic<bool> failed_{false};
};

} // namespace detail

TieredDatabaseStorageSnapshot::TieredDatabaseStorageSnapshot(
    std::shared_ptr<const detail::TieredDatabaseStorageEpoch> epoch) noexcept
    : epoch_(std::move(epoch)) {}

std::uint64_t TieredDatabaseStorageSnapshot::manifest_generation() const noexcept {
  return epoch_->manifest_snapshot_.generation();
}

const manifest::DatabaseId& TieredDatabaseStorageSnapshot::database_id() const noexcept {
  return epoch_->manifest_snapshot_.database_id();
}

const manifest::TemporalDatabaseStorageSnapshot&
TieredDatabaseStorageSnapshot::manifest_snapshot() const noexcept {
  return epoch_->manifest_snapshot_;
}

const LoadedColdLocationManifest* TieredDatabaseStorageSnapshot::cold_manifest() const noexcept {
  return epoch_->cold_manifest_.get();
}

const ColdPartLocationDescriptor*
TieredDatabaseStorageSnapshot::find_cold_location(const cseg::PartId& part_id) const noexcept {
  if (epoch_->cold_manifest_ == nullptr)
    return nullptr;
  const auto locations = epoch_->cold_manifest_->manifest().locations();
  const auto found =
      std::ranges::lower_bound(locations, part_id, {}, &ColdPartLocationDescriptor::part_id);
  return found != locations.end() && found->part_id == part_id ? std::addressof(*found) : nullptr;
}

TieredDatabaseStoragePublisher::TieredDatabaseStoragePublisher(
    std::unique_ptr<detail::TieredDatabaseStoragePublisherImpl> impl) noexcept
    : impl_(std::move(impl)) {}
TieredDatabaseStoragePublisher::~TieredDatabaseStoragePublisher() = default;
TieredDatabaseStoragePublisher::TieredDatabaseStoragePublisher(
    TieredDatabaseStoragePublisher&&) noexcept = default;
TieredDatabaseStoragePublisher&
TieredDatabaseStoragePublisher::operator=(TieredDatabaseStoragePublisher&&) noexcept = default;

common::Result<TieredDatabaseStoragePublisher> TieredDatabaseStoragePublisher::create(
    manifest::TemporalDatabaseStorageSnapshot manifest_snapshot,
    std::shared_ptr<const LoadedColdLocationManifest> cold_manifest,
    const manifest::ManifestDecodeLimits manifest_limits,
    const ColdLocationManifestDecodeLimits cold_limits) {
  auto decoded = decode_manifest_snapshot(manifest_snapshot, manifest_limits);
  if (!decoded.has_value())
    return common::make_unexpected(decoded.error());
  if (cold_manifest != nullptr) {
    common::Status binding = validate_loaded_cold(*cold_manifest, *decoded, cold_limits);
    if (!binding.is_ok())
      return common::make_unexpected(std::move(binding));
  }
  try {
    auto epoch = std::make_shared<const detail::TieredDatabaseStorageEpoch>(
        std::move(manifest_snapshot), std::move(cold_manifest));
    return TieredDatabaseStoragePublisher{
        std::make_unique<detail::TieredDatabaseStoragePublisherImpl>(std::move(epoch))};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("tiered storage publisher allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("tiered storage publisher exceeded limits"));
  }
}

common::Result<TieredDatabaseStorageSnapshot> TieredDatabaseStoragePublisher::snapshot() const {
  if (impl_ == nullptr)
    return common::make_unexpected(invalid("tiered storage publisher was moved from"));
  return impl_->snapshot();
}

common::Result<TieredDatabaseStorageSnapshot> TieredDatabaseStoragePublisher::publish(
    const DurableTieredDatabaseStoragePublicationRequest& request) {
  if (impl_ == nullptr)
    return common::make_unexpected(invalid("tiered storage publisher was moved from"));
  return impl_->publish(request);
}

void TieredDatabaseStoragePublisher::fail_closed_after_durable_successor() noexcept {
  if (impl_ != nullptr)
    impl_->fail_closed();
}

bool TieredDatabaseStoragePublisher::is_usable() const noexcept {
  return impl_ != nullptr && impl_->is_usable();
}

common::Status TieredDatabaseStoragePublisher::poison_status() const {
  return impl_ == nullptr ? invalid("tiered storage publisher was moved from")
                          : impl_->poison_status();
}

} // namespace chronos::tiering
