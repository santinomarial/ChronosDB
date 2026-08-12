#include "chronos/tiering/tiered_publication.hpp"

#include "chronos/manifest/raft_tablet_physical_snapshot.hpp"
#include "chronos/manifest/temporal_codec.hpp"
#include "chronos/manifest/temporal_validation.hpp"
#include "chronos/tiering/tiered_reclamation.hpp"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
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
      : epoch_(std::move(epoch)), published_epochs_{epoch_} {}

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
        if (request.source_retirement != nullptr)
          return fail(invalid("tiered source retirement does not advance Manifest v2"));
        if (!std::ranges::equal(request.manifest_snapshot.manifest_bytes(),
                                current->manifest_snapshot_.manifest_bytes())) {
          return fail(invalid("equal Manifest v2 generation has different published bytes"));
        }
      } else {
        if (predecessor->generation() == std::numeric_limits<std::uint64_t>::max() ||
            successor->generation() != predecessor->generation() + 1U) {
          return fail(invalid("tiered publication skips the live Manifest v2 generation"));
        }
        common::Status transition;
        if (request.source_retirement == nullptr) {
          transition = manifest::validate_manifest_v2_temporal_transition(*predecessor, *successor,
                                                                          request.schema_bindings);
        } else {
          auto rebuilt = manifest::build_raft_tablet_source_retirement_manifest(
              *predecessor, *request.source_retirement);
          if (!rebuilt.has_value())
            return fail(rebuilt.error());
          if (!std::ranges::equal(rebuilt->manifest.bytes(),
                                  request.manifest_snapshot.manifest_bytes())) {
            return fail(
                invalid("tiered source-retirement Manifest differs from its exact authority"));
          }
          transition = manifest::validate_manifest_v2_temporal_schema_binding(
              *successor, request.schema_bindings);
        }
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
              current->cold_manifest_->manifest(), request.cold_manifest->manifest(), *successor);
          if (!transition.is_ok())
            return fail(std::move(transition));
        }
      }

      auto next = std::make_shared<const TieredDatabaseStorageEpoch>(request.manifest_snapshot,
                                                                     request.cold_manifest);
      std::erase_if(published_epochs_, [](const auto& weak) { return weak.expired(); });
      published_epochs_.push_back(next);
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

  [[nodiscard]] common::Result<TieredLocalPartReclamationProof>
  authorize_local_reclamation(const TieredPairCommitRecord& record,
                              const std::span<const cseg::PartId> part_ids) const {
    if (failed_.load(std::memory_order_acquire))
      return common::make_unexpected(unavailable("tiered storage publisher is failed closed"));
    if (part_ids.empty())
      return common::make_unexpected(invalid("tiered local reclamation identities are empty"));
    for (std::size_t index = 1U; index < part_ids.size(); ++index) {
      if (!(part_ids[index - 1U] < part_ids[index])) {
        return common::make_unexpected(
            invalid("tiered local reclamation identities are not strictly sorted"));
      }
    }
    try {
      const std::shared_ptr<const TieredDatabaseStorageEpoch> current =
          std::atomic_load_explicit(&epoch_, std::memory_order_acquire);
      if (part_ids.size() > current->manifest_snapshot_.parts().size() ||
          current->cold_manifest_ == nullptr) {
        return common::make_unexpected(
            invalid("tiered local reclamation has no complete current cold authority"));
      }
      auto manifest_digest = ingest::sha256(current->manifest_snapshot_.manifest_bytes());
      auto cold_digest = ingest::sha256(current->cold_manifest_->encoded_bytes());
      if (!manifest_digest.has_value())
        return common::make_unexpected(manifest_digest.error());
      if (!cold_digest.has_value())
        return common::make_unexpected(cold_digest.error());
      if (record.database_id != current->manifest_snapshot_.database_id() ||
          record.object_store_id != current->cold_manifest_->manifest().object_store_id() ||
          record.manifest_generation != current->manifest_snapshot_.generation() ||
          record.cold_generation != current->cold_manifest_->manifest().generation() ||
          record.manifest_length != current->manifest_snapshot_.manifest_bytes().size() ||
          record.cold_length != current->cold_manifest_->encoded_bytes().size() ||
          record.manifest_sha256 != *manifest_digest || record.cold_sha256 != *cold_digest) {
        return common::make_unexpected(
            invalid("tiered local reclamation pair differs from current publication"));
      }

      std::vector<manifest::TemporalPartDescriptor> parts;
      parts.reserve(part_ids.size());
      for (const cseg::PartId& part_id : part_ids) {
        const auto found = std::ranges::find(current->manifest_snapshot_.parts(), part_id,
                                             &manifest::TemporalPartDescriptor::part_id);
        if (found == current->manifest_snapshot_.parts().end()) {
          return common::make_unexpected(
              invalid("tiered local reclamation part is not currently Manifest-referenced"));
        }
        const auto owner =
            std::ranges::find(current->manifest_snapshot_.tablets(), found->tablet_id,
                              &manifest::TemporalTabletDescriptor::tablet_id);
        if (owner == current->manifest_snapshot_.tablets().end() ||
            owner->commit_source != found->commit_source || owner->source_id != found->source_id) {
          return common::make_unexpected(
              invalid("tiered local reclamation part differs from its tablet source authority"));
        }
        const auto locations = current->cold_manifest_->manifest().locations();
        const auto location =
            std::ranges::lower_bound(locations, part_id, {}, &ColdPartLocationDescriptor::part_id);
        if (location == locations.end() || location->part_id != part_id ||
            location->file_length != found->file_length ||
            location->content_sha256 != found->content_sha256) {
          return common::make_unexpected(
              invalid("tiered local reclamation part has no exact current cold route"));
        }
        parts.push_back(*found);
      }

      std::vector<std::weak_ptr<const TieredDatabaseStorageEpoch>> reader_pins;
      reader_pins.reserve(published_epochs_.size());
      for (const auto& weak : published_epochs_) {
        const std::shared_ptr<const TieredDatabaseStorageEpoch> epoch = weak.lock();
        if (epoch == nullptr)
          continue;
        bool requires_local = false;
        for (const manifest::TemporalPartDescriptor& candidate : parts) {
          const auto historical =
              std::ranges::find(epoch->manifest_snapshot_.parts(), candidate.part_id,
                                &manifest::TemporalPartDescriptor::part_id);
          if (historical == epoch->manifest_snapshot_.parts().end())
            continue;
          if (*historical != candidate || epoch->cold_manifest_ == nullptr) {
            requires_local = true;
            break;
          }
          const auto locations = epoch->cold_manifest_->manifest().locations();
          const auto location = std::ranges::lower_bound(locations, candidate.part_id, {},
                                                         &ColdPartLocationDescriptor::part_id);
          if (location == locations.end() || location->part_id != candidate.part_id ||
              location->file_length != candidate.file_length ||
              location->content_sha256 != candidate.content_sha256) {
            requires_local = true;
            break;
          }
        }
        if (requires_local)
          reader_pins.push_back(weak);
      }
      return TieredLocalPartReclamationProof{record, TieredDatabaseStorageSnapshot{current},
                                             std::move(parts), std::move(reader_pins)};
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(
          exhausted("tiered local reclamation authorization allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(
          exhausted("tiered local reclamation authorization exceeded limits"));
    }
  }

  [[nodiscard]] common::Result<TieredRemoteObjectReclamationProof>
  authorize_remote_reclamation(const TieredPairCommitRecord& record,
                               const std::span<const cseg::PartId> part_ids,
                               const TieredRemoteObjectReclamationLimits limits) const {
    if (failed_.load(std::memory_order_acquire))
      return common::make_unexpected(unavailable("tiered storage publisher is failed closed"));
    if (part_ids.empty() || part_ids.size() > limits.maximum_objects)
      return common::make_unexpected(invalid("tiered remote reclamation identities exceed limits"));
    for (std::size_t index = 1U; index < part_ids.size(); ++index) {
      if (!(part_ids[index - 1U] < part_ids[index])) {
        return common::make_unexpected(
            invalid("tiered remote reclamation identities are not strictly sorted"));
      }
    }
    try {
      const std::shared_ptr<const TieredDatabaseStorageEpoch> current =
          std::atomic_load_explicit(&epoch_, std::memory_order_acquire);
      if (current->cold_manifest_ == nullptr) {
        return common::make_unexpected(
            invalid("tiered remote reclamation has no current cold authority"));
      }
      auto manifest_digest = ingest::sha256(current->manifest_snapshot_.manifest_bytes());
      auto cold_digest = ingest::sha256(current->cold_manifest_->encoded_bytes());
      if (!manifest_digest.has_value())
        return common::make_unexpected(manifest_digest.error());
      if (!cold_digest.has_value())
        return common::make_unexpected(cold_digest.error());
      if (record.database_id != current->manifest_snapshot_.database_id() ||
          record.object_store_id != current->cold_manifest_->manifest().object_store_id() ||
          record.manifest_generation != current->manifest_snapshot_.generation() ||
          record.cold_generation != current->cold_manifest_->manifest().generation() ||
          record.manifest_length != current->manifest_snapshot_.manifest_bytes().size() ||
          record.cold_length != current->cold_manifest_->encoded_bytes().size() ||
          record.manifest_sha256 != *manifest_digest || record.cold_sha256 != *cold_digest) {
        return common::make_unexpected(
            invalid("tiered remote reclamation pair differs from current publication"));
      }

      std::vector<ColdPartLocationDescriptor> retired_locations;
      retired_locations.reserve(part_ids.size());
      const auto current_locations = current->cold_manifest_->manifest().locations();
      for (const cseg::PartId& part_id : part_ids) {
        const auto current_route = std::ranges::lower_bound(current_locations, part_id, {},
                                                            &ColdPartLocationDescriptor::part_id);
        if (std::ranges::find(current->manifest_snapshot_.parts(), part_id,
                              &manifest::TemporalPartDescriptor::part_id) !=
                current->manifest_snapshot_.parts().end() ||
            (current_route != current_locations.end() && current_route->part_id == part_id)) {
          return common::make_unexpected(
              invalid("tiered remote reclamation candidate remains current authority"));
        }

        std::optional<ColdPartLocationDescriptor> candidate;
        for (const auto& weak : published_epochs_) {
          const std::shared_ptr<const TieredDatabaseStorageEpoch> epoch = weak.lock();
          if (epoch == nullptr || epoch->cold_manifest_ == nullptr)
            continue;
          const auto locations = epoch->cold_manifest_->manifest().locations();
          const auto route = std::ranges::lower_bound(locations, part_id, {},
                                                      &ColdPartLocationDescriptor::part_id);
          if (route == locations.end() || route->part_id != part_id)
            continue;
          const auto descriptor = std::ranges::find(epoch->manifest_snapshot_.parts(), part_id,
                                                    &manifest::TemporalPartDescriptor::part_id);
          if (descriptor == epoch->manifest_snapshot_.parts().end() ||
              descriptor->file_length != route->file_length ||
              descriptor->content_sha256 != route->content_sha256) {
            return common::make_unexpected(
                invalid("published retired route lost its logical byte identity"));
          }
          if (candidate.has_value() && *candidate != *route) {
            return common::make_unexpected(
                invalid("published history changes a retired object route"));
          }
          candidate = *route;
        }
        if (!candidate.has_value()) {
          return common::make_unexpected(
              invalid("tiered remote reclamation route is absent from publication history"));
        }
        if (std::ranges::any_of(current_locations,
                                [&](const auto& location) {
                                  return location.object_key == candidate->object_key;
                                }) ||
            std::ranges::any_of(retired_locations, [&](const auto& location) {
              return location.object_key == candidate->object_key;
            })) {
          return common::make_unexpected(
              invalid("tiered remote reclamation object key remains or is duplicated"));
        }
        retired_locations.push_back(std::move(*candidate));
      }

      std::vector<std::weak_ptr<const TieredDatabaseStorageEpoch>> reader_pins;
      reader_pins.reserve(published_epochs_.size());
      for (const auto& weak : published_epochs_) {
        const std::shared_ptr<const TieredDatabaseStorageEpoch> epoch = weak.lock();
        if (epoch == nullptr || epoch->cold_manifest_ == nullptr)
          continue;
        const auto locations = epoch->cold_manifest_->manifest().locations();
        bool references_retired_object = false;
        for (const ColdPartLocationDescriptor& candidate : retired_locations) {
          const auto route = std::ranges::lower_bound(locations, candidate.part_id, {},
                                                      &ColdPartLocationDescriptor::part_id);
          if (route != locations.end() && route->part_id == candidate.part_id) {
            if (*route != candidate) {
              return common::make_unexpected(
                  invalid("published reader route differs from retired object identity"));
            }
            references_retired_object = true;
          }
          const auto reused_key = std::ranges::find(locations, candidate.object_key,
                                                    &ColdPartLocationDescriptor::object_key);
          if (reused_key != locations.end() && *reused_key != candidate) {
            return common::make_unexpected(invalid("published reader reuses a retired object key"));
          }
        }
        if (references_retired_object)
          reader_pins.push_back(weak);
      }
      return TieredRemoteObjectReclamationProof{record, TieredDatabaseStorageSnapshot{current},
                                                std::move(retired_locations),
                                                std::move(reader_pins)};
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(
          exhausted("tiered remote reclamation authorization allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(
          exhausted("tiered remote reclamation authorization exceeded limits"));
    }
  }

private:
  std::shared_ptr<const TieredDatabaseStorageEpoch> epoch_;
  // Single-writer history. Entries are installed before release-publication; weak owners capture
  // exactly the epochs whose readers may still require local bytes.
  std::vector<std::weak_ptr<const TieredDatabaseStorageEpoch>> published_epochs_;
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

common::Result<TieredLocalPartReclamationProof>
TieredLocalPartReclamationCoordinator::authorize(const TieredDatabaseStoragePublisher& publisher,
                                                 const TieredPairCommitRecord& committed_pair,
                                                 const std::span<const cseg::PartId> part_ids) {
  if (publisher.impl_ == nullptr)
    return common::make_unexpected(invalid("tiered storage publisher was moved from"));
  return publisher.impl_->authorize_local_reclamation(committed_pair, part_ids);
}

common::Result<TieredRemoteObjectReclamationProof>
TieredRemoteObjectReclamationCoordinator::authorize(
    const TieredDatabaseStoragePublisher& publisher, const TieredPairCommitRecord& committed_pair,
    const std::span<const cseg::PartId> part_ids,
    const TieredRemoteObjectReclamationLimits limits) {
  if (publisher.impl_ == nullptr)
    return common::make_unexpected(invalid("tiered storage publisher was moved from"));
  return publisher.impl_->authorize_remote_reclamation(committed_pair, part_ids, limits);
}

} // namespace chronos::tiering
