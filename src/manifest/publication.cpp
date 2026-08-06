#include "chronos/manifest/publication.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/checked_math.hpp"

#include <algorithm>
#include <atomic>
#include <limits>
#include <new>
#include <optional>
#include <ranges>
#include <string>
#include <utility>

namespace chronos::manifest {
namespace detail {

class PublishedTabletStorageBuilder {
public:
  [[nodiscard]] static PublishedTabletStorage
  make(schema::TableId table_id, schema::TabletId tablet_id,
       std::optional<head::HeadCommitPosition> applied_position,
       std::vector<head::HeadSnapshot> sealed_heads, std::vector<head::HeadSnapshot> active_head,
       const std::size_t visible_head_rows) {
    return PublishedTabletStorage{table_id,
                                  tablet_id,
                                  applied_position,
                                  std::move(sealed_heads),
                                  std::move(active_head),
                                  visible_head_rows};
  }
};

} // namespace detail

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

[[nodiscard]] common::Status unavailable(std::string message) {
  return common::Status{common::StatusCode::kUnavailable, std::move(message)};
}

[[nodiscard]] const TabletDescriptor*
find_durable_tablet(const LoadedManifestGeneration& manifest,
                    const schema::TabletId& tablet_id) noexcept {
  const auto found =
      std::ranges::lower_bound(manifest.tablets(), tablet_id, {}, &TabletDescriptor::tablet_id);
  return found != manifest.tablets().end() && found->tablet_id == tablet_id ? &*found : nullptr;
}

[[nodiscard]] const PartDescriptor* find_part(const LoadedManifestGeneration& manifest,
                                              const cseg::PartId& part_id) noexcept {
  const auto found = std::ranges::find(manifest.parts(), part_id, &PartDescriptor::part_id);
  return found == manifest.parts().end() ? nullptr : &*found;
}

[[nodiscard]] const RetryDescriptor* find_retry(const LoadedManifestGeneration& manifest,
                                                const RetryDescriptor& retry) noexcept {
  const auto found =
      std::ranges::find_if(manifest.retries(), [&](const RetryDescriptor& candidate) {
        return candidate.client_id == retry.client_id &&
               candidate.client_batch_id == retry.client_batch_id;
      });
  return found == manifest.retries().end() ? nullptr : &*found;
}

[[nodiscard]] common::Status validate_head_identity(const head::HeadSnapshot& head,
                                                    const schema::TableId& table_id,
                                                    const schema::TabletId& tablet_id) {
  if (head.table_id() != table_id || head.tablet_id() != tablet_id) {
    return invalid("database publication head identity disagrees with its tablet epoch");
  }
  if (head.schema_ptr() == nullptr || head.schema_ptr()->table_id() != table_id) {
    return invalid("database publication head has no matching owning schema");
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status
validate_head_rows_after_durable(const head::HeadSnapshot& head, const wal::WalId& wal_id,
                                 const std::uint64_t durable_sequence) {
  for (std::uint32_t row = 0U; row < head.row_count(); ++row) {
    const common::Result<head::HeadRowMetadata> metadata = head.row_metadata(row);
    if (!metadata.has_value()) {
      return metadata.error();
    }
    if (metadata->commit_position.wal_id != wal_id ||
        metadata->commit_position.record_sequence <= durable_sequence) {
      return corruption(
          "query-visible head row is duplicated by or predates its durable Manifest boundary");
    }
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status add_visible_rows(std::size_t& total, const std::size_t rows) {
  const std::optional<std::size_t> next = common::checked_add(total, rows);
  if (!next.has_value()) {
    return exhausted("database publication visible row count overflows size_t");
  }
  total = *next;
  return common::Status::ok();
}

[[nodiscard]] common::Result<PublishedTabletStorage>
copy_tablet(const ingest::TabletSnapshot& snapshot, const LoadedManifestGeneration& manifest) {
  const TabletDescriptor* durable = find_durable_tablet(manifest, snapshot.tablet_id());
  if (durable != nullptr && durable->table_id != snapshot.table_id()) {
    return common::make_unexpected(
        corruption("Manifest and live tablet epochs disagree on table identity"));
  }
  const std::uint64_t durable_sequence = durable == nullptr ? 0U : durable->durable_record_sequence;
  const std::optional<head::HeadCommitPosition> applied_position = snapshot.applied_position();
  if (applied_position.has_value() && applied_position->wal_id != manifest.wal_id()) {
    return common::make_unexpected(invalid("live tablet epoch belongs to a different WAL history"));
  }

  std::vector<head::HeadSnapshot> sealed;
  std::vector<head::HeadSnapshot> active;
  sealed.reserve(snapshot.sealed_generations().size());
  std::size_t visible_rows = 0U;
  std::uint64_t previous_generation = 0U;
  for (const head::HeadSnapshot& head : snapshot.sealed_generations()) {
    common::Status status = validate_head_identity(head, snapshot.table_id(), snapshot.tablet_id());
    if (!status.is_ok()) {
      return common::make_unexpected(std::move(status));
    }
    if (!head.is_sealed() || head.generation() <= previous_generation) {
      return common::make_unexpected(
          invalid("sealed database publication heads must be sealed and generation ordered"));
    }
    status = validate_head_rows_after_durable(head, manifest.wal_id(), durable_sequence);
    if (!status.is_ok()) {
      return common::make_unexpected(std::move(status));
    }
    status = add_visible_rows(visible_rows, head.row_count());
    if (!status.is_ok()) {
      return common::make_unexpected(std::move(status));
    }
    previous_generation = head.generation();
    sealed.push_back(head);
  }
  const head::HeadSnapshot& active_head = snapshot.active_generation();
  common::Status status =
      validate_head_identity(active_head, snapshot.table_id(), snapshot.tablet_id());
  if (!status.is_ok()) {
    return common::make_unexpected(std::move(status));
  }
  if (active_head.generation() <= previous_generation) {
    return common::make_unexpected(
        invalid("active database publication head must follow every sealed generation"));
  }
  status = validate_head_rows_after_durable(active_head, manifest.wal_id(), durable_sequence);
  if (!status.is_ok()) {
    return common::make_unexpected(std::move(status));
  }
  status = add_visible_rows(visible_rows, active_head.row_count());
  if (!status.is_ok()) {
    return common::make_unexpected(std::move(status));
  }
  active.push_back(active_head);
  return detail::PublishedTabletStorageBuilder::make(snapshot.table_id(), snapshot.tablet_id(),
                                                     applied_position, std::move(sealed),
                                                     std::move(active), visible_rows);
}

[[nodiscard]] common::Status validate_head_prefix(const head::HeadSnapshot& previous,
                                                  const head::HeadSnapshot& next,
                                                  const bool require_exact_rows) {
  if (previous.table_id() != next.table_id() || previous.tablet_id() != next.tablet_id() ||
      previous.generation() != next.generation() ||
      previous.schema_ptr()->schema_id() != next.schema_ptr()->schema_id() ||
      (require_exact_rows ? previous.row_count() != next.row_count()
                          : previous.row_count() > next.row_count())) {
    return invalid("tablet publication regresses or changes a visible head generation");
  }
  for (std::uint32_t row = 0U; row < previous.row_count(); ++row) {
    const common::Result<head::RowVersionIdentity> old_identity =
        previous.row_version_identity(row);
    const common::Result<head::RowVersionIdentity> new_identity = next.row_version_identity(row);
    if (!old_identity.has_value() || !new_identity.has_value()) {
      return corruption("tablet publication cannot inspect a retained head row identity");
    }
    if (*old_identity != *new_identity) {
      return corruption("tablet publication changes a retained head row identity");
    }
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status validate_tablet_successor(const PublishedTabletStorage& previous,
                                                       const PublishedTabletStorage& next) {
  if (previous.table_id() != next.table_id() || previous.tablet_id() != next.tablet_id()) {
    return invalid("tablet publication changes table or tablet identity");
  }
  const std::optional<head::HeadCommitPosition> previous_position = previous.applied_position();
  const std::optional<head::HeadCommitPosition> next_position = next.applied_position();
  if (previous_position.has_value()) {
    if (!next_position.has_value() || next_position->wal_id != previous_position->wal_id ||
        next_position->record_sequence < previous_position->record_sequence) {
      return invalid("tablet publication regresses its applied WAL position");
    }
  }
  for (const head::HeadSnapshot& old_sealed : previous.sealed_heads()) {
    const auto retained =
        std::ranges::find_if(next.sealed_heads(), [&](const head::HeadSnapshot& candidate) {
          return candidate.generation() == old_sealed.generation();
        });
    if (retained == next.sealed_heads().end()) {
      return invalid("tablet publication drops a visible sealed generation");
    }
    common::Status status = validate_head_prefix(old_sealed, *retained, true);
    if (!status.is_ok()) {
      return status;
    }
  }
  const head::HeadSnapshot* old_active = previous.active_head();
  const head::HeadSnapshot* new_active = next.active_head();
  if (old_active == nullptr || new_active == nullptr ||
      new_active->generation() < old_active->generation()) {
    return invalid("tablet publication drops or regresses its active generation");
  }
  if (new_active->generation() == old_active->generation()) {
    return validate_head_prefix(*old_active, *new_active, false);
  }
  const auto sealed_successor =
      std::ranges::find_if(next.sealed_heads(), [&](const head::HeadSnapshot& candidate) {
        return candidate.generation() == old_active->generation();
      });
  if (sealed_successor == next.sealed_heads().end()) {
    return invalid("tablet rotation does not retain the previous active generation");
  }
  return validate_head_prefix(*old_active, *sealed_successor, false);
}

[[nodiscard]] common::Status validate_manifest_successor(const LoadedManifestGeneration& current,
                                                         const LoadedManifestGeneration& next) {
  if (next.database_id() != current.database_id() || next.wal_id() != current.wal_id()) {
    return corruption("durable Manifest successor changes database or WAL identity");
  }
  if (current.generation() == std::numeric_limits<std::uint64_t>::max() ||
      next.generation() != current.generation() + 1U ||
      next.previous_generation() != current.generation()) {
    return corruption("durable Manifest publication is not the exact next generation");
  }
  if (next.reclaim_checkpoint().record_sequence < current.reclaim_checkpoint().record_sequence) {
    return corruption("durable Manifest successor regresses its WAL checkpoint");
  }
  for (const TabletDescriptor& tablet : current.tablets()) {
    const TabletDescriptor* retained = find_durable_tablet(next, tablet.tablet_id);
    if (retained == nullptr || retained->table_id != tablet.table_id ||
        retained->durable_record_sequence < tablet.durable_record_sequence ||
        retained->durable_row_count < tablet.durable_row_count) {
      return corruption("durable Manifest successor drops or regresses a tablet");
    }
  }
  for (const PartDescriptor& part : current.parts()) {
    const PartDescriptor* retained = find_part(next, part.part_id);
    if (retained == nullptr || *retained != part) {
      return corruption("durable Manifest successor drops or changes an installed part");
    }
  }
  for (const RetryDescriptor& retry : current.retries()) {
    const RetryDescriptor* retained = find_retry(next, retry);
    if (retained == nullptr || *retained != retry) {
      return corruption("durable Manifest successor drops or changes a protected retry outcome");
    }
  }
  return common::Status::ok();
}

struct HeadBounds {
  std::uint64_t minimum_sequence{std::numeric_limits<std::uint64_t>::max()};
  std::uint64_t maximum_sequence{};
  std::int64_t minimum_event_time{std::numeric_limits<std::int64_t>::max()};
  std::int64_t maximum_event_time{std::numeric_limits<std::int64_t>::min()};
};

[[nodiscard]] common::Result<HeadBounds> head_bounds(const head::HeadSnapshot& snapshot,
                                                     const wal::WalId& wal_id) {
  const std::optional<std::size_t> event_ordinal =
      snapshot.schema_ptr()->column_ordinal(snapshot.schema_ptr()->event_time_column());
  if (!event_ordinal.has_value()) {
    return common::make_unexpected(corruption("sealed replacement head has no event-time column"));
  }
  const common::Result<head::HeadColumnView> event_column = snapshot.column(*event_ordinal);
  if (!event_column.has_value()) {
    return common::make_unexpected(event_column.error());
  }
  HeadBounds bounds;
  for (std::uint32_t row = 0U; row < snapshot.row_count(); ++row) {
    const common::Result<head::HeadRowMetadata> metadata = snapshot.row_metadata(row);
    if (!metadata.has_value()) {
      return common::make_unexpected(metadata.error());
    }
    if (metadata->commit_position.wal_id != wal_id) {
      return common::make_unexpected(
          corruption("sealed replacement head belongs to a different WAL history"));
    }
    bounds.minimum_sequence =
        std::min(bounds.minimum_sequence, metadata->commit_position.record_sequence);
    bounds.maximum_sequence =
        std::max(bounds.maximum_sequence, metadata->commit_position.record_sequence);
    const common::Result<head::HeadCellView> cell = event_column->cell(row);
    if (!cell.has_value() || cell->is_null()) {
      return common::make_unexpected(
          corruption("sealed replacement head has an inaccessible event-time value"));
    }
    const common::Result<common::ByteView> bytes = cell->bytes();
    if (!bytes.has_value()) {
      return common::make_unexpected(bytes.error());
    }
    common::ByteReader reader{*bytes};
    const common::Result<std::int64_t> value = reader.read_i64_le();
    if (!value.has_value() || !reader.empty()) {
      return common::make_unexpected(
          corruption("sealed replacement head has an invalid event-time width"));
    }
    bounds.minimum_event_time = std::min(bounds.minimum_event_time, *value);
    bounds.maximum_event_time = std::max(bounds.maximum_event_time, *value);
  }
  return bounds;
}

[[nodiscard]] bool same_replacement_identity(const SealedHeadReplacement& left,
                                             const SealedHeadReplacement& right) noexcept {
  return left.tablet_id == right.tablet_id && left.head_generation == right.head_generation;
}

} // namespace

namespace detail {

class DatabaseStoragePublication {
public:
  DatabaseStoragePublication(std::shared_ptr<const LoadedManifestGeneration> manifest,
                             std::vector<PublishedTabletStorage> tablets,
                             std::vector<ingest::SealedGenerationRetirementReceipt> retirements,
                             const std::size_t visible_head_rows) noexcept
      : manifest_(std::move(manifest)), tablets_(std::move(tablets)),
        retirements_(std::move(retirements)), visible_head_rows_(visible_head_rows) {}

  std::shared_ptr<const LoadedManifestGeneration> manifest_;
  std::vector<PublishedTabletStorage> tablets_;
  std::vector<ingest::SealedGenerationRetirementReceipt> retirements_;
  std::size_t visible_head_rows_{};
};

class DatabaseStoragePublisherImpl {
public:
  DatabaseStoragePublisherImpl(std::shared_ptr<const DatabaseStoragePublication> publication,
                               void (*hook)(void*) noexcept, void* hook_context) noexcept
      : publication_(std::move(publication)), hook_(hook), hook_context_(hook_context) {}

  [[nodiscard]] common::Result<DatabaseStorageSnapshot> snapshot() const {
    if (failed_.load(std::memory_order_acquire)) {
      return common::make_unexpected(unavailable("database storage publisher is failed closed"));
    }
    return DatabaseStorageSnapshot{
        std::atomic_load_explicit(&publication_, std::memory_order_acquire)};
  }

  [[nodiscard]] common::Result<DatabaseStorageSnapshot>
  publish_tablet_snapshot(const ingest::TabletSnapshot& tablet) {
    if (failed_.load(std::memory_order_acquire)) {
      return common::make_unexpected(unavailable("database storage publisher is failed closed"));
    }
    const std::shared_ptr<const DatabaseStoragePublication> current =
        std::atomic_load_explicit(&publication_, std::memory_order_acquire);
    try {
      common::Result<PublishedTabletStorage> copied = copy_tablet(tablet, *current->manifest_);
      if (!copied.has_value()) {
        return common::make_unexpected(copied.error());
      }
      std::vector<PublishedTabletStorage> tablets = current->tablets_;
      const auto found = std::ranges::lower_bound(tablets, tablet.tablet_id(), {},
                                                  &PublishedTabletStorage::tablet_id);
      if (found != tablets.end() && found->tablet_id() == tablet.tablet_id()) {
        common::Status transition = validate_tablet_successor(*found, *copied);
        if (!transition.is_ok()) {
          return common::make_unexpected(std::move(transition));
        }
        *found = std::move(*copied);
      } else {
        tablets.insert(found, std::move(*copied));
      }
      std::size_t visible_rows = 0U;
      for (const PublishedTabletStorage& value : tablets) {
        common::Status status = add_visible_rows(visible_rows, value.visible_head_row_count());
        if (!status.is_ok()) {
          return common::make_unexpected(std::move(status));
        }
      }
      auto next = std::make_shared<const DatabaseStoragePublication>(
          current->manifest_, std::move(tablets), current->retirements_, visible_rows);
      if (hook_ != nullptr) {
        hook_(hook_context_);
      }
      std::atomic_store_explicit(&publication_, next, std::memory_order_release);
      return DatabaseStorageSnapshot{std::move(next)};
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(exhausted("database tablet publication allocation failed"));
    } catch (const std::length_error&) {
      return common::make_unexpected(exhausted("database tablet publication exceeds limits"));
    }
  }

  [[nodiscard]] common::Result<DatabaseStorageSnapshot>
  publish_manifest(const DurableManifestPublicationRequest& request) {
    if (failed_.load(std::memory_order_acquire)) {
      return common::make_unexpected(unavailable("database storage publisher is failed closed"));
    }
    if (request.selected_manifest == nullptr) {
      return common::make_unexpected(invalid("durable Manifest publication requires an owner"));
    }
    const std::shared_ptr<const DatabaseStoragePublication> current =
        std::atomic_load_explicit(&publication_, std::memory_order_acquire);
    auto fail = [&](common::Status status) -> common::Result<DatabaseStorageSnapshot> {
      failed_.store(true, std::memory_order_release);
      return common::make_unexpected(std::move(status));
    };
    try {
      common::Status status =
          validate_manifest_successor(*current->manifest_, *request.selected_manifest);
      if (!status.is_ok()) {
        return fail(std::move(status));
      }
      for (std::size_t index = 0U; index < request.replacements.size(); ++index) {
        if (request.replacements[index].head_generation == 0U) {
          return fail(invalid("sealed-head replacement generation must be nonzero"));
        }
        for (std::size_t other = index + 1U; other < request.replacements.size(); ++other) {
          if (same_replacement_identity(request.replacements[index], request.replacements[other]) ||
              request.replacements[index].replacement_part_id ==
                  request.replacements[other].replacement_part_id) {
            return fail(invalid("sealed-head replacements contain a duplicate identity"));
          }
        }
      }

      std::vector<PublishedTabletStorage> tablets = current->tablets_;
      std::vector<cseg::PartId> matched_new_parts;
      matched_new_parts.reserve(request.replacements.size());
      std::vector<ingest::SealedGenerationRetirementReceipt> retirement_receipts;
      retirement_receipts.reserve(request.replacements.size());
      for (const SealedHeadReplacement& replacement : request.replacements) {
        auto tablet = std::ranges::lower_bound(tablets, replacement.tablet_id, {},
                                               &PublishedTabletStorage::tablet_id);
        if (tablet == tablets.end() || tablet->tablet_id() != replacement.tablet_id) {
          return fail(invalid("sealed-head replacement names an unpublished tablet"));
        }
        const auto sealed =
            std::ranges::find_if(tablet->sealed_heads_, [&](const head::HeadSnapshot& candidate) {
              return candidate.generation() == replacement.head_generation;
            });
        if (sealed == tablet->sealed_heads_.end() || !sealed->is_sealed() ||
            sealed->row_count() == 0U) {
          return fail(invalid("sealed-head replacement does not name a nonempty sealed head"));
        }
        if (find_part(*current->manifest_, replacement.replacement_part_id) != nullptr) {
          return fail(invalid("sealed-head replacement part was already selected"));
        }
        const PartDescriptor* part =
            find_part(*request.selected_manifest, replacement.replacement_part_id);
        if (part == nullptr || part->table_id != sealed->table_id() ||
            part->tablet_id != sealed->tablet_id() ||
            part->schema_id != sealed->schema_ptr()->schema_id() ||
            part->schema_version != sealed->schema_ptr()->version() ||
            part->row_count != sealed->row_count()) {
          return fail(corruption("replacement part descriptor disagrees with its sealed head"));
        }
        const common::Result<HeadBounds> bounds =
            head_bounds(*sealed, current->manifest_->wal_id());
        if (!bounds.has_value()) {
          return fail(bounds.error());
        }
        if (part->minimum_record_sequence != bounds->minimum_sequence ||
            part->maximum_record_sequence != bounds->maximum_sequence ||
            part->minimum_event_time != bounds->minimum_event_time ||
            part->maximum_event_time != bounds->maximum_event_time) {
          return fail(corruption("replacement part bounds disagree with its sealed head"));
        }
        retirement_receipts.push_back(ingest::SealedGenerationRetirementReceipt{
            ingest::SealedGenerationRetirementReceipt::Fields{
                .table_id = sealed->table_id(),
                .tablet_id = sealed->tablet_id(),
                .schema_id = sealed->schema_ptr()->schema_id(),
                .schema_version = sealed->schema_ptr()->version(),
                .head_generation = sealed->generation(),
                .row_count = sealed->row_count(),
                .wal_id = current->manifest_->wal_id(),
                .minimum_record_sequence = bounds->minimum_sequence,
                .maximum_record_sequence = bounds->maximum_sequence}});
        matched_new_parts.push_back(part->part_id);
        tablet->visible_head_rows_ -= sealed->row_count();
        tablet->sealed_heads_.erase(sealed);
      }

      for (const PartDescriptor& part : request.selected_manifest->parts()) {
        if (find_part(*current->manifest_, part.part_id) == nullptr &&
            !std::ranges::contains(matched_new_parts, part.part_id)) {
          return fail(corruption("durable Manifest adds a part without a sealed-head replacement"));
        }
      }
      if (matched_new_parts.size() + current->manifest_->parts().size() !=
          request.selected_manifest->parts().size()) {
        return fail(corruption("sealed-head replacements do not exactly cover new parts"));
      }

      for (const PublishedTabletStorage& tablet : tablets) {
        const TabletDescriptor* previous =
            find_durable_tablet(*current->manifest_, tablet.tablet_id());
        const TabletDescriptor* next =
            find_durable_tablet(*request.selected_manifest, tablet.tablet_id());
        const std::uint64_t previous_rows = previous == nullptr ? 0U : previous->durable_row_count;
        std::uint64_t replaced_rows = 0U;
        for (const SealedHeadReplacement& replacement : request.replacements) {
          if (replacement.tablet_id == tablet.tablet_id()) {
            const PartDescriptor* part =
                find_part(*request.selected_manifest, replacement.replacement_part_id);
            const std::optional<std::uint64_t> next_rows =
                common::checked_add(replaced_rows, part->row_count);
            if (!next_rows.has_value()) {
              return fail(corruption("replacement durable row count overflows uint64"));
            }
            replaced_rows = *next_rows;
          }
        }
        const std::optional<std::uint64_t> expected_rows =
            common::checked_add(previous_rows, replaced_rows);
        if (replaced_rows != 0U && (next == nullptr || !expected_rows.has_value() ||
                                    next->durable_row_count != *expected_rows)) {
          return fail(corruption("replacement rows disagree with the new tablet durable count"));
        }
        const std::uint64_t durable_sequence = next == nullptr ? 0U : next->durable_record_sequence;
        for (const head::HeadSnapshot& head : tablet.sealed_heads_) {
          status = validate_head_rows_after_durable(head, request.selected_manifest->wal_id(),
                                                    durable_sequence);
          if (!status.is_ok()) {
            return fail(std::move(status));
          }
        }
        if (!tablet.active_head_.empty()) {
          status = validate_head_rows_after_durable(
              tablet.active_head_.front(), request.selected_manifest->wal_id(), durable_sequence);
          if (!status.is_ok()) {
            return fail(std::move(status));
          }
        }
      }

      std::size_t visible_rows = 0U;
      for (const PublishedTabletStorage& tablet : tablets) {
        status = add_visible_rows(visible_rows, tablet.visible_head_row_count());
        if (!status.is_ok()) {
          return fail(std::move(status));
        }
      }
      auto next = std::make_shared<const DatabaseStoragePublication>(
          request.selected_manifest, std::move(tablets), std::move(retirement_receipts),
          visible_rows);
      if (hook_ != nullptr) {
        hook_(hook_context_);
      }
      std::atomic_store_explicit(&publication_, next, std::memory_order_release);
      return DatabaseStorageSnapshot{std::move(next)};
    } catch (const std::bad_alloc&) {
      return fail(exhausted("durable Manifest publication allocation failed"));
    } catch (const std::length_error&) {
      return fail(exhausted("durable Manifest publication exceeds limits"));
    }
  }

  [[nodiscard]] bool is_usable() const noexcept {
    return !failed_.load(std::memory_order_acquire);
  }

private:
  std::shared_ptr<const DatabaseStoragePublication> publication_;
  std::atomic<bool> failed_{false};
  void (*hook_)(void*) noexcept {};
  void* hook_context_{};
};

} // namespace detail

PublishedTabletStorage::PublishedTabletStorage(
    schema::TableId table_id, schema::TabletId tablet_id,
    std::optional<head::HeadCommitPosition> applied_position,
    std::vector<head::HeadSnapshot> sealed_heads, std::vector<head::HeadSnapshot> active_head,
    const std::size_t visible_head_rows) noexcept
    : table_id_(table_id), tablet_id_(tablet_id), applied_position_(applied_position),
      sealed_heads_(std::move(sealed_heads)), active_head_(std::move(active_head)),
      visible_head_rows_(visible_head_rows) {}

const schema::TableId& PublishedTabletStorage::table_id() const noexcept {
  return table_id_;
}

const schema::TabletId& PublishedTabletStorage::tablet_id() const noexcept {
  return tablet_id_;
}

const std::optional<head::HeadCommitPosition>&
PublishedTabletStorage::applied_position() const noexcept {
  return applied_position_;
}

std::span<const head::HeadSnapshot> PublishedTabletStorage::sealed_heads() const noexcept {
  return sealed_heads_;
}

const head::HeadSnapshot* PublishedTabletStorage::active_head() const noexcept {
  return active_head_.empty() ? nullptr : &active_head_.front();
}

std::size_t PublishedTabletStorage::visible_head_row_count() const noexcept {
  return visible_head_rows_;
}

DatabaseStorageSnapshot::DatabaseStorageSnapshot(
    std::shared_ptr<const detail::DatabaseStoragePublication> publication) noexcept
    : publication_(std::move(publication)) {}

std::uint64_t DatabaseStorageSnapshot::generation() const noexcept {
  return publication_->manifest_->generation();
}

const DatabaseId& DatabaseStorageSnapshot::database_id() const noexcept {
  return publication_->manifest_->database_id();
}

const wal::WalId& DatabaseStorageSnapshot::wal_id() const noexcept {
  return publication_->manifest_->wal_id();
}

const WalCheckpoint& DatabaseStorageSnapshot::reclaim_checkpoint() const noexcept {
  return publication_->manifest_->reclaim_checkpoint();
}

common::ByteView DatabaseStorageSnapshot::manifest_bytes() const noexcept {
  return publication_->manifest_->encoded_bytes();
}

std::span<const TabletDescriptor> DatabaseStorageSnapshot::durable_tablets() const noexcept {
  return publication_->manifest_->tablets();
}

std::span<const PartDescriptor> DatabaseStorageSnapshot::parts() const noexcept {
  return publication_->manifest_->parts();
}

std::span<const RetryDescriptor> DatabaseStorageSnapshot::retries() const noexcept {
  return publication_->manifest_->retries();
}

std::span<const PublishedTabletStorage> DatabaseStorageSnapshot::tablets() const noexcept {
  return publication_->tablets_;
}

std::span<const ingest::SealedGenerationRetirementReceipt>
DatabaseStorageSnapshot::retirement_receipts() const noexcept {
  return publication_->retirements_;
}

const PublishedTabletStorage*
DatabaseStorageSnapshot::find_tablet(const schema::TabletId& tablet_id) const noexcept {
  const auto found = std::ranges::lower_bound(publication_->tablets_, tablet_id, {},
                                              &PublishedTabletStorage::tablet_id);
  return found != publication_->tablets_.end() && found->tablet_id() == tablet_id ? &*found
                                                                                  : nullptr;
}

std::size_t DatabaseStorageSnapshot::visible_head_row_count() const noexcept {
  return publication_->visible_head_rows_;
}

DatabaseStoragePublisher::DatabaseStoragePublisher(
    std::unique_ptr<detail::DatabaseStoragePublisherImpl> implementation) noexcept
    : implementation_(std::move(implementation)) {}

DatabaseStoragePublisher::~DatabaseStoragePublisher() = default;
DatabaseStoragePublisher::DatabaseStoragePublisher(DatabaseStoragePublisher&&) noexcept = default;
DatabaseStoragePublisher&
DatabaseStoragePublisher::operator=(DatabaseStoragePublisher&&) noexcept = default;

common::Result<DatabaseStoragePublisher>
DatabaseStoragePublisher::create(std::shared_ptr<const LoadedManifestGeneration> selected_manifest,
                                 const std::span<const DatabaseStorageTabletInput> tablets) {
  return create_with_publication_hook(std::move(selected_manifest), tablets, nullptr, nullptr);
}

common::Result<DatabaseStoragePublisher> DatabaseStoragePublisher::create_with_publication_hook(
    std::shared_ptr<const LoadedManifestGeneration> selected_manifest,
    const std::span<const DatabaseStorageTabletInput> tablets, const PublicationHook hook,
    void* const hook_context) {
  if (selected_manifest == nullptr) {
    return common::make_unexpected(invalid("database storage publisher requires a Manifest owner"));
  }
  try {
    std::vector<PublishedTabletStorage> copied;
    copied.reserve(tablets.size());
    for (const DatabaseStorageTabletInput& input : tablets) {
      common::Result<PublishedTabletStorage> tablet =
          copy_tablet(input.snapshot.get(), *selected_manifest);
      if (!tablet.has_value()) {
        return common::make_unexpected(tablet.error());
      }
      copied.push_back(std::move(*tablet));
    }
    std::ranges::sort(copied, {}, &PublishedTabletStorage::tablet_id);
    for (std::size_t index = 1U; index < copied.size(); ++index) {
      if (copied[index - 1U].tablet_id() == copied[index].tablet_id()) {
        return common::make_unexpected(invalid("database storage publication repeats a tablet"));
      }
    }
    std::size_t visible_rows = 0U;
    for (const PublishedTabletStorage& tablet : copied) {
      common::Status status = add_visible_rows(visible_rows, tablet.visible_head_row_count());
      if (!status.is_ok()) {
        return common::make_unexpected(std::move(status));
      }
    }
    auto publication = std::make_shared<const detail::DatabaseStoragePublication>(
        std::move(selected_manifest), std::move(copied),
        std::vector<ingest::SealedGenerationRetirementReceipt>{}, visible_rows);
    auto implementation = std::make_unique<detail::DatabaseStoragePublisherImpl>(
        std::move(publication), hook, hook_context);
    return DatabaseStoragePublisher{std::move(implementation)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("database storage publisher allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("database storage publication exceeds limits"));
  }
}

common::Result<DatabaseStorageSnapshot> DatabaseStoragePublisher::snapshot() const {
  if (implementation_ == nullptr) {
    return common::make_unexpected(unavailable("database storage publisher was moved from"));
  }
  return implementation_->snapshot();
}

common::Result<DatabaseStorageSnapshot>
DatabaseStoragePublisher::publish_tablet_snapshot(const ingest::TabletSnapshot& tablet) {
  if (implementation_ == nullptr) {
    return common::make_unexpected(unavailable("database storage publisher was moved from"));
  }
  return implementation_->publish_tablet_snapshot(tablet);
}

common::Result<DatabaseStorageSnapshot>
DatabaseStoragePublisher::publish_manifest(const DurableManifestPublicationRequest& request) {
  if (implementation_ == nullptr) {
    return common::make_unexpected(unavailable("database storage publisher was moved from"));
  }
  return implementation_->publish_manifest(request);
}

bool DatabaseStoragePublisher::is_usable() const noexcept {
  return implementation_ != nullptr && implementation_->is_usable();
}

common::Status DatabaseStoragePublisher::poison_status() const {
  return is_usable() ? common::Status::ok()
                     : unavailable("database storage publisher is failed closed or unavailable");
}

} // namespace chronos::manifest
