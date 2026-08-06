#include "chronos/manifest/generation_builder.hpp"

#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/checked_math.hpp"
#include "chronos/cseg/format.hpp"
#include "chronos/cseg/part_codec.hpp"
#include "chronos/manifest/naming.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::manifest {
namespace {

[[nodiscard]] common::Status status(const common::StatusCode code, const std::string_view message) {
  return common::Status{code, std::string{message}};
}

[[nodiscard]] common::Status invalid(const std::string_view message) {
  return status(common::StatusCode::kInvalidArgument, message);
}

[[nodiscard]] common::Status corruption(const std::string_view message) {
  return status(common::StatusCode::kCorruption, message);
}

[[nodiscard]] common::Status exhausted(const std::string_view message) {
  return status(common::StatusCode::kResourceExhausted, message);
}

[[nodiscard]] bool retry_less(const RetryDescriptor& left, const RetryDescriptor& right) noexcept {
  if (left.client_id != right.client_id) {
    return left.client_id < right.client_id;
  }
  return left.client_batch_id < right.client_batch_id;
}

[[nodiscard]] common::Result<common::ByteView>
cell_bytes(const columnar::PhysicalColumnView& column, const std::uint32_t row) {
  const common::Result<columnar::ColumnCellView> cell = column.cell(row);
  if (!cell.has_value() || cell->is_null()) {
    return common::make_unexpected(
        corruption("Validated sealed CSEG record-sequence cell is inaccessible or null"));
  }
  const common::Result<common::ByteView> bytes = cell->bytes();
  if (!bytes.has_value()) {
    return common::make_unexpected(
        corruption("Validated sealed CSEG record-sequence cell is not byte-valued"));
  }
  return *bytes;
}

[[nodiscard]] common::Result<std::uint64_t> load_u64(const common::ByteView bytes) {
  if (bytes.size() != sizeof(std::uint64_t)) {
    return common::make_unexpected(
        corruption("Validated sealed CSEG record-sequence cell has an invalid width"));
  }
  std::uint64_t value = 0U;
  for (std::size_t index = 0U; index < bytes.size(); ++index) {
    value |= static_cast<std::uint64_t>(std::to_integer<std::uint8_t>(bytes[index]))
             << (index * 8U);
  }
  return value;
}

struct RecordCount {
  std::uint64_t sequence{};
  std::uint32_t rows{};
};

[[nodiscard]] common::Result<std::vector<RecordCount>>
record_counts(const cseg::DecodedCsegPartView& part) {
  std::vector<std::uint64_t> records;
  if (part.metadata().row_count() > std::numeric_limits<std::size_t>::max()) {
    return common::make_unexpected(exhausted("Sealed CSEG row count does not fit this platform"));
  }
  records.reserve(static_cast<std::size_t>(part.metadata().row_count()));
  const std::size_t user_count =
      part.metadata().columns().size() - cseg::format::kSystemColumnCount;
  for (const cseg::CsegGranuleDescriptor& granule : part.metadata().granules()) {
    common::Result<cseg::DecodedCsegPage> page =
        part.decode_page(static_cast<std::size_t>(granule.first_page_index) + user_count + 1U);
    if (!page.has_value()) {
      return common::make_unexpected(
          corruption("Validated sealed CSEG record-sequence page no longer decodes"));
    }
    for (std::uint32_t row = 0U; row < granule.row_count; ++row) {
      const common::Result<common::ByteView> bytes = cell_bytes(page->physical(), row);
      if (!bytes.has_value()) {
        return common::make_unexpected(bytes.error());
      }
      const common::Result<std::uint64_t> sequence = load_u64(*bytes);
      if (!sequence.has_value()) {
        return common::make_unexpected(sequence.error());
      }
      records.push_back(*sequence);
    }
  }
  std::ranges::sort(records);
  std::vector<RecordCount> counts;
  counts.reserve(records.size());
  for (const std::uint64_t sequence : records) {
    if (counts.empty() || counts.back().sequence != sequence) {
      counts.push_back({.sequence = sequence, .rows = 1U});
    } else if (counts.back().rows == std::numeric_limits<std::uint32_t>::max()) {
      return common::make_unexpected(
          exhausted("Sealed CSEG rows for one WAL record exceed the retry format"));
    } else {
      ++counts.back().rows;
    }
  }
  return counts;
}

[[nodiscard]] const TabletSchemaBinding*
find_binding(const std::span<const TabletSchemaBinding> bindings,
             const schema::TabletId& tablet_id) noexcept {
  const auto found =
      std::ranges::lower_bound(bindings, tablet_id, {}, [](const TabletSchemaBinding& binding) {
        return binding.tablet_id;
      });
  return found != bindings.end() && found->tablet_id == tablet_id ? &*found : nullptr;
}

[[nodiscard]] common::Status
validate_binding_order(const std::span<const TabletSchemaBinding> bindings) {
  for (std::size_t index = 1U; index < bindings.size(); ++index) {
    if (!(bindings[index - 1U].tablet_id < bindings[index].tablet_id)) {
      return invalid("Manifest schema bindings are not strictly sorted by tablet identity");
    }
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status validate_new_retries(const std::span<const RetryDescriptor> retries,
                                                  const std::span<const RecordCount> records,
                                                  const PartDescriptor& part,
                                                  const wal::WalId& wal_id) {
  if (retries.size() != records.size()) {
    return invalid("Sealed manifest retries do not exactly cover represented WAL records");
  }
  std::vector<const RetryDescriptor*> by_sequence;
  by_sequence.reserve(retries.size());
  for (const RetryDescriptor& retry : retries) {
    if (retry.table_id != part.table_id || retry.tablet_id != part.tablet_id ||
        retry.wal_id != wal_id) {
      return invalid("Sealed manifest retry target or WAL identity disagrees with its part");
    }
    by_sequence.push_back(&retry);
  }
  std::ranges::sort(by_sequence, {},
                    [](const RetryDescriptor* retry) { return retry->record_sequence; });
  for (std::size_t index = 0U; index < records.size(); ++index) {
    if (by_sequence[index]->record_sequence != records[index].sequence ||
        by_sequence[index]->applied_row_count != records[index].rows) {
      return invalid("Sealed manifest retry sequence or row count disagrees with its CSEG rows");
    }
    if (index != 0U &&
        by_sequence[index - 1U]->record_sequence == by_sequence[index]->record_sequence) {
      return invalid("Sealed manifest contains duplicate retry record sequences");
    }
  }
  std::vector<const RetryDescriptor*> by_identity;
  by_identity.reserve(retries.size());
  for (const RetryDescriptor& retry : retries) {
    by_identity.push_back(&retry);
  }
  std::ranges::sort(by_identity, {}, [](const RetryDescriptor* retry) {
    return std::pair{retry->client_id, retry->client_batch_id};
  });
  for (std::size_t index = 1U; index < by_identity.size(); ++index) {
    if (by_identity[index - 1U]->client_id == by_identity[index]->client_id &&
        by_identity[index - 1U]->client_batch_id == by_identity[index]->client_batch_id) {
      return invalid("Sealed manifest contains duplicate retry identities");
    }
  }
  return common::Status::ok();
}

[[nodiscard]] common::Result<EncodedManifest> build(const SealedHeadManifestBuildInput& input) {
  const DecodedManifestView& predecessor = input.predecessor.get();
  const EncodedSealedHeadPart& sealed = input.sealed_part.get();
  const PartDescriptor& part = sealed.descriptor;
  const std::optional<std::uint64_t> generation =
      common::checked_add(predecessor.generation(), std::uint64_t{1U});
  if (!generation.has_value()) {
    return common::make_unexpected(exhausted("Manifest generation overflowed"));
  }
  const common::Status binding_order = validate_binding_order(input.schema_bindings);
  if (!binding_order.is_ok()) {
    return common::make_unexpected(binding_order);
  }
  if (sealed.wal_id != predecessor.wal_id()) {
    return common::make_unexpected(
        invalid("Sealed CSEG WAL identity disagrees with its predecessor manifest"));
  }
  const TabletSchemaBinding* binding = find_binding(input.schema_bindings, part.tablet_id);
  if (binding == nullptr || binding->lineage.get().table_id() != part.table_id) {
    return common::make_unexpected(
        invalid("Sealed CSEG tablet is missing its exact schema lineage binding"));
  }
  const std::shared_ptr<const schema::TableSchema> schema_value =
      binding->lineage.get().find(part.schema_id);
  if (!schema_value || schema_value->version() != part.schema_version) {
    return common::make_unexpected(
        invalid("Sealed CSEG schema identity or version is absent from its lineage"));
  }
  const common::Result<std::string> file_name = part_file_name(part.part_id);
  if (!file_name.has_value()) {
    return common::make_unexpected(file_name.error());
  }
  const common::Status part_status = validate_manifest_v1_part_image(
      part, predecessor.wal_id(), *schema_value,
      {.file_name = *file_name, .bytes = sealed.encoded_part.bytes()},
      input.part_validation_limits);
  if (!part_status.is_ok()) {
    return common::make_unexpected(part_status);
  }
  cseg::CsegPartDecodeResult decoded = cseg::decode_cseg_v1_part_exact(
      sealed.encoded_part.bytes(), input.part_validation_limits.decode);
  if (!decoded.has_value()) {
    return common::make_unexpected(
        corruption("Validated sealed CSEG part no longer decodes exactly"));
  }
  const common::Result<std::vector<RecordCount>> records = record_counts(*decoded);
  if (!records.has_value()) {
    return common::make_unexpected(records.error());
  }
  const common::Status retry_status =
      validate_new_retries(input.new_retries, *records, part, predecessor.wal_id());
  if (!retry_status.is_ok()) {
    return common::make_unexpected(retry_status);
  }

  std::vector<TabletDescriptor> tablets(predecessor.tablets().begin(), predecessor.tablets().end());
  auto target = std::ranges::lower_bound(
      tablets, part.tablet_id, {}, [](const TabletDescriptor& tablet) { return tablet.tablet_id; });
  if (target != tablets.end() && target->tablet_id == part.tablet_id) {
    if (target->table_id != part.table_id) {
      return common::make_unexpected(invalid("Sealed CSEG changed its tablet table identity"));
    }
    if (part.minimum_record_sequence <= target->durable_record_sequence) {
      return common::make_unexpected(
          invalid("Sealed CSEG overlaps its tablet durable record boundary"));
    }
    const std::optional<std::uint64_t> rows =
        common::checked_add(target->durable_row_count, part.row_count);
    if (!rows.has_value()) {
      return common::make_unexpected(exhausted("Manifest durable row count overflowed"));
    }
    target->recovery_schema_id = part.schema_id;
    target->recovery_schema_version = part.schema_version;
    target->durable_record_sequence = part.maximum_record_sequence;
    target->durable_row_count = *rows;
  } else {
    tablets.insert(target, TabletDescriptor{
                               .table_id = part.table_id,
                               .tablet_id = part.tablet_id,
                               .recovery_schema_id = part.schema_id,
                               .recovery_schema_version = part.schema_version,
                               .durable_record_sequence = part.maximum_record_sequence,
                               .first_part_index = 0U,
                               .part_count = 0U,
                               .durable_row_count = part.row_count,
                           });
  }

  std::vector<PartDescriptor> parts;
  const std::optional<std::size_t> part_capacity =
      common::checked_add(predecessor.parts().size(), std::size_t{1U});
  if (!part_capacity.has_value()) {
    return common::make_unexpected(exhausted("Manifest part count does not fit this platform"));
  }
  parts.reserve(*part_capacity);
  for (TabletDescriptor& tablet : tablets) {
    tablet.first_part_index = parts.size();
    const auto old_tablet = std::ranges::lower_bound(
        predecessor.tablets(), tablet.tablet_id, {},
        [](const TabletDescriptor& descriptor) { return descriptor.tablet_id; });
    std::span<const PartDescriptor> old_parts;
    if (old_tablet != predecessor.tablets().end() && old_tablet->tablet_id == tablet.tablet_id) {
      old_parts =
          predecessor.parts().subspan(static_cast<std::size_t>(old_tablet->first_part_index),
                                      static_cast<std::size_t>(old_tablet->part_count));
    }
    if (tablet.tablet_id == part.tablet_id) {
      const auto position = std::ranges::lower_bound(
          old_parts, part.part_id, {},
          [](const PartDescriptor& descriptor) { return descriptor.part_id; });
      if (position != old_parts.end() && position->part_id == part.part_id) {
        return common::make_unexpected(invalid("Sealed CSEG part identity is already installed"));
      }
      parts.insert(parts.end(), old_parts.begin(), position);
      parts.push_back(part);
      parts.insert(parts.end(), position, old_parts.end());
    } else {
      parts.insert(parts.end(), old_parts.begin(), old_parts.end());
    }
    tablet.part_count = parts.size() - static_cast<std::size_t>(tablet.first_part_index);
  }

  std::vector<RetryDescriptor> retries(predecessor.retries().begin(), predecessor.retries().end());
  const std::optional<std::size_t> retry_capacity =
      common::checked_add(retries.size(), input.new_retries.size());
  if (!retry_capacity.has_value()) {
    return common::make_unexpected(exhausted("Manifest retry count does not fit this platform"));
  }
  retries.reserve(*retry_capacity);
  retries.insert(retries.end(), input.new_retries.begin(), input.new_retries.end());
  std::ranges::sort(retries, retry_less);
  for (std::size_t index = 1U; index < retries.size(); ++index) {
    if (retries[index - 1U].client_id == retries[index].client_id &&
        retries[index - 1U].client_batch_id == retries[index].client_batch_id) {
      return common::make_unexpected(
          invalid("Sealed manifest retry identity is already protected"));
    }
  }

  common::Result<EncodedManifest> encoded = encode_manifest_v1({
      .generation = *generation,
      .database_id = predecessor.database_id(),
      .wal_id = predecessor.wal_id(),
      .reclaim_checkpoint = predecessor.reclaim_checkpoint(),
      .tablets = tablets,
      .parts = parts,
      .retries = retries,
  });
  if (!encoded.has_value()) {
    return common::make_unexpected(encoded.error());
  }
  ManifestDecodeResult next = decode_manifest_v1_exact(encoded->bytes());
  if (!next.has_value()) {
    return common::make_unexpected(
        status(common::StatusCode::kInternal, "Generated Manifest v1 bytes did not decode"));
  }
  const common::Status transition =
      validate_manifest_v1_transition(predecessor, *next, input.schema_bindings);
  if (!transition.is_ok()) {
    return common::make_unexpected(transition);
  }
  return encoded;
}

} // namespace

common::Result<EncodedManifest>
build_manifest_v1_for_sealed_head(const SealedHeadManifestBuildInput& input) {
  try {
    return build(input);
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("Manifest generation allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("Manifest generation allocation exceeded limits"));
  }
}

} // namespace chronos::manifest
