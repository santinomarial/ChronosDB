#ifndef CHRONOS_TESTS_MANIFEST_MANIFEST_FLUSH_CRASH_FIXTURE_HPP_
#define CHRONOS_TESTS_MANIFEST_MANIFEST_FLUSH_CRASH_FIXTURE_HPP_

#include "chronos/common/uuid.hpp"
#include "chronos/manifest/codec.hpp"
#include "chronos/manifest/types.hpp"
#include "chronos/schema/column_definition.hpp"
#include "chronos/schema/logical_type.hpp"
#include "chronos/schema/schema_lineage.hpp"
#include "chronos/schema/table_schema.hpp"
#include "cseg/cseg_test_fixture.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::manifest::test {

template <typename Identifier> [[nodiscard]] Identifier crash_id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] inline common::Uuid crash_nonce(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return common::Uuid{bytes};
}

struct ManifestFlushCrashFixture {
  cseg::EncodedCsegPart encoded{cseg::test::make_valid_part(cseg::PageCompression::kZstd)};
  schema::TableId table_id{crash_id<schema::TableId>(2U)};
  schema::TabletId tablet_id{crash_id<schema::TabletId>(3U)};
  schema::SchemaId schema_id{crash_id<schema::SchemaId>(4U)};
  cseg::PartId part_id{crash_id<cseg::PartId>(1U)};
  schema::TableSchema schema_value{make_schema()};
  schema::SchemaLineage lineage{schema::SchemaLineage::create(schema_value).value()};
  PartDescriptor descriptor{.part_id = part_id,
                            .table_id = table_id,
                            .tablet_id = tablet_id,
                            .schema_id = schema_id,
                            .schema_version = schema::SchemaVersion::initial(),
                            .file_length = encoded.size(),
                            .row_count = 2U,
                            .minimum_record_sequence = 7U,
                            .maximum_record_sequence = 7U,
                            .minimum_event_time = -5,
                            .maximum_event_time = 10};
  wal::WalId wal_id{make_wal_id()};
  DatabaseId database_id{crash_id<DatabaseId>(6U)};

  [[nodiscard]] schema::TableSchema make_schema() const {
    const schema::ColumnId event_id = crash_id<schema::ColumnId>(5U);
    std::vector<schema::ColumnDefinition> columns;
    columns.push_back(
        schema::ColumnDefinition::create(
            event_id, "event_time",
            schema::LogicalType::create(schema::LogicalTypeKind::kTimestampNs).value(), false)
            .value());
    return schema::TableSchema::create(table_id, schema_id, schema::SchemaVersion::initial(),
                                       std::nullopt, std::move(columns),
                                       {.event_time_column = event_id,
                                        .physical_ordering_key = {event_id},
                                        .partition_columns = {event_id},
                                        .shard_key = {event_id},
                                        .deduplication_key = {}})
        .value();
  }

  [[nodiscard]] static wal::WalId make_wal_id() {
    wal::WalId value{};
    value.bytes.front() = std::byte{0x70U};
    return value;
  }

  [[nodiscard]] EncodedManifest manifest(const std::uint64_t generation) const {
    if (generation == 1U) {
      return encode_manifest_v1({.generation = 1U,
                                 .database_id = database_id,
                                 .wal_id = wal_id,
                                 .reclaim_checkpoint = {.record_sequence = 0U,
                                                        .segment_number = 1U,
                                                        .byte_offset = 64U},
                                 .tablets = {},
                                 .parts = {},
                                 .retries = {}})
          .value();
    }
    const std::array tablets{
        TabletDescriptor{.table_id = table_id,
                         .tablet_id = tablet_id,
                         .recovery_schema_id = schema_id,
                         .recovery_schema_version = schema::SchemaVersion::initial(),
                         .durable_record_sequence = 7U,
                         .first_part_index = 0U,
                         .part_count = 1U,
                         .durable_row_count = 2U}};
    const std::array parts{descriptor};
    return encode_manifest_v1({.generation = 2U,
                               .database_id = database_id,
                               .wal_id = wal_id,
                               .reclaim_checkpoint = {.record_sequence = 0U,
                                                      .segment_number = 1U,
                                                      .byte_offset = 64U},
                               .tablets = tablets,
                               .parts = parts,
                               .retries = {}})
        .value();
  }

  [[nodiscard]] std::array<TabletSchemaBinding, 1> bindings() const {
    return {TabletSchemaBinding{.tablet_id = tablet_id, .lineage = std::cref(lineage)}};
  }
};

} // namespace chronos::manifest::test

#endif // CHRONOS_TESTS_MANIFEST_MANIFEST_FLUSH_CRASH_FIXTURE_HPP_
