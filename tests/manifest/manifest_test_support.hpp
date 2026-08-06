#ifndef CHRONOS_TESTS_MANIFEST_MANIFEST_TEST_SUPPORT_HPP_
#define CHRONOS_TESTS_MANIFEST_MANIFEST_TEST_SUPPORT_HPP_

#include "chronos/manifest/codec.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace chronos::manifest::test {

template <typename Identifier> [[nodiscard]] Identifier make_id(const std::uint8_t value) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(value);
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] inline wal::WalId make_wal_id(const std::uint8_t value = 0x70U) {
  wal::WalId wal_id{};
  wal_id.bytes.back() = static_cast<std::byte>(value);
  return wal_id;
}

[[nodiscard]] inline ingest::Sha256Digest make_digest(const std::uint8_t value = 0x90U) {
  ingest::Sha256Digest::Bytes bytes{};
  for (std::size_t index = 0U; index < bytes.size(); ++index) {
    bytes[index] = static_cast<std::byte>(static_cast<std::uint8_t>(value + index));
  }
  return ingest::Sha256Digest{bytes};
}

struct ManifestFixture {
  explicit ManifestFixture(const std::uint8_t seed = 1U)
      : database_id(make_id<DatabaseId>(static_cast<std::uint8_t>(0x10U + seed))),
        wal_id(make_wal_id(static_cast<std::uint8_t>(0x20U + seed))),
        tablets{TabletDescriptor{
            .table_id = make_id<schema::TableId>(static_cast<std::uint8_t>(0x30U + seed)),
            .tablet_id = make_id<schema::TabletId>(static_cast<std::uint8_t>(0x40U + seed)),
            .recovery_schema_id =
                make_id<schema::SchemaId>(static_cast<std::uint8_t>(0x50U + seed)),
            .recovery_schema_version = schema::SchemaVersion::initial(),
            .durable_record_sequence = static_cast<std::uint64_t>(9U + seed),
            .first_part_index = 0U,
            .part_count = 1U,
            .durable_row_count = static_cast<std::uint64_t>(3U + seed),
        }},
        parts{PartDescriptor{
            .part_id = make_id<cseg::PartId>(static_cast<std::uint8_t>(0x60U + seed)),
            .table_id = tablets.front().table_id,
            .tablet_id = tablets.front().tablet_id,
            .schema_id = tablets.front().recovery_schema_id,
            .schema_version = tablets.front().recovery_schema_version,
            .file_length = 1'208U,
            .row_count = tablets.front().durable_row_count,
            .minimum_record_sequence = static_cast<std::uint64_t>(7U + seed),
            .maximum_record_sequence = tablets.front().durable_record_sequence,
            .minimum_event_time = -1'000 - seed,
            .maximum_event_time = 2'000 + seed,
        }},
        retries{RetryDescriptor{
            .client_id = make_id<ingest::ClientId>(static_cast<std::uint8_t>(0x70U + seed)),
            .client_batch_id =
                make_id<ingest::ClientBatchId>(static_cast<std::uint8_t>(0x80U + seed)),
            .table_id = tablets.front().table_id,
            .tablet_id = tablets.front().tablet_id,
            .request_digest = make_digest(static_cast<std::uint8_t>(0x90U + seed)),
            .wal_id = wal_id,
            .record_sequence = tablets.front().durable_record_sequence,
            .applied_row_count = static_cast<std::uint32_t>(3U + seed),
        }} {}

  [[nodiscard]] ManifestEncodeInput input(const std::uint64_t generation = 2U) const {
    return ManifestEncodeInput{
        .generation = generation,
        .database_id = database_id,
        .wal_id = wal_id,
        .reclaim_checkpoint = {.record_sequence = 5U, .segment_number = 1U, .byte_offset = 128U},
        .tablets = tablets,
        .parts = parts,
        .retries = retries,
    };
  }

  DatabaseId database_id;
  wal::WalId wal_id;
  std::vector<TabletDescriptor> tablets;
  std::vector<PartDescriptor> parts;
  std::vector<RetryDescriptor> retries;
};

[[nodiscard]] inline EncodedManifest encode_fixture(const ManifestFixture& fixture,
                                                    const std::uint64_t generation = 2U) {
  return encode_manifest_v1(fixture.input(generation)).value();
}

} // namespace chronos::manifest::test

#endif // CHRONOS_TESTS_MANIFEST_MANIFEST_TEST_SUPPORT_HPP_
