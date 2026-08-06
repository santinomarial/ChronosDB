#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/page_codec.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/logical_type.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace {

struct Fixture {
  std::vector<std::byte> values;
  chronos::schema::LogicalType logical_type;
  chronos::columnar::PhysicalColumnView physical;
  chronos::cseg::EncodedCsegPage encoded;
  chronos::cseg::CsegColumnDescriptor column;
  chronos::cseg::CsegPageDescriptor page;

  Fixture(const std::uint32_t rows, const chronos::cseg::PageCompression policy)
      : values(rows, std::byte{0x2aU}),
        logical_type(
            chronos::schema::LogicalType::create(chronos::schema::LogicalTypeKind::kUInt8).value()),
        physical(chronos::columnar::PhysicalColumnView::create(
                     {.type = logical_type, .nullable = false, .row_count = rows, .null_count = 0U},
                     {.validity = {}, .offsets = {}, .values = values})
                     .value()),
        encoded(chronos::cseg::encode_cseg_v1_page(physical, policy).value()),
        column{.column_id = column_id(),
               .storage_kind = chronos::cseg::StorageKind::kUser,
               .logical_type = logical_type,
               .nullable = false,
               .event_time = false,
               .schema_ordinal = 0U,
               .ordering_ordinal = std::nullopt},
        page(make_page(encoded)) {}

private:
  [[nodiscard]] static chronos::schema::ColumnId column_id() {
    chronos::common::Uuid::Bytes bytes{};
    bytes.back() = std::byte{1U};
    return chronos::schema::ColumnId::from_bytes(bytes).value();
  }

  [[nodiscard]] static chronos::cseg::CsegPageDescriptor
  make_page(const chronos::cseg::EncodedCsegPage& encoded) {
    const chronos::cseg::CsegPageMetadataInput metadata = encoded.metadata();
    return {.granule_ordinal = 0U,
            .stored_column_ordinal = 0U,
            .compression = metadata.compression,
            .row_count = metadata.row_count,
            .null_count = metadata.null_count,
            .page_offset = 0U,
            .stored_length = metadata.stored_length,
            .uncompressed_length = metadata.uncompressed_length,
            .validity_length = metadata.validity_length,
            .offsets_length = metadata.offsets_length,
            .values_length = metadata.values_length,
            .page_crc32c = metadata.page_crc32c};
  }
};

void benchmark_stored_encode(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const auto policy = state.range(1) == 0 ? chronos::cseg::PageCompression::kNone
                                          : chronos::cseg::PageCompression::kZstd;
  const Fixture fixture{rows, policy};
  for ([[maybe_unused]] auto iteration : state) {
    auto encoded = chronos::cseg::encode_cseg_v1_page(fixture.physical, policy);
    if (!encoded.has_value()) {
      const std::string message = encoded.error().to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark::DoNotOptimize(encoded->bytes().data());
  }
  state.SetItemsProcessed(state.iterations() * rows);
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(fixture.values.size()));
  state.SetLabel(policy == chronos::cseg::PageCompression::kNone
                     ? "PLAIN + raw copy + stored CRC32C; local only"
                     : "PLAIN + canonical ZSTD policy + stored CRC32C; local only");
}

void benchmark_stored_decode(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const auto policy = state.range(1) == 0 ? chronos::cseg::PageCompression::kNone
                                          : chronos::cseg::PageCompression::kZstd;
  const Fixture fixture{rows, policy};
  for ([[maybe_unused]] auto iteration : state) {
    auto decoded =
        chronos::cseg::decode_cseg_v1_page(fixture.encoded.bytes(), fixture.column, fixture.page);
    if (!decoded.has_value()) {
      const std::string message = decoded.error().to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark::DoNotOptimize(decoded->physical().values().data());
  }
  state.SetItemsProcessed(state.iterations() * rows);
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(fixture.values.size()));
  state.SetLabel(fixture.encoded.compression() == chronos::cseg::PageCompression::kNone
                     ? "stored CRC32C + borrowed raw physical validation; local only"
                     : "stored CRC32C + bounded ZSTD + owned physical validation; local only");
}

// Google Benchmark registers functions during static initialization.
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_stored_encode)->ArgsProduct({{64, 1024, 65536}, {0, 1}});
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_stored_decode)->ArgsProduct({{64, 1024, 65536}, {0, 1}});

} // namespace
