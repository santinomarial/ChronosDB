#include "chronos/columnar/column_vector.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/cseg/plain_page.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/logical_type.hpp"

#include <benchmark/benchmark.h>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace {

void append_u32(std::vector<std::byte>& bytes, const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index) {
    bytes.push_back(std::byte{static_cast<std::uint8_t>(value >> (index * 8U))});
  }
}

[[nodiscard]] chronos::schema::LogicalType type(const chronos::schema::LogicalTypeKind kind) {
  return chronos::schema::LogicalType::create(kind).value();
}

struct Fixture {
  std::vector<std::byte> validity;
  std::vector<std::byte> offsets;
  std::vector<std::byte> values;
  chronos::schema::LogicalType logical_type;
  bool nullable;
  std::uint32_t row_count;
  std::uint32_t null_count{0U};
  chronos::columnar::PhysicalColumnView physical;
  chronos::cseg::EncodedCsegPlainPage encoded;
  chronos::cseg::CsegColumnDescriptor column;
  chronos::cseg::CsegPageDescriptor page;

  Fixture(const std::uint32_t rows, const bool variable)
      : logical_type(type(variable ? chronos::schema::LogicalTypeKind::kString
                                   : chronos::schema::LogicalTypeKind::kTimestampNs)),
        nullable(variable), row_count(rows), physical(make_physical(rows, variable)),
        encoded(make_encoded()), column{.column_id = column_id(),
                                        .storage_kind = chronos::cseg::StorageKind::kUser,
                                        .logical_type = logical_type,
                                        .nullable = nullable,
                                        .event_time = false,
                                        .schema_ordinal = 0U,
                                        .ordering_ordinal = std::nullopt},
        page{.granule_ordinal = 0U,
             .stored_column_ordinal = 0U,
             .compression = chronos::cseg::PageCompression::kNone,
             .row_count = row_count,
             .null_count = null_count,
             .page_offset = 0U,
             .stored_length = encoded.size(),
             .uncompressed_length = encoded.size(),
             .validity_length = encoded.validity_length(),
             .offsets_length = encoded.offsets_length(),
             .values_length = encoded.values_length(),
             .page_crc32c = 0U} {}

private:
  [[nodiscard]] static chronos::schema::ColumnId column_id() {
    chronos::common::Uuid::Bytes bytes{};
    bytes.back() = std::byte{1U};
    return chronos::schema::ColumnId::from_bytes(bytes).value();
  }

  [[nodiscard]] chronos::columnar::PhysicalColumnView make_physical(const std::uint32_t rows,
                                                                    const bool variable) {
    if (variable) {
      validity.assign(chronos::columnar::bitmap_size(rows), std::byte{0U});
      offsets.reserve((static_cast<std::size_t>(rows) + 1U) * sizeof(std::uint32_t));
      append_u32(offsets, 0U);
      for (std::uint32_t row = 0U; row < rows; ++row) {
        if ((row % 8U) == 0U) {
          ++null_count;
        } else {
          validity[row / 8U] |= std::byte{static_cast<std::uint8_t>(1U << (row % 8U))};
          values.insert(values.end(), 16U, std::byte{'x'});
        }
        append_u32(offsets, static_cast<std::uint32_t>(values.size()));
      }
    } else {
      values.resize(static_cast<std::size_t>(rows) * sizeof(std::uint64_t), std::byte{0U});
    }
    return chronos::columnar::PhysicalColumnView::create(
               {.type = logical_type,
                .nullable = nullable,
                .row_count = row_count,
                .null_count = null_count},
               {.validity = validity, .offsets = offsets, .values = values})
        .value();
  }

  [[nodiscard]] chronos::cseg::EncodedCsegPlainPage make_encoded() const {
    return chronos::cseg::encode_cseg_v1_plain_page(physical).value();
  }
};

void benchmark_encode(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const bool variable = state.range(1) != 0;
  const Fixture fixture{rows, variable};
  for ([[maybe_unused]] auto iteration : state) {
    auto encoded = chronos::cseg::encode_cseg_v1_plain_page(fixture.physical);
    if (!encoded.has_value()) {
      const std::string message = encoded.error().to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark::DoNotOptimize(encoded->bytes().data());
  }
  state.SetItemsProcessed(state.iterations() * rows);
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(fixture.encoded.size()));
  state.SetLabel(variable ? "nullable STRING/16-byte values; exact payload copy; local only"
                          : "TIMESTAMP_NS; exact payload copy; local only");
}

void benchmark_decode(benchmark::State& state) {
  const auto rows = static_cast<std::uint32_t>(state.range(0));
  const bool variable = state.range(1) != 0;
  const Fixture fixture{rows, variable};
  for ([[maybe_unused]] auto iteration : state) {
    auto decoded = chronos::cseg::decode_cseg_v1_plain_page(fixture.encoded.bytes(), fixture.column,
                                                            fixture.page);
    if (!decoded.has_value()) {
      const std::string message = decoded.error().to_string();
      state.SkipWithError(message);
      return;
    }
    benchmark::DoNotOptimize(decoded->physical().values().data());
  }
  state.SetItemsProcessed(state.iterations() * rows);
  state.SetBytesProcessed(state.iterations() * static_cast<std::int64_t>(fixture.encoded.size()));
  state.SetLabel(variable ? "nullable STRING canonical validation incl. UTF-8; borrowed; local only"
                          : "TIMESTAMP_NS canonical validation; borrowed; local only");
}

// Google Benchmark registers functions during static initialization.
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_encode)->ArgsProduct({{64, 1024, 65536}, {0, 1}});
// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
BENCHMARK(benchmark_decode)->ArgsProduct({{64, 1024, 65536}, {0, 1}});

} // namespace
