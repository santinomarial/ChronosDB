#include "chronos/common/uuid.hpp"
#include "chronos/cseg/metadata_codec.hpp"
#include "chronos/cseg/pruning.hpp"
#include "chronos/schema/identity.hpp"
#include "chronos/schema/logical_type.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
#include <optional>
#include <random>
#include <span>
#include <utility>
#include <vector>

namespace chronos::cseg {
namespace {

template <typename Identifier> [[nodiscard]] Identifier identifier(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

[[nodiscard]] schema::LogicalType type(const schema::LogicalTypeKind kind) {
  return schema::LogicalType::create(kind).value();
}

[[nodiscard]] CsegPageMetadataInput page(const std::uint64_t values_length) {
  return {.compression = PageCompression::kNone,
          .row_count = 2U,
          .null_count = 0U,
          .stored_length = values_length,
          .uncompressed_length = values_length,
          .validity_length = 0U,
          .offsets_length = 0U,
          .values_length = values_length,
          .page_crc32c = 0U};
}

struct PruningFixture {
  PruningFixture() {
    columns.push_back({.column_id = identifier<schema::ColumnId>(5U),
                       .storage_kind = StorageKind::kUser,
                       .logical_type = type(schema::LogicalTypeKind::kTimestampNs),
                       .nullable = false,
                       .event_time = true,
                       .schema_ordinal = 0U,
                       .ordering_ordinal = 0U});
    columns.push_back({.column_id = std::nullopt,
                       .storage_kind = StorageKind::kWalId,
                       .logical_type = type(schema::LogicalTypeKind::kUuid)});
    columns.push_back({.column_id = std::nullopt,
                       .storage_kind = StorageKind::kRecordSequence,
                       .logical_type = type(schema::LogicalTypeKind::kUInt64)});
    columns.push_back({.column_id = std::nullopt,
                       .storage_kind = StorageKind::kRowOrdinal,
                       .logical_type = type(schema::LogicalTypeKind::kUInt32)});
    columns.push_back({.column_id = std::nullopt,
                       .storage_kind = StorageKind::kOperation,
                       .logical_type = type(schema::LogicalTypeKind::kUInt8)});
    granules = std::array<CsegGranuleDescriptor, 3>{
        CsegGranuleDescriptor{.first_row = 0U,
                              .row_count = 2U,
                              .first_page_index = 0U,
                              .minimum_event_time = -10,
                              .maximum_event_time = -5},
        CsegGranuleDescriptor{.first_row = 2U,
                              .row_count = 2U,
                              .first_page_index = 5U,
                              .minimum_event_time = 0,
                              .maximum_event_time = 9},
        CsegGranuleDescriptor{.first_row = 4U,
                              .row_count = 2U,
                              .first_page_index = 10U,
                              .minimum_event_time = 10,
                              .maximum_event_time = 20},
    };
    for (std::size_t granule = 0U; granule < granules.size(); ++granule) {
      pages.push_back(page(16U));
      pages.push_back(page(32U));
      pages.push_back(page(16U));
      pages.push_back(page(8U));
      pages.push_back(page(2U));
    }
    encoded = std::make_optional(
        encode_cseg_v1_metadata({.part_id = part_id,
                                 .table_id = identifier<schema::TableId>(2U),
                                 .tablet_id = identifier<schema::TabletId>(3U),
                                 .schema_id = identifier<schema::SchemaId>(4U),
                                 .schema_version = schema::SchemaVersion::initial(),
                                 .row_count = 6U,
                                 .event_time_column_ordinal = 0U,
                                 .ordering_column_count = 1U,
                                 .minimum_event_time = -10,
                                 .maximum_event_time = 20,
                                 .columns = columns,
                                 .granules = granules,
                                 .pages = pages})
            .value());
    decoded = std::make_optional(decode_cseg_v1_metadata_exact(encoded->bytes()).value());
  }

  PartId part_id{identifier<PartId>(1U)};
  std::vector<CsegColumnDescriptor> columns;
  std::array<CsegGranuleDescriptor, 3> granules;
  std::vector<CsegPageMetadataInput> pages;
  std::optional<EncodedCsegMetadata> encoded;
  std::optional<DecodedCsegMetadataView> decoded;
};

[[nodiscard]] bool reference_match(const std::int64_t minimum, const std::int64_t maximum,
                                   const EventTimePredicate& predicate) {
  for (std::int64_t value = minimum; value <= maximum; ++value) {
    const bool above_lower = !predicate.lower.has_value() || value > predicate.lower->value ||
                             (value == predicate.lower->value && predicate.lower->inclusive);
    const bool below_upper = !predicate.upper.has_value() || value < predicate.upper->value ||
                             (value == predicate.upper->value && predicate.upper->inclusive);
    if (above_lower && below_upper) {
      return true;
    }
  }
  return false;
}

TEST(CsegEventTimePruningTest, UnboundedAndHalfOpenPredicatesProduceExactOwnedPlans) {
  const PruningFixture fixture;
  common::Result<CsegEventTimePruningPlan> all = plan_cseg_v1_event_time_pruning(*fixture.decoded);
  ASSERT_TRUE(all.has_value());
  EXPECT_EQ(all->part_id(), fixture.part_id);
  EXPECT_TRUE(
      std::ranges::equal(all->selected_granules(), std::array<std::uint32_t, 3>{0U, 1U, 2U}));
  EXPECT_EQ(all->selected_rows(), 6U);
  EXPECT_EQ(all->skipped_rows(), 0U);

  const EventTimePredicate middle{.lower = EventTimeBound{.value = 0, .inclusive = true},
                                  .upper = EventTimeBound{.value = 10, .inclusive = false}};
  common::Result<CsegEventTimePruningPlan> pruned =
      plan_cseg_v1_event_time_pruning(*fixture.decoded, middle);
  ASSERT_TRUE(pruned.has_value());
  EXPECT_TRUE(std::ranges::equal(pruned->selected_granules(), std::array<std::uint32_t, 1>{1U}));
  EXPECT_EQ(pruned->selected_rows(), 2U);
  EXPECT_EQ(pruned->skipped_rows(), 4U);
  EXPECT_EQ(pruned->skipped_granules(), 2U);
}

TEST(CsegEventTimePruningTest, OpenClosedAndEmptyBoundaryCasesNeverOverflow) {
  const PruningFixture fixture;
  const std::array predicates{
      EventTimePredicate{.lower = EventTimeBound{.value = -5, .inclusive = true},
                         .upper = EventTimeBound{.value = -5, .inclusive = true}},
      EventTimePredicate{.lower = EventTimeBound{.value = -5, .inclusive = false},
                         .upper = EventTimeBound{.value = 0, .inclusive = false}},
      EventTimePredicate{.lower = EventTimeBound{.value = 5, .inclusive = true},
                         .upper = EventTimeBound{.value = -5, .inclusive = true}},
      EventTimePredicate{.lower = EventTimeBound{.value = std::numeric_limits<std::int64_t>::min(),
                                                 .inclusive = false},
                         .upper = EventTimeBound{.value = std::numeric_limits<std::int64_t>::max(),
                                                 .inclusive = false}},
  };
  const std::array expected_counts{1U, 0U, 0U, 3U};
  for (std::size_t index = 0U; index < predicates.size(); ++index) {
    const auto plan = plan_cseg_v1_event_time_pruning(*fixture.decoded, predicates[index]);
    ASSERT_TRUE(plan.has_value());
    EXPECT_EQ(plan->selected_granules().size(), expected_counts[index]);
  }
  const auto reversed = cseg_event_time_range_may_match(10, -10, std::nullopt);
  ASSERT_FALSE(reversed.has_value());
  EXPECT_EQ(reversed.error().code(), common::StatusCode::kInvalidArgument);
}

TEST(CsegEventTimePruningTest, EnforcesConfiguredGranuleLimitBeforeAllocation) {
  const PruningFixture fixture;
  const auto result =
      plan_cseg_v1_event_time_pruning(*fixture.decoded, std::nullopt, {.max_granules = 2U});
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
}

TEST(CsegEventTimePruningPropertyTest,
     NeverDropsIndependentIntegerOracleMatchesAndIsDeterministic) {
  const PruningFixture fixture;
  std::mt19937_64 generator{0x435345475052554eULL};
  std::uniform_int_distribution<std::int64_t> endpoint(-25, 25);
  std::bernoulli_distribution present(0.8);
  std::bernoulli_distribution inclusive(0.5);
  for (std::size_t iteration = 0U; iteration < 2'000U; ++iteration) {
    EventTimePredicate predicate;
    if (present(generator)) {
      predicate.lower =
          EventTimeBound{.value = endpoint(generator), .inclusive = inclusive(generator)};
    }
    if (present(generator)) {
      predicate.upper =
          EventTimeBound{.value = endpoint(generator), .inclusive = inclusive(generator)};
    }
    const auto plan = plan_cseg_v1_event_time_pruning(*fixture.decoded, predicate);
    ASSERT_TRUE(plan.has_value());
    const auto repeated = plan_cseg_v1_event_time_pruning(*fixture.decoded, predicate);
    ASSERT_TRUE(repeated.has_value());
    EXPECT_TRUE(std::ranges::equal(plan->selected_granules(), repeated->selected_granules()));
    std::vector<std::uint32_t> expected;
    for (std::uint32_t ordinal = 0U; ordinal < fixture.granules.size(); ++ordinal) {
      const CsegGranuleDescriptor& granule = fixture.granules[ordinal];
      if (reference_match(granule.minimum_event_time, granule.maximum_event_time, predicate)) {
        expected.push_back(ordinal);
      }
    }
    EXPECT_TRUE(std::ranges::includes(plan->selected_granules(), expected));
  }
}

} // namespace
} // namespace chronos::cseg
