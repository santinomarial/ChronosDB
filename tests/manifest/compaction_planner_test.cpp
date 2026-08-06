#include "chronos/common/uuid.hpp"
#include "chronos/manifest/compaction_planner.hpp"
#include "chronos/schema/identity.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <random>
#include <span>
#include <vector>

namespace chronos::manifest {
namespace {

template <typename Identifier> [[nodiscard]] Identifier identifier(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.front() = std::byte{seed};
  return Identifier::from_bytes(bytes).value();
}

struct PlannerFixture {
  [[nodiscard]] PartDescriptor part(const std::uint8_t part_seed, const std::uint64_t sequence,
                                    const std::int64_t minimum, const std::int64_t maximum) const {
    return {.part_id = identifier<cseg::PartId>(part_seed),
            .table_id = table_id,
            .tablet_id = tablet_id,
            .schema_id = schema_id,
            .schema_version = schema::SchemaVersion::initial(),
            .file_length = 100U,
            .row_count = 10U,
            .minimum_record_sequence = sequence,
            .maximum_record_sequence = sequence,
            .minimum_event_time = minimum,
            .maximum_event_time = maximum};
  }

  schema::TableId table_id{identifier<schema::TableId>(0x20U)};
  schema::TabletId tablet_id{identifier<schema::TabletId>(0x30U)};
  schema::SchemaId schema_id{identifier<schema::SchemaId>(0x40U)};
};

TEST(AppendOnlyCompactionPlannerTest, RebuildsLatePartRolesInArrivalOrder) {
  const PlannerFixture fixture;
  const std::array parts{fixture.part(1U, 10U, 100, 200), fixture.part(2U, 20U, 210, 300),
                         fixture.part(3U, 30U, 150, 160), fixture.part(4U, 40U, 301, 400)};
  const auto classified = classify_append_only_parts(parts);
  ASSERT_TRUE(classified.has_value());
  ASSERT_EQ(classified->size(), parts.size());
  EXPECT_EQ((*classified)[0].role, AppendOnlyPartRole::kBase);
  EXPECT_EQ((*classified)[1].role, AppendOnlyPartRole::kBase);
  EXPECT_EQ((*classified)[2].role, AppendOnlyPartRole::kDelta);
  EXPECT_EQ((*classified)[3].role, AppendOnlyPartRole::kBase);

  const auto planned = plan_append_only_compaction(parts);
  ASSERT_TRUE(planned.has_value());
  ASSERT_TRUE(planned->has_value());
  const PlannedAppendOnlyCompaction& plan = **planned;
  EXPECT_EQ(plan.table_id(), fixture.table_id);
  EXPECT_EQ(plan.tablet_id(), fixture.tablet_id);
  EXPECT_EQ(plan.schema_id(), fixture.schema_id);
  EXPECT_TRUE(std::ranges::equal(plan.input_part_ids(), std::array{identifier<cseg::PartId>(1U),
                                                                   identifier<cseg::PartId>(3U)}));
  EXPECT_EQ(plan.input_bytes(), 200U);
  EXPECT_EQ(plan.input_rows(), 20U);
  EXPECT_EQ(plan.minimum_event_time(), 100);
  EXPECT_EQ(plan.maximum_event_time(), 200);
  EXPECT_EQ(plan.delta_input_parts(), 1U);
}

TEST(AppendOnlyCompactionPlannerTest, PlansTouchingBaseRangesWithoutInventingDeltaState) {
  const PlannerFixture fixture;
  const std::array parts{fixture.part(1U, 10U, 0, 10), fixture.part(2U, 20U, 10, 20)};
  const auto classified = classify_append_only_parts(parts);
  ASSERT_TRUE(classified.has_value());
  EXPECT_EQ((*classified)[0].role, AppendOnlyPartRole::kBase);
  EXPECT_EQ((*classified)[1].role, AppendOnlyPartRole::kBase);
  const auto planned = plan_append_only_compaction(parts);
  ASSERT_TRUE(planned.has_value());
  ASSERT_TRUE(planned->has_value());
  EXPECT_EQ((**planned).delta_input_parts(), 0U);
}

TEST(AppendOnlyCompactionPlannerTest, SeparatesIdentityGroupsAndHonorsResourceLimits) {
  const PlannerFixture fixture;
  std::array parts{fixture.part(1U, 10U, 0, 20), fixture.part(2U, 20U, 5, 10),
                   fixture.part(3U, 30U, 6, 7)};
  parts[0].tablet_id = identifier<schema::TabletId>(0x31U);
  const auto planned = plan_append_only_compaction(parts);
  ASSERT_TRUE(planned.has_value());
  ASSERT_TRUE(planned->has_value());
  EXPECT_EQ((**planned).tablet_id(), fixture.tablet_id);
  EXPECT_TRUE(
      std::ranges::equal((**planned).input_part_ids(),
                         std::array{identifier<cseg::PartId>(2U), identifier<cseg::PartId>(3U)}));

  const auto bytes_limited = plan_append_only_compaction(
      std::span<const PartDescriptor>{parts}.subspan(1U), {.maximum_input_bytes = 150U});
  ASSERT_TRUE(bytes_limited.has_value());
  EXPECT_FALSE(bytes_limited->has_value());
  const auto count_limited = classify_append_only_parts(parts, 2U);
  ASSERT_FALSE(count_limited.has_value());
  EXPECT_EQ(count_limited.error().code(), common::StatusCode::kResourceExhausted);
}

TEST(AppendOnlyCompactionPlannerTest, RejectsInvalidLimitsDescriptorsAndDuplicateIdentities) {
  const PlannerFixture fixture;
  std::array parts{fixture.part(1U, 10U, 0, 10), fixture.part(2U, 20U, 5, 20)};
  EXPECT_EQ(plan_append_only_compaction(parts, {.minimum_input_parts = 1U}).error().code(),
            common::StatusCode::kInvalidArgument);
  parts[1].part_id = parts[0].part_id;
  EXPECT_EQ(classify_append_only_parts(parts).error().code(), common::StatusCode::kInvalidArgument);
  parts[1] = fixture.part(2U, 20U, 30, 20);
  EXPECT_EQ(classify_append_only_parts(parts).error().code(), common::StatusCode::kInvalidArgument);
}

TEST(AppendOnlyCompactionPlannerPropertyTest, GeneratedPlansAreStableBoundedAndGroupExact) {
  const PlannerFixture fixture;
  std::mt19937_64 generator{0x504c414e434f4d50ULL};
  std::uniform_int_distribution<std::int64_t> start(-1'000, 1'000);
  std::uniform_int_distribution<std::int64_t> width(0, 200);
  for (std::size_t iteration = 0U; iteration < 500U; ++iteration) {
    std::vector<PartDescriptor> parts;
    parts.reserve(32U);
    for (std::uint8_t index = 1U; index <= 32U; ++index) {
      const std::int64_t minimum = start(generator);
      parts.push_back(fixture.part(index, static_cast<std::uint64_t>(index), minimum,
                                   minimum + width(generator)));
    }
    const auto first = plan_append_only_compaction(
        parts, {.maximum_input_parts = 6U, .maximum_input_bytes = 600U});
    const auto second = plan_append_only_compaction(
        parts, {.maximum_input_parts = 6U, .maximum_input_bytes = 600U});
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(first->has_value(), second->has_value());
    if (!first->has_value()) {
      continue;
    }
    EXPECT_TRUE(std::ranges::equal((**first).input_part_ids(), (**second).input_part_ids()));
    EXPECT_GE((**first).input_part_ids().size(), 2U);
    EXPECT_LE((**first).input_part_ids().size(), 6U);
    EXPECT_LE((**first).input_bytes(), 600U);
    EXPECT_TRUE(std::ranges::is_sorted((**first).input_part_ids()));
    for (const cseg::PartId& part_id : (**first).input_part_ids()) {
      const auto found = std::ranges::find(parts, part_id, &PartDescriptor::part_id);
      ASSERT_NE(found, parts.end());
      EXPECT_EQ(found->tablet_id, fixture.tablet_id);
      EXPECT_EQ(found->schema_id, fixture.schema_id);
    }
  }
}

} // namespace
} // namespace chronos::manifest
