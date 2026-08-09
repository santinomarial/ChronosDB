#include "chronos/cseg/temporal_system_row.hpp"

#include <cstddef>
#include <gtest/gtest.h>

namespace chronos::cseg {
namespace {

[[nodiscard]] TemporalSystemRowView valid_row() {
  common::Uuid::Bytes source{};
  source.front() = std::byte{1U};
  static constexpr std::byte identity[]{std::byte{7U}};
  return TemporalSystemRowView{.commit_source = temporal_format::CommitSource::kRaft,
                               .source_id = common::Uuid{source},
                               .commit_position = 9U,
                               .row_ordinal = 2U,
                               .operation = temporal_format::Operation::kCorrection,
                               .logical_identity = identity,
                               .receive_time_ns = 100,
                               .system_commit_time_ns = 110};
}

TEST(TemporalSystemRowTest, AcceptsWalOrRaftRowsAndRejectsUnboundSemantics) {
  TemporalSystemRowView row = valid_row();
  EXPECT_TRUE(validate_temporal_system_row(row).is_ok());
  row.commit_source = temporal_format::CommitSource::kWal;
  EXPECT_TRUE(validate_temporal_system_row(row).is_ok());
  row.source_id = {};
  EXPECT_EQ(validate_temporal_system_row(row).code(), common::StatusCode::kInvalidArgument);
  row = valid_row();
  row.operation = static_cast<temporal_format::Operation>(5U);
  EXPECT_EQ(validate_temporal_system_row(row).code(), common::StatusCode::kInvalidArgument);
  row = valid_row();
  row.logical_identity = {};
  EXPECT_EQ(validate_temporal_system_row(row).code(), common::StatusCode::kInvalidArgument);
  row = valid_row();
  EXPECT_EQ(
      validate_temporal_system_row(row, {.maximum_logical_identity_bytes =
                                             temporal_format::kMaximumLogicalIdentityBytes + 1U})
          .code(),
      common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::cseg
