#include "chronos/common/status.hpp"
#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/metadata_snapshot.hpp"
#include "chronos/raft/schema_definition_codec.hpp"
#include "chronos/raft/tablet_group_binding_codec.hpp"
#include "support/failing_allocator.hpp"

#include <array>
#include <cstddef>
#include <gtest/gtest.h>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

[[nodiscard]] std::vector<std::byte> payload(const std::size_t size, const std::byte value) {
  return {size, value};
}

[[nodiscard]] MetadataApplicationSnapshot snapshot(const bool minor_one) {
  common::Uuid::Bytes group_bytes{};
  group_bytes.front() = std::byte{9U};
  SnapshotMetadata metadata{.last_included_index = 8U,
                            .last_included_term = 3U,
                            .manifest_generation = 8U,
                            .part_set_checksum = {},
                            .configuration_index = 6U,
                            .voters = {1U, 2U, 3U, 4U}};
  metadata.part_set_checksum.fill(std::byte{0x5aU});
  MetadataApplicationSnapshot value{.group_id = GroupId{group_bytes},
                                    .raft_snapshot = std::move(metadata),
                                    .entries = {{.index = 2U,
                                                 .term = 1U,
                                                 .type = kRaftMetadataCommandEntryType,
                                                 .payload = payload(17U, std::byte{0x11U})},
                                                {.index = 5U,
                                                 .term = 2U,
                                                 .type = kRaftSchemaDefinitionEntryType,
                                                 .payload = payload(33U, std::byte{0x22U})}}};
  if (minor_one) {
    value.entries.push_back({.index = 8U,
                             .term = 3U,
                             .type = kRaftTabletGroupBindingEntryType,
                             .payload = payload(49U, std::byte{0x33U})});
  }
  return value;
}

template <typename Operation>
[[nodiscard]] auto run_with_allocation_failure(const std::size_t fail_after, std::size_t& observed,
                                               Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    test::ScopedAllocationFailure failure{fail_after};
    result.emplace(operation());
    observed = failure.observed_allocations();
    failure.disable();
  }
  return std::move(*result);
}

template <typename Operation, typename VerifySuccess>
[[nodiscard]] std::size_t sweep_allocation_failures(Operation&& operation,
                                                    VerifySuccess&& verify_success) {
  std::size_t failure_count = 0U;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    SCOPED_TRACE(fail_after);
    std::size_t observed = 0U;
    auto result = run_with_allocation_failure(fail_after, observed, operation);
    if (result.has_value()) {
      verify_success(*result);
      EXPECT_EQ(failure_count, observed);
      return failure_count;
    }
    ++failure_count;
    EXPECT_EQ(observed, fail_after + 1U);
    EXPECT_EQ(result.error().code(), common::StatusCode::kResourceExhausted);
  }
  ADD_FAILURE() << "allocation sweep did not reach success";
  return failure_count;
}

TEST(MetadataSnapshotAllocationFailureTest, EncodesBothMinorVersionsFailClosed) {
  for (const bool minor_one : std::array{false, true}) {
    SCOPED_TRACE(minor_one ? "minor 1" : "minor 0");
    const MetadataApplicationSnapshot expected = snapshot(minor_one);
    const auto canonical = encode_metadata_application_snapshot_v1(expected);
    ASSERT_TRUE(canonical.has_value());

    const std::size_t failures = sweep_allocation_failures(
        [&] { return encode_metadata_application_snapshot_v1(expected); },
        [&](const std::vector<std::byte>& encoded) { EXPECT_EQ(encoded, *canonical); });
    EXPECT_GT(failures, 0U);
  }
}

TEST(MetadataSnapshotAllocationFailureTest, DecodesEveryOwnedAllocationForBothMinorVersions) {
  for (const bool minor_one : std::array{false, true}) {
    SCOPED_TRACE(minor_one ? "minor 1" : "minor 0");
    const MetadataApplicationSnapshot expected = snapshot(minor_one);
    const auto encoded = encode_metadata_application_snapshot_v1(expected);
    ASSERT_TRUE(encoded.has_value());

    const std::size_t failures = sweep_allocation_failures(
        [&] { return decode_metadata_application_snapshot_v1(*encoded); },
        [&](const MetadataApplicationSnapshot& decoded) { EXPECT_EQ(decoded, expected); });
    EXPECT_GT(failures, 0U);
  }
}

} // namespace
} // namespace chronos::raft
