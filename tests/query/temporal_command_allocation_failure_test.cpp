#include "chronos/query/temporal_command.hpp"
#include "columnar/columnar_test_support.hpp"
#include "support/failing_allocator.hpp"

#include <cstddef>
#include <gtest/gtest.h>
#include <optional>
#include <utility>
#include <vector>

namespace chronos::query {
namespace {

template <typename Operation>
[[nodiscard]] auto run_failure(const std::size_t fail_after, Operation&& operation) {
  using Result = decltype(operation());
  std::optional<Result> result;
  {
    ::chronos::test::ScopedAllocationFailure failure{fail_after};
    result.emplace(operation());
    failure.disable();
  }
  return std::move(*result);
}

[[nodiscard]] columnar::OwnedColumnarBatch batch() {
  return columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                              columnar::test::batch_columns())
      .value();
}

[[nodiscard]] std::vector<TemporalMutationDescriptor> mutations() {
  return {{{std::byte{1U}}, 100, 110, TemporalMutationKind::kOriginal},
          {{std::byte{2U}}, 200, 220, TemporalMutationKind::kCorrection}};
}

TEST(TemporalCommandAllocationFailureTest, ClassifiesEveryOwnedEncodeAllocation) {
  const columnar::OwnedColumnarBatch source = batch();
  const std::vector<TemporalMutationDescriptor> descriptors = mutations();
  bool succeeded = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    auto encoded = run_failure(
        fail_after, [&] { return encode_temporal_command_v1(source, descriptors, 1000); });
    if (encoded.has_value()) {
      succeeded = true;
      break;
    }
    EXPECT_EQ(encoded.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(encoded.error().message(), "temporal command encoding allocation failed");
  }
  EXPECT_TRUE(succeeded);
}

TEST(TemporalCommandAllocationFailureTest, ClassifiesEveryOwnedExactDecodeAllocation) {
  const columnar::OwnedColumnarBatch source = batch();
  const std::vector<TemporalMutationDescriptor> descriptors = mutations();
  const auto encoded = encode_temporal_command_v1(source, descriptors, 1000);
  ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();

  bool succeeded = false;
  for (std::size_t fail_after = 0U; fail_after < 64U; ++fail_after) {
    auto decoded =
        run_failure(fail_after, [&] { return decode_temporal_command_v1(encoded->bytes()); });
    if (decoded.has_value()) {
      EXPECT_EQ(decoded->mutations().size(), descriptors.size());
      succeeded = true;
      break;
    }
    EXPECT_EQ(decoded.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_EQ(decoded.error().message(), "temporal command decode allocation failed");
  }
  EXPECT_TRUE(succeeded);
}

} // namespace
} // namespace chronos::query
