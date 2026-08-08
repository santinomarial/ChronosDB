#include "chronos/common/result.hpp"

#include <array>
#include <expected>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace chronos::common {
namespace {

static_assert(std::is_same_v<Result<int>, std::expected<int, Status>>);
static_assert(std::is_default_constructible_v<Result<void>>);

TEST(StatusTest, DefaultConstructionDeliberatelyRepresentsSuccess) {
  const Status status;
  EXPECT_TRUE(status.is_ok());
  EXPECT_EQ(status.code(), StatusCode::kOk);
  EXPECT_TRUE(status.message().empty());
  EXPECT_EQ(status.to_string(), "ok");
  EXPECT_EQ(status, Status::ok());
}

TEST(StatusTest, OkStatusCannotCarryContradictoryText) {
  const Status status{StatusCode::kOk, "this text must be discarded"};
  EXPECT_TRUE(status.is_ok());
  EXPECT_TRUE(status.message().empty());
  EXPECT_EQ(status.to_string(), "ok");
}

TEST(StatusTest, ProvidesStableNamesForEveryCode) {
  constexpr std::array<std::pair<StatusCode, std::string_view>, 13> kCases{{
      {StatusCode::kOk, "ok"},
      {StatusCode::kCancelled, "cancelled"},
      {StatusCode::kInvalidArgument, "invalid_argument"},
      {StatusCode::kOutOfRange, "out_of_range"},
      {StatusCode::kNotFound, "not_found"},
      {StatusCode::kAlreadyExists, "already_exists"},
      {StatusCode::kCorruption, "corruption"},
      {StatusCode::kIoError, "io_error"},
      {StatusCode::kResourceExhausted, "resource_exhausted"},
      {StatusCode::kUnavailable, "unavailable"},
      {StatusCode::kNotSupported, "not_supported"},
      {StatusCode::kUnauthenticated, "unauthenticated"},
      {StatusCode::kInternal, "internal"},
  }};

  for (const auto& [code, name] : kCases) {
    SCOPED_TRACE(name);
    EXPECT_EQ(status_code_name(code), name);
    if (code != StatusCode::kOk) {
      const Status status{code, "detail"};
      EXPECT_FALSE(status.is_ok());
      EXPECT_EQ(status.to_string(), std::string{name} + ": detail");
    }
  }
}

TEST(StatusTest, NonOkStatusAlwaysHasMessageAndOwnsItsLifetime) {
  std::string source = "owned diagnostic";
  const Status status{StatusCode::kCorruption, source};
  source.assign("changed");
  EXPECT_EQ(status.message(), "owned diagnostic");

  const Status fallback{StatusCode::kInternal, {}};
  EXPECT_EQ(fallback.message(), "internal");
}

TEST(ResultTest, ProvidesValueAndErrorAccess) {
  const Result<int> value = 42;
  ASSERT_TRUE(value.has_value());
  EXPECT_EQ(*value, 42);

  const Result<int> error =
      make_unexpected(Status{StatusCode::kInvalidArgument, "integer was rejected"});
  ASSERT_FALSE(error.has_value());
  EXPECT_EQ(error.error().code(), StatusCode::kInvalidArgument);
  EXPECT_EQ(error.error().message(), "integer was rejected");
}

TEST(ResultTest, SupportsVoidAndMoveOnlyValues) {
  const Result<void> success;
  EXPECT_TRUE(success.has_value());

  const Result<void> error = make_unexpected(Status{StatusCode::kCancelled, "cancelled by test"});
  ASSERT_FALSE(error.has_value());
  EXPECT_EQ(error.error().code(), StatusCode::kCancelled);

  Result<std::unique_ptr<int>> move_only = std::make_unique<int>(73);
  ASSERT_TRUE(move_only.has_value());
  std::unique_ptr<int> extracted = std::move(*move_only);
  ASSERT_NE(extracted, nullptr);
  EXPECT_EQ(*extracted, 73);
}

TEST(ResultTest, ProjectErrorHelperRejectsOkErrorState) {
  const Result<int> result = make_unexpected(Status::ok());
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), StatusCode::kInternal);
  EXPECT_FALSE(result.error().message().empty());
}

} // namespace
} // namespace chronos::common
