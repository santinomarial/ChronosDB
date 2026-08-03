#include "chronos/schema/utf8.hpp"

#include <array>
#include <gtest/gtest.h>
#include <string>
#include <string_view>

namespace chronos::schema {
namespace {

[[nodiscard]] std::string bytes(std::initializer_list<unsigned int> values) {
  std::string output;
  output.reserve(values.size());
  for (const unsigned int value : values) {
    output.push_back(static_cast<char>(value));
  }
  return output;
}

TEST(Utf8Test, AcceptsAsciiNullAndScalarValueBoundaries) {
  const std::array<std::string, 9> valid{{
      std::string{},
      std::string{"plain ASCII"},
      std::string{"a\0b", 3},
      bytes({0xc2, 0x80}),
      bytes({0xdf, 0xbf}),
      bytes({0xe0, 0xa0, 0x80}),
      bytes({0xef, 0xbf, 0xbf}),
      bytes({0xf0, 0x90, 0x80, 0x80}),
      bytes({0xf4, 0x8f, 0xbf, 0xbf}),
  }};

  for (const std::string& value : valid) {
    EXPECT_TRUE(is_valid_utf8(value));
  }
}

TEST(Utf8Test, RejectsOverlongSurrogateOutOfRangeAndLegacyForms) {
  const std::array<std::string, 12> invalid{{
      bytes({0x80}),
      bytes({0xc0, 0x80}),
      bytes({0xc1, 0xbf}),
      bytes({0xe0, 0x80, 0x80}),
      bytes({0xed, 0xa0, 0x80}),
      bytes({0xed, 0xbf, 0xbf}),
      bytes({0xf0, 0x80, 0x80, 0x80}),
      bytes({0xf4, 0x90, 0x80, 0x80}),
      bytes({0xf5, 0x80, 0x80, 0x80}),
      bytes({0xf8, 0x88, 0x80, 0x80, 0x80}),
      bytes({0xfe}),
      bytes({0xff}),
  }};

  for (const std::string& value : invalid) {
    EXPECT_FALSE(is_valid_utf8(value));
  }
}

TEST(Utf8Test, RejectsEveryTruncationAndBadContinuationPosition) {
  const std::array<std::string, 10> invalid{{
      bytes({0xc2}),
      bytes({0xc2, 0x20}),
      bytes({0xe2}),
      bytes({0xe2, 0x82}),
      bytes({0xe2, 0x20, 0xac}),
      bytes({0xe2, 0x82, 0x20}),
      bytes({0xf0}),
      bytes({0xf0, 0x90}),
      bytes({0xf0, 0x90, 0x80}),
      bytes({0xf0, 0x90, 0x20, 0x80}),
  }};

  for (const std::string& value : invalid) {
    EXPECT_FALSE(is_valid_utf8(value));
  }
}

TEST(Utf8Test, ValidatesMixedSequencesWithoutReadingPastBounds) {
  const std::string valid = std::string{"prefix"} + bytes({0xe2, 0x82, 0xac}) +
                            bytes({0xf0, 0x9f, 0x98, 0x80}) + "suffix";
  EXPECT_TRUE(is_valid_utf8(valid));

  std::string invalid = valid;
  invalid.pop_back();
  invalid.push_back(static_cast<char>(0x80));
  EXPECT_FALSE(is_valid_utf8(invalid));
}

} // namespace
} // namespace chronos::schema
