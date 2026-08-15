#include "chronos/common/uuid_generator.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <set>

namespace chronos::common {
namespace {

[[nodiscard]] Uuid::Bytes bytes(const std::uint8_t tail) {
  Uuid::Bytes value{};
  value.back() = static_cast<std::byte>(tail);
  return value;
}

class ScriptedEntropySource final : public UuidEntropySource {
public:
  [[nodiscard]] Result<Uuid::Bytes> read() override {
    ++calls;
    if (!failure.is_ok())
      return make_unexpected(failure);
    if (next >= values.size())
      return Uuid::Bytes{};
    return values[next++];
  }

  std::array<Uuid::Bytes, 8U> values{};
  std::size_t next{};
  std::size_t calls{};
  Status failure;
};

TEST(SystemUuidGeneratorTest, ProducesNonnilDistinctDurableIdentities) {
  SystemUuidGenerator generator;
  std::set<Uuid> generated;
  for (std::size_t index = 0U; index < 32U; ++index) {
    Result<Uuid> id = generator.generate();
    ASSERT_TRUE(id.has_value()) << id.error().to_string();
    EXPECT_FALSE(id->is_nil());
    EXPECT_TRUE(generated.insert(*id).second);
  }
}

TEST(SystemUuidGeneratorTest, RetriesNilEntropyAndReturnsTheFirstNonnilValue) {
  ScriptedEntropySource entropy;
  entropy.values[2U] = bytes(7U);
  SystemUuidGenerator generator{entropy};

  const auto generated = generator.generate();
  ASSERT_TRUE(generated.has_value()) << generated.error().to_string();
  EXPECT_EQ(generated->bytes(), bytes(7U));
  EXPECT_EQ(entropy.calls, 3U);
}

TEST(SystemUuidGeneratorTest, BoundsRepeatedNilEntropy) {
  ScriptedEntropySource entropy;
  SystemUuidGenerator generator{entropy};

  const auto generated = generator.generate();
  ASSERT_FALSE(generated.has_value());
  EXPECT_EQ(generated.error().code(), StatusCode::kInternal);
  EXPECT_EQ(entropy.calls, entropy.values.size());
}

TEST(SystemUuidGeneratorTest, PropagatesEntropyFailureWithoutRetry) {
  ScriptedEntropySource entropy;
  entropy.failure = Status{StatusCode::kIoError, "injected entropy failure"};
  SystemUuidGenerator generator{entropy};

  const auto generated = generator.generate();
  ASSERT_FALSE(generated.has_value());
  EXPECT_EQ(generated.error(), entropy.failure);
  EXPECT_EQ(entropy.calls, 1U);
}

} // namespace
} // namespace chronos::common
