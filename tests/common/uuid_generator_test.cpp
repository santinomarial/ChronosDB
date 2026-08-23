#include "chronos/common/uuid_generator.hpp"
#include "common/uuid_entropy_internal.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <gtest/gtest.h>
#include <set>
#include <span>
#include <string>

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

struct EntropyReadStep {
  std::ptrdiff_t byte_count{};
  int error_number{};
  std::byte fill{};
};

class ScriptedSystemEntropyReader final : public detail::UuidEntropyReader {
public:
  [[nodiscard]] detail::UuidEntropyReadOutcome
  read(const std::span<std::byte> destination) noexcept override {
    if (request_count < requested_sizes.size())
      requested_sizes[request_count] = destination.size();
    ++request_count;
    if (steps.empty())
      return {};
    const EntropyReadStep step = steps.front();
    steps.pop_front();
    if (step.byte_count > 0 && static_cast<std::size_t>(step.byte_count) <= destination.size()) {
      std::fill_n(destination.begin(), static_cast<std::size_t>(step.byte_count), step.fill);
    }
    return {.byte_count = step.byte_count, .error_number = step.error_number};
  }

  std::deque<EntropyReadStep> steps;
  std::array<std::size_t, 8U> requested_sizes{};
  std::size_t request_count{};
};

TEST(SystemUuidEntropySourceTest, CompletesPartialReadsAndRetriesInterruptions) {
  ScriptedSystemEntropyReader reader;
  reader.steps = {{.byte_count = 5, .fill = std::byte{1}},
                  {.byte_count = -1, .error_number = EINTR},
                  {.byte_count = 3, .fill = std::byte{2}},
                  {.byte_count = 8, .fill = std::byte{3}}};

  const auto entropy = detail::read_uuid_entropy_to_completion(reader);
  ASSERT_TRUE(entropy.has_value()) << entropy.error().to_string();
  EXPECT_EQ(reader.request_count, 4U);
  constexpr std::array<std::size_t, 4U> kExpectedRequests{16U, 11U, 11U, 8U};
  EXPECT_TRUE(std::equal(kExpectedRequests.begin(), kExpectedRequests.end(),
                         reader.requested_sizes.begin()));
  EXPECT_TRUE(std::all_of(entropy->begin(), entropy->begin() + 5,
                          [](const std::byte value) { return value == std::byte{1}; }));
  EXPECT_TRUE(std::all_of(entropy->begin() + 5, entropy->begin() + 8,
                          [](const std::byte value) { return value == std::byte{2}; }));
  EXPECT_TRUE(std::all_of(entropy->begin() + 8, entropy->end(),
                          [](const std::byte value) { return value == std::byte{3}; }));
}

TEST(SystemUuidEntropySourceTest, RejectsZeroProgressAsIoError) {
  ScriptedSystemEntropyReader reader;
  reader.steps = {{.byte_count = 0}};

  const auto entropy = detail::read_uuid_entropy_to_completion(reader);
  ASSERT_FALSE(entropy.has_value());
  EXPECT_EQ(entropy.error().code(), StatusCode::kIoError);
  EXPECT_NE(entropy.error().message().find("(errno " + std::to_string(EIO) + ")"),
            std::string::npos);
  EXPECT_EQ(reader.request_count, 1U);
  EXPECT_EQ(reader.requested_sizes.front(), 16U);
}

TEST(SystemUuidEntropySourceTest, PropagatesTerminalSystemError) {
  ScriptedSystemEntropyReader reader;
  reader.steps = {{.byte_count = -1, .error_number = EACCES}};

  const auto entropy = detail::read_uuid_entropy_to_completion(reader);
  ASSERT_FALSE(entropy.has_value());
  EXPECT_EQ(entropy.error().code(), StatusCode::kIoError);
  EXPECT_NE(entropy.error().message().find("(errno " + std::to_string(EACCES) + ")"),
            std::string::npos);
  EXPECT_EQ(reader.request_count, 1U);
  EXPECT_EQ(reader.requested_sizes.front(), 16U);
}

TEST(SystemUuidEntropySourceTest, RejectsInvalidProviderProgress) {
  ScriptedSystemEntropyReader reader;
  reader.steps = {{.byte_count = 17}};

  const auto entropy = detail::read_uuid_entropy_to_completion(reader);
  ASSERT_FALSE(entropy.has_value());
  EXPECT_EQ(entropy.error().code(), StatusCode::kIoError);
  EXPECT_NE(entropy.error().message().find("(errno " + std::to_string(EIO) + ")"),
            std::string::npos);
}

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
