#include "chronos/columnar/columnar_batch_format.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/query/temporal_command.hpp"
#include "chronos/wal/application.hpp"
#include "columnar/columnar_test_support.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <limits>
#include <string_view>
#include <vector>

namespace chronos::query {
namespace {

void store_u16_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::uint16_t value) {
  bytes[offset] = static_cast<std::byte>(value & 0xffU);
  bytes[offset + 1U] = static_cast<std::byte>((value >> 8U) & 0xffU);
}

void store_u32_le(const common::MutableByteView bytes, const std::size_t offset,
                  const std::uint32_t value) {
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    bytes[offset + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
}

[[nodiscard]] std::uint32_t load_u32_le(const common::ByteView bytes, const std::size_t offset) {
  std::uint32_t value = 0U;
  for (std::size_t index = 0U; index < sizeof(value); ++index)
    value |= std::to_integer<std::uint32_t>(bytes[offset + index]) << (index * 8U);
  return value;
}

[[nodiscard]] std::vector<std::byte> valid_command() {
  const columnar::OwnedColumnarBatch batch =
      columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                           columnar::test::batch_columns())
          .value();
  const std::vector<TemporalMutationDescriptor> descriptors{
      {{std::byte{1U}}, 100, 110, TemporalMutationKind::kOriginal},
      {{std::byte{2U}}, 200, 220, TemporalMutationKind::kCorrection}};
  const EncodedTemporalCommand encoded =
      encode_temporal_command_v1(batch, descriptors, 1000).value();
  return {encoded.bytes().begin(), encoded.bytes().end()};
}

[[nodiscard]] common::MutableByteView body(std::vector<std::byte>& bytes) {
  return common::MutableByteView{bytes}.subspan(wal::kApplicationBodyOffset);
}

void refresh_command_checksums(const common::MutableByteView command) {
  constexpr std::size_t kHeaderCrcOffset = 88U;
  store_u32_le(command, kHeaderCrcOffset, 0U);
  store_u32_le(command, kHeaderCrcOffset,
               common::crc32c(common::ByteView{command}.first(kTemporalCommandHeaderSize)));
  const std::size_t trailer = command.size() - kTemporalCommandTrailerSize;
  store_u32_le(command, trailer, common::crc32c(common::ByteView{command}.first(trailer)));
}

void refresh_command_payload_checksums(const common::MutableByteView command) {
  constexpr std::size_t kBatchSizeOffset = 20U;
  constexpr std::size_t kMetadataSizeOffset = 28U;
  constexpr std::size_t kBatchCrcOffset = 80U;
  constexpr std::size_t kMetadataCrcOffset = 84U;
  const std::size_t batch_size = load_u32_le(command, kBatchSizeOffset);
  const std::size_t metadata_size = load_u32_le(command, kMetadataSizeOffset);
  const common::ByteView batch =
      common::ByteView{command}.subspan(kTemporalCommandHeaderSize, batch_size);
  const common::ByteView metadata =
      common::ByteView{command}.subspan(kTemporalCommandHeaderSize + batch_size, metadata_size);
  store_u32_le(command, kBatchCrcOffset, common::crc32c(batch));
  store_u32_le(command, kMetadataCrcOffset, common::crc32c(metadata));
  refresh_command_checksums(command);
}

[[nodiscard]] common::MutableByteView embedded_batch(const common::MutableByteView command) {
  constexpr std::size_t kBatchSizeOffset = 20U;
  return command.subspan(kTemporalCommandHeaderSize, load_u32_le(command, kBatchSizeOffset));
}

void refresh_nested_and_command_checksums(const common::MutableByteView command) {
  common::MutableByteView batch = embedded_batch(command);
  store_u32_le(
      batch, columnar::format::kHeaderCrc32cOffset,
      common::crc32c(common::ByteView{batch}.first(columnar::format::kHeaderCrc32cOffset)));
  const std::size_t trailer = batch.size() - columnar::format::kBatchTrailerLength;
  store_u32_le(batch, trailer, common::crc32c(common::ByteView{batch}.first(trailer)));
  refresh_command_payload_checksums(command);
}

void expect_failure(const common::ByteView bytes, const common::StatusCode code,
                    const std::string_view message) {
  const auto decoded = decode_temporal_command_v1(bytes);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), code) << decoded.error().to_string();
  EXPECT_EQ(decoded.error().message(), message);
}

TEST(TemporalCommandCorruptionTest, RejectsChecksumValidHostileFramingAndCountFields) {
  using Mutation = void (*)(common::MutableByteView);
  struct Case {
    const char* label;
    Mutation mutate;
  };
  constexpr std::array<Case, 7U> kCases{{
      {"header size", [](const common::MutableByteView bytes) { store_u32_le(bytes, 12U, 95U); }},
      {"total size", [](const common::MutableByteView bytes) { store_u32_le(bytes, 16U, 549U); }},
      {"batch size",
       [](const common::MutableByteView bytes) {
         store_u32_le(bytes, 20U, std::numeric_limits<std::uint32_t>::max());
       }},
      {"zero count", [](const common::MutableByteView bytes) { store_u32_le(bytes, 24U, 0U); }},
      {"count exceeds metadata minimum",
       [](const common::MutableByteView bytes) { store_u32_le(bytes, 24U, 3U); }},
      {"metadata size",
       [](const common::MutableByteView bytes) {
         store_u32_le(bytes, 28U, std::numeric_limits<std::uint32_t>::max());
       }},
      {"reserved", [](const common::MutableByteView bytes) { store_u32_le(bytes, 92U, 1U); }},
  }};

  for (const Case& test : kCases) {
    SCOPED_TRACE(test.label);
    std::vector<std::byte> bytes = valid_command();
    test.mutate(body(bytes));
    refresh_command_checksums(body(bytes));
    expect_failure(bytes, common::StatusCode::kCorruption, "temporal command header is invalid");
  }
}

TEST(TemporalCommandCorruptionTest, ClassifiesUnknownIdentityAndVersionsAsUnsupported) {
  struct ApplicationCase {
    const char* label;
    std::uint32_t format;
    std::uint32_t kind;
    std::uint64_t flags;
  };
  constexpr std::array<ApplicationCase, 3U> kApplicationCases{{
      {"format", kTemporalApplicationFormat + 1U, kTemporalApplicationKind, 0U},
      {"kind", kTemporalApplicationFormat, kTemporalApplicationKind + 1U, 0U},
      {"flags", kTemporalApplicationFormat, kTemporalApplicationKind, 1U},
  }};
  const std::vector<std::byte> canonical = valid_command();
  const common::ByteView canonical_body =
      common::ByteView{canonical}.subspan(wal::kApplicationBodyOffset);
  for (const ApplicationCase& test : kApplicationCases) {
    SCOPED_TRACE(test.label);
    const auto encoded = wal::encode_application_payload({.application_format = test.format,
                                                          .application_kind = test.kind,
                                                          .application_flags = test.flags,
                                                          .application_body = canonical_body});
    ASSERT_TRUE(encoded.has_value()) << encoded.error().to_string();
    expect_failure(encoded->bytes(), common::StatusCode::kNotSupported,
                   "application payload is not temporal v1");
  }

  using Mutation = void (*)(common::MutableByteView);
  struct VersionCase {
    const char* label;
    Mutation mutate;
  };
  constexpr std::array<VersionCase, 2U> kVersionCases{{
      {"major", [](const common::MutableByteView bytes) { store_u16_le(bytes, 8U, 2U); }},
      {"minor", [](const common::MutableByteView bytes) { store_u16_le(bytes, 10U, 1U); }},
  }};
  for (const VersionCase& test : kVersionCases) {
    SCOPED_TRACE(test.label);
    std::vector<std::byte> bytes = canonical;
    test.mutate(body(bytes));
    refresh_command_checksums(body(bytes));
    expect_failure(bytes, common::StatusCode::kNotSupported,
                   "temporal command version is unsupported");
  }
}

TEST(TemporalCommandCorruptionTest, EnforcesCallerLimitsBeforeDescriptorAllocation) {
  const std::vector<std::byte> bytes = valid_command();
  for (const TemporalCommandLimits limits : {
           TemporalCommandLimits{.maximum_mutations = 1U},
           TemporalCommandLimits{.maximum_metadata_bytes = 49U},
       }) {
    const auto decoded = decode_temporal_command_v1(bytes, limits);
    ASSERT_FALSE(decoded.has_value());
    EXPECT_EQ(decoded.error().code(), common::StatusCode::kCorruption);
    EXPECT_EQ(decoded.error().message(), "temporal command header is invalid");
  }

  TemporalCommandLimits batch_limits;
  batch_limits.batch.max_batch_length = 399U;
  const auto decoded = decode_temporal_command_v1(bytes, batch_limits);
  ASSERT_FALSE(decoded.has_value());
  EXPECT_EQ(decoded.error().code(), common::StatusCode::kResourceExhausted);
  EXPECT_EQ(decoded.error().message(), "columnar batch exceeds configured decode limits");
}

TEST(TemporalCommandCorruptionTest, RejectsChecksumValidHostileMutationMetadata) {
  using Mutation = void (*)(common::MutableByteView);
  struct Case {
    const char* label;
    Mutation mutate;
    const char* message;
  };
  constexpr std::size_t kFirstDescriptor = kTemporalCommandHeaderSize + 400U;
  constexpr std::size_t kSecondDescriptor = kFirstDescriptor + 25U;
  constexpr std::array<Case, 5U> kCases{{
      {"reserved bytes",
       [](const common::MutableByteView bytes) { bytes[kFirstDescriptor + 1U] = std::byte{1U}; },
       "temporal mutation metadata is invalid"},
      {"zero identity length",
       [](const common::MutableByteView bytes) { store_u32_le(bytes, kFirstDescriptor + 4U, 0U); },
       "temporal mutation metadata is invalid"},
      {"hostile identity length",
       [](const common::MutableByteView bytes) {
         store_u32_le(bytes, kFirstDescriptor + 4U, std::numeric_limits<std::uint32_t>::max());
       },
       "temporal mutation metadata is invalid"},
      {"unknown kind",
       [](const common::MutableByteView bytes) { bytes[kFirstDescriptor] = std::byte{5U}; },
       "temporal mutation identity or kind is invalid"},
      {"duplicate identity",
       [](const common::MutableByteView bytes) {
         bytes[kSecondDescriptor + kTemporalMutationMetadataSize] = std::byte{1U};
       },
       "temporal command repeats a logical identity"},
  }};

  for (const Case& test : kCases) {
    SCOPED_TRACE(test.label);
    std::vector<std::byte> bytes = valid_command();
    test.mutate(body(bytes));
    refresh_command_payload_checksums(body(bytes));
    expect_failure(bytes, common::StatusCode::kCorruption, test.message);
  }
}

TEST(TemporalCommandCorruptionTest, PropagatesChecksumValidNestedBatchFailures) {
  using Mutation = void (*)(common::MutableByteView);
  struct Case {
    const char* label;
    Mutation mutate;
    common::StatusCode code;
    const char* message;
  };
  constexpr std::array<Case, 7U> kCases{{
      {"magic", [](const common::MutableByteView bytes) { bytes[0] ^= std::byte{1U}; },
       common::StatusCode::kCorruption, "columnar batch magic mismatch"},
      {"version", [](const common::MutableByteView bytes) { store_u16_le(bytes, 8U, 2U); },
       common::StatusCode::kNotSupported, "columnar batch format version is unsupported"},
      {"header size", [](const common::MutableByteView bytes) { store_u32_le(bytes, 12U, 95U); },
       common::StatusCode::kCorruption, "columnar batch fixed layout fields are invalid"},
      {"row count", [](const common::MutableByteView bytes) { store_u32_le(bytes, 20U, 0U); },
       common::StatusCode::kCorruption, "columnar batch row or column count is outside v1 bounds"},
      {"column count", [](const common::MutableByteView bytes) { store_u32_le(bytes, 24U, 0U); },
       common::StatusCode::kCorruption, "columnar batch row or column count is outside v1 bounds"},
      {"reserved", [](const common::MutableByteView bytes) { store_u32_le(bytes, 92U, 1U); },
       common::StatusCode::kCorruption, "columnar batch header reserved field is nonzero"},
      {"terminal padding", [](const common::MutableByteView bytes) { bytes[392] = std::byte{1U}; },
       common::StatusCode::kCorruption, "columnar batch terminal padding is nonzero"},
  }};

  for (const Case& test : kCases) {
    SCOPED_TRACE(test.label);
    std::vector<std::byte> bytes = valid_command();
    common::MutableByteView batch = embedded_batch(body(bytes));
    test.mutate(batch);
    refresh_nested_and_command_checksums(body(bytes));
    expect_failure(bytes, test.code, test.message);
  }
}

} // namespace
} // namespace chronos::query
