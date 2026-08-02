#include "chronos/wal/codec.hpp"
#include "chronos/wal/wal_writer.hpp"
#include "wal/wal_writer_test_support.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::wal {
namespace {

TEST(WalRotationTest, SynchronizesPriorSegmentAndNeverSplitsARecord) {
  test::TemporaryDirectory temporary{"chronos-wal-rotation-test"};
  ASSERT_TRUE(temporary.valid());
  test::FixedWalIdGenerator generator{test::make_wal_id()};
  common::Result<WalWriter> created =
      WalWriter::create_new({.directory_path = temporary.path().string()}, generator);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  WalWriter writer = std::move(*created);

  const std::vector<std::byte> maximum_payload =
      test::make_application_payload(kMaximumPayloadLength);
  for (std::uint64_t sequence = 1; sequence <= 3; ++sequence) {
    const common::Result<WalAppendResult> appended =
        writer.append_application_entry(maximum_payload);
    ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
    EXPECT_EQ(appended->record_sequence, sequence);
    EXPECT_EQ(appended->record_start.segment_number, 1U);
    EXPECT_EQ(appended->record_end.byte_offset,
              kSegmentHeaderSize + (sequence * kMaximumRecordLength));
  }

  const common::Result<WalAppendResult> rotated = writer.append_application_entry(maximum_payload);
  ASSERT_TRUE(rotated.has_value()) << rotated.error().to_string();
  EXPECT_EQ(rotated->record_sequence, 4U);
  EXPECT_EQ(rotated->record_start.segment_number, 2U);
  EXPECT_EQ(rotated->record_start.byte_offset, kSegmentHeaderSize);
  EXPECT_EQ(rotated->record_end.byte_offset, kSegmentHeaderSize + kMaximumRecordLength);
  EXPECT_EQ(writer.active_segment().header.first_record_sequence, 4U);
  EXPECT_EQ(writer.written_record_sequence(), 4U);
  EXPECT_EQ(writer.durable_record_sequence(), 3U);
  EXPECT_EQ(writer.durable_position().segment_number, 2U);
  EXPECT_EQ(writer.durable_position().byte_offset, kSegmentHeaderSize);

  EXPECT_EQ(std::filesystem::file_size(temporary.path() / "wal-00000000000000000001.cwal"),
            static_cast<std::uintmax_t>(kSegmentHeaderSize) +
                (3U * static_cast<std::uintmax_t>(kMaximumRecordLength)));
  EXPECT_EQ(std::filesystem::file_size(temporary.path() / "wal-00000000000000000002.cwal"),
            kSegmentHeaderSize + kMaximumRecordLength);

  const common::Result<PhysicalWalPosition> synchronized = writer.synchronize();
  ASSERT_TRUE(synchronized.has_value()) << synchronized.error().to_string();
  EXPECT_EQ(writer.durable_record_sequence(), 4U);
  EXPECT_EQ(writer.durable_position(), writer.written_position());
}

TEST(WalRotationTest, UsesASmallRuntimeTargetWithoutChangingSerializedSegmentHeaders) {
  test::TemporaryDirectory temporary{"chronos-wal-small-rotation-test"};
  ASSERT_TRUE(temporary.valid());
  test::FixedWalIdGenerator generator{test::make_wal_id()};
  common::Result<WalWriter> created =
      WalWriter::create_new({.directory_path = temporary.path().string(),
                             .target_segment_size = 192U,
                             .maximum_application_payload = kApplicationEnvelopeSize},
                            generator);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();

  const std::vector<std::byte> payload = test::make_application_payload(kApplicationEnvelopeSize);
  const common::Result<WalAppendResult> first = created->append_application_entry(payload);
  const common::Result<WalAppendResult> second = created->append_application_entry(payload);
  const common::Result<WalAppendResult> third = created->append_application_entry(payload);
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  ASSERT_TRUE(third.has_value());
  EXPECT_EQ(first->record_start.segment_number, 1U);
  EXPECT_EQ(second->record_end.byte_offset, 192U);
  EXPECT_EQ(third->record_start.segment_number, 2U);
  EXPECT_EQ(third->record_start.byte_offset, kSegmentHeaderSize);
  EXPECT_EQ(created->active_segment().header.first_record_sequence, 3U);
  EXPECT_EQ(created->durable_record_sequence(), 2U);

  const std::vector<std::byte> second_header = [&temporary] {
    std::ifstream input(temporary.path() / "wal-00000000000000000002.cwal", std::ios::binary);
    std::vector<char> bytes(kSegmentHeaderSize);
    input.read(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    std::vector<std::byte> result(bytes.size());
    std::transform(bytes.begin(), bytes.end(), result.begin(), [](const char value) {
      return static_cast<std::byte>(static_cast<unsigned char>(value));
    });
    return result;
  }();
  const common::Result<SegmentHeader> decoded = decode_segment_header(second_header);
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->segment_number, 2U);
}

} // namespace
} // namespace chronos::wal
