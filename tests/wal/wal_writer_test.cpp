#include "chronos/wal/codec.hpp"
#include "chronos/wal/wal_paths.hpp"
#include "chronos/wal/wal_writer.hpp"
#include "wal/wal_writer_test_support.hpp"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <iterator>
#include <string>
#include <vector>

namespace chronos::wal {
namespace {

[[nodiscard]] std::vector<std::byte> read_bytes(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  const std::vector<char> characters{std::istreambuf_iterator<char>{input},
                                     std::istreambuf_iterator<char>{}};
  std::vector<std::byte> bytes(characters.size());
  for (std::size_t index = 0; index < characters.size(); ++index) {
    bytes[index] = static_cast<std::byte>(static_cast<unsigned char>(characters[index]));
  }
  return bytes;
}

TEST(WalLogIdGeneratorTest, SystemGeneratorReturnsANonzeroIdentity) {
  SystemWalLogIdGenerator generator;
  const common::Result<WalId> generated = generator.generate();
  ASSERT_TRUE(generated.has_value()) << generated.error().to_string();
  EXPECT_TRUE(generated->is_valid());
}

TEST(WalWriterTest, CreatesLockedInitialSegmentAndTracksWrittenAndDurableFrontiers) {
  test::TemporaryDirectory temporary{"chronos-wal-writer-test"};
  ASSERT_TRUE(temporary.valid());
  const WalId expected_id = test::make_wal_id();
  test::FixedWalIdGenerator generator{expected_id};

  common::Result<WalWriter> created =
      WalWriter::create_new({.directory_path = temporary.path().string()}, generator);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  WalWriter writer = std::move(*created);

  EXPECT_TRUE(writer.is_open());
  EXPECT_FALSE(writer.is_failed());
  EXPECT_EQ(generator.calls, 1U);
  EXPECT_EQ(writer.wal_id(), expected_id);
  EXPECT_EQ(writer.active_segment().header.segment_number, 1U);
  EXPECT_EQ(writer.active_segment().end_offset, kSegmentHeaderSize);
  EXPECT_EQ(writer.written_position(),
            (PhysicalWalPosition{.wal_id = expected_id, .segment_number = 1, .byte_offset = 64}));
  EXPECT_EQ(writer.durable_position(), writer.written_position());
  EXPECT_EQ(writer.written_record_sequence(), 0U);
  EXPECT_EQ(writer.durable_record_sequence(), 0U);
  ASSERT_TRUE(writer.next_record_sequence().has_value());
  EXPECT_EQ(*writer.next_record_sequence(), 1U);

  EXPECT_TRUE(std::filesystem::is_regular_file(temporary.path() / "LOCK"));
  EXPECT_TRUE(std::filesystem::is_regular_file(temporary.path() / "wal-00000000000000000001.cwal"));

  test::FixedWalIdGenerator competing_generator{test::make_wal_id(9U)};
  const common::Result<WalWriter> competing =
      WalWriter::create_new({.directory_path = temporary.path().string()}, competing_generator);
  ASSERT_FALSE(competing.has_value());
  EXPECT_EQ(competing.error().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(competing_generator.calls, 0U);

  const std::vector<std::byte> payload = test::make_application_payload(19U);
  const common::Result<WalAppendResult> appended = writer.append_application_entry(payload);
  ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
  ASSERT_TRUE(writer.next_record_sequence().has_value());
  EXPECT_EQ(*writer.next_record_sequence(), 2U);
  EXPECT_EQ(appended->record_sequence, 1U);
  EXPECT_EQ(appended->record_start.byte_offset, kSegmentHeaderSize);
  EXPECT_EQ(appended->record_end, writer.written_position());
  EXPECT_EQ(writer.durable_position().byte_offset, kSegmentHeaderSize);
  EXPECT_EQ(writer.durable_record_sequence(), 0U);

  const common::Result<PhysicalWalPosition> synchronized = writer.synchronize();
  ASSERT_TRUE(synchronized.has_value()) << synchronized.error().to_string();
  EXPECT_EQ(*synchronized, appended->record_end);
  EXPECT_EQ(writer.durable_record_sequence(), 1U);

  const std::vector<std::byte> file =
      read_bytes(temporary.path() / "wal-00000000000000000001.cwal");
  ASSERT_EQ(file.size(), appended->record_end.byte_offset);
  const common::Result<SegmentHeader> decoded_header = decode_segment_header(file);
  ASSERT_TRUE(decoded_header.has_value()) << decoded_header.error().to_string();
  EXPECT_EQ(decoded_header->wal_id, expected_id);
  const common::Result<DecodedRecord> decoded_record =
      decode_record(common::ByteView{file}.subspan(kSegmentHeaderSize));
  ASSERT_TRUE(decoded_record.has_value()) << decoded_record.error().to_string();
  EXPECT_EQ(decoded_record->header.record_sequence, 1U);
  EXPECT_TRUE(std::equal(decoded_record->payload.begin(), decoded_record->payload.end(),
                         payload.begin(), payload.end()));

  EXPECT_TRUE(writer.close().is_ok());
  EXPECT_FALSE(writer.is_open());
}

TEST(WalWriterTest, RejectsInvalidConfigurationGeneratorAndApplicationEnvelopeBeforeAppend) {
  test::FixedWalIdGenerator generator{test::make_wal_id()};
  const common::Result<WalWriter> bad_path =
      WalWriter::create_new({.directory_path = ""}, generator);
  ASSERT_FALSE(bad_path.has_value());
  EXPECT_EQ(bad_path.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_EQ(generator.calls, 0U);

  test::TemporaryDirectory temporary{"chronos-wal-writer-invalid-test"};
  ASSERT_TRUE(temporary.valid());
  test::FixedWalIdGenerator zero_generator{WalId{}};
  const common::Result<WalWriter> zero_id =
      WalWriter::create_new({.directory_path = temporary.path().string()}, zero_generator);
  ASSERT_FALSE(zero_id.has_value());
  EXPECT_EQ(zero_id.error().code(), common::StatusCode::kInvalidArgument);

  test::TemporaryDirectory valid_temporary{"chronos-wal-writer-invalid-payload-test"};
  test::FixedWalIdGenerator valid_generator{test::make_wal_id()};
  common::Result<WalWriter> created =
      WalWriter::create_new({.directory_path = valid_temporary.path().string()}, valid_generator);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  const PhysicalWalPosition initial = created->written_position();
  const std::vector<std::byte> invalid_payload(15U);
  const common::Result<WalAppendResult> invalid =
      created->append_application_entry(invalid_payload);
  ASSERT_FALSE(invalid.has_value());
  EXPECT_EQ(invalid.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_FALSE(created->is_failed());
  EXPECT_EQ(created->written_position(), initial);
}

TEST(WalWriterTest, NeverReplacesAnExistingInitialSegment) {
  test::TemporaryDirectory temporary{"chronos-wal-writer-existing-test"};
  ASSERT_TRUE(temporary.valid());
  test::FixedWalIdGenerator first_generator{test::make_wal_id(1U)};
  common::Result<WalWriter> first =
      WalWriter::create_new({.directory_path = temporary.path().string()}, first_generator);
  ASSERT_TRUE(first.has_value()) << first.error().to_string();
  ASSERT_TRUE(first->close().is_ok());

  test::FixedWalIdGenerator second_generator{test::make_wal_id(2U)};
  const common::Result<WalWriter> second =
      WalWriter::create_new({.directory_path = temporary.path().string()}, second_generator);
  ASSERT_FALSE(second.has_value());
  EXPECT_EQ(second.error().code(), common::StatusCode::kAlreadyExists);
  EXPECT_TRUE(std::filesystem::is_regular_file(temporary.path() / "wal-00000000000000000001.cwal"));
}

} // namespace
} // namespace chronos::wal
