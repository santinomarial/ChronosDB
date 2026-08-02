#include "chronos/wal/wal_writer.hpp"
#include "wal/wal_writer_internal.hpp"
#include "wal/wal_writer_test_support.hpp"

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <gtest/gtest.h>
#include <limits>
#include <string>
#include <vector>

namespace chronos::wal {
namespace {

[[nodiscard]] WalWriter make_injected_writer(test::ScriptedWalSyscalls& syscalls) {
  test::FixedWalIdGenerator generator{test::make_wal_id()};
  common::Result<WalWriter> created = detail::WalWriterTestAccess::create_new(
      {.directory_path = "/database/wal"}, generator, syscalls);
  EXPECT_TRUE(created.has_value())
      << (created.has_value() ? std::string{} : created.error().to_string());
  return created.has_value() ? std::move(*created) : WalWriter{};
}

TEST(WalWriterFailureTest, PartialRecordFailurePoisonsWriterAndPreventsEveryLaterOperation) {
  test::ScriptedWalSyscalls syscalls;
  WalWriter writer = make_injected_writer(syscalls);
  ASSERT_TRUE(writer.is_open());
  const std::size_t creation_write_count = static_cast<std::size_t>(
      std::count_if(syscalls.events.begin(), syscalls.events.end(),
                    [](const std::string& event) { return event.starts_with("pwrite:"); }));

  syscalls.pwrite_outcomes = {{5, 0}, {-1, EIO}};
  const std::vector<std::byte> payload = test::make_application_payload();
  const common::Result<WalAppendResult> failed = writer.append_application_entry(payload);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kIoError);
  EXPECT_NE(failed.error().message().find("after 5 of 64 bytes"), std::string::npos);
  EXPECT_TRUE(writer.is_failed());
  EXPECT_EQ(writer.failure_status(), failed.error());
  EXPECT_EQ(writer.written_position().byte_offset, kSegmentHeaderSize);
  EXPECT_EQ(writer.written_record_sequence(), 0U);

  const std::size_t events_after_failure = syscalls.events.size();
  const common::Result<WalAppendResult> later_append = writer.append_application_entry(payload);
  ASSERT_FALSE(later_append.has_value());
  EXPECT_EQ(later_append.error(), failed.error());
  const common::Result<PhysicalWalPosition> later_sync = writer.synchronize();
  ASSERT_FALSE(later_sync.has_value());
  EXPECT_EQ(later_sync.error(), failed.error());
  EXPECT_EQ(syscalls.events.size(), events_after_failure);
  EXPECT_EQ(static_cast<std::size_t>(std::count_if(
                syscalls.events.begin(), syscalls.events.end(),
                [](const std::string& event) { return event.starts_with("pwrite:"); })),
            creation_write_count + 2U);
}

TEST(WalWriterFailureTest, SynchronizationFailurePreservesDurableFrontierAndPoisonsWriter) {
  test::ScriptedWalSyscalls syscalls;
  WalWriter writer = make_injected_writer(syscalls);
  ASSERT_TRUE(writer.is_open());
  const std::vector<std::byte> payload = test::make_application_payload();
  const common::Result<WalAppendResult> appended = writer.append_application_entry(payload);
  ASSERT_TRUE(appended.has_value()) << appended.error().to_string();
  EXPECT_EQ(writer.written_record_sequence(), 1U);
  EXPECT_EQ(writer.durable_record_sequence(), 0U);

  syscalls.fdatasync_outcomes = {{-1, EIO}};
  const common::Result<PhysicalWalPosition> synchronized = writer.synchronize();
  ASSERT_FALSE(synchronized.has_value());
  EXPECT_EQ(synchronized.error().code(), common::StatusCode::kIoError);
  EXPECT_TRUE(writer.is_failed());
  EXPECT_EQ(writer.durable_record_sequence(), 0U);
  EXPECT_EQ(writer.durable_position().byte_offset, kSegmentHeaderSize);

  const std::size_t event_count = syscalls.events.size();
  EXPECT_FALSE(writer.append_application_entry(payload).has_value());
  EXPECT_EQ(syscalls.events.size(), event_count);
}

TEST(WalWriterFailureTest, RotationSyncFailureCannotCreateOrAppendToASuccessor) {
  test::ScriptedWalSyscalls syscalls;
  WalWriter writer = make_injected_writer(syscalls);
  ASSERT_TRUE(writer.is_open());
  const std::vector<std::byte> maximum_payload =
      test::make_application_payload(kMaximumPayloadLength);
  for (int index = 0; index < 3; ++index) {
    ASSERT_TRUE(writer.append_application_entry(maximum_payload).has_value());
  }

  const std::size_t open_count_before = static_cast<std::size_t>(
      std::count_if(syscalls.events.begin(), syscalls.events.end(),
                    [](const std::string& event) { return event.starts_with("open_at:"); }));
  syscalls.fdatasync_outcomes = {{-1, EIO}};
  const common::Result<WalAppendResult> rotation = writer.append_application_entry(maximum_payload);
  ASSERT_FALSE(rotation.has_value());
  EXPECT_EQ(rotation.error().code(), common::StatusCode::kIoError);
  EXPECT_TRUE(writer.is_failed());
  EXPECT_EQ(writer.active_segment().header.segment_number, 1U);
  EXPECT_EQ(writer.written_record_sequence(), 3U);
  EXPECT_EQ(static_cast<std::size_t>(std::count_if(
                syscalls.events.begin(), syscalls.events.end(),
                [](const std::string& event) { return event.starts_with("open_at:"); })),
            open_count_before);
}

TEST(WalWriterFailureTest, SuccessorInstallationFailurePoisonsTheSynchronizedWriter) {
  test::ScriptedWalSyscalls syscalls;
  WalWriter writer = make_injected_writer(syscalls);
  ASSERT_TRUE(writer.is_open());
  const std::vector<std::byte> maximum_payload =
      test::make_application_payload(kMaximumPayloadLength);
  for (int index = 0; index < 3; ++index) {
    ASSERT_TRUE(writer.append_application_entry(maximum_payload).has_value());
  }

  syscalls.rename_outcomes = {{-1, EIO}};
  const common::Result<WalAppendResult> rotation = writer.append_application_entry(maximum_payload);
  ASSERT_FALSE(rotation.has_value());
  EXPECT_EQ(rotation.error().code(), common::StatusCode::kIoError);
  EXPECT_TRUE(writer.is_failed());
  EXPECT_EQ(writer.active_segment().header.segment_number, 1U);
  EXPECT_EQ(writer.written_record_sequence(), 3U);
  EXPECT_EQ(writer.durable_record_sequence(), 3U);
  EXPECT_TRUE(std::any_of(syscalls.events.begin(), syscalls.events.end(),
                          [](const std::string& event) { return event.starts_with("rename:"); }));

  const std::size_t event_count = syscalls.events.size();
  EXPECT_FALSE(writer.append_application_entry(maximum_payload).has_value());
  EXPECT_EQ(syscalls.events.size(), event_count);
}

TEST(WalWriterFailureTest, SuccessorDirectorySyncFailurePoisonsAfterRename) {
  test::ScriptedWalSyscalls syscalls;
  WalWriter writer = make_injected_writer(syscalls);
  ASSERT_TRUE(writer.is_open());
  const std::vector<std::byte> maximum_payload =
      test::make_application_payload(kMaximumPayloadLength);
  for (int index = 0; index < 3; ++index) {
    ASSERT_TRUE(writer.append_application_entry(maximum_payload).has_value());
  }

  syscalls.fsync_outcomes = {{0, 0}, {-1, EIO}};
  const common::Result<WalAppendResult> rotation = writer.append_application_entry(maximum_payload);
  ASSERT_FALSE(rotation.has_value());
  EXPECT_EQ(rotation.error().code(), common::StatusCode::kIoError);
  EXPECT_NE(rotation.error().message().find("synchronize WAL directory after install"),
            std::string::npos);
  EXPECT_TRUE(writer.is_failed());
  EXPECT_EQ(writer.active_segment().header.segment_number, 1U);
  EXPECT_EQ(writer.written_record_sequence(), 3U);
  EXPECT_EQ(writer.durable_record_sequence(), 3U);
  EXPECT_TRUE(std::any_of(syscalls.events.begin(), syscalls.events.end(),
                          [](const std::string& event) { return event.starts_with("rename:"); }));

  const std::size_t event_count = syscalls.events.size();
  const common::Result<PhysicalWalPosition> later = writer.synchronize();
  ASSERT_FALSE(later.has_value());
  EXPECT_EQ(later.error(), rotation.error());
  EXPECT_EQ(syscalls.events.size(), event_count);
}

TEST(WalWriterFailureTest, RecordAndSegmentSequenceExhaustionHaveDistinctDiagnostics) {
  {
    test::ScriptedWalSyscalls syscalls;
    WalWriter writer = make_injected_writer(syscalls);
    ASSERT_TRUE(writer.is_open());
    detail::WalWriterTestAccess::set_sequence_state(
        writer, std::numeric_limits<std::uint64_t>::max(), false);
    const std::vector<std::byte> payload = test::make_application_payload();
    const common::Result<WalAppendResult> terminal = writer.append_application_entry(payload);
    ASSERT_TRUE(terminal.has_value()) << terminal.error().to_string();
    EXPECT_EQ(terminal->record_sequence, std::numeric_limits<std::uint64_t>::max());

    const common::Result<WalAppendResult> exhausted = writer.append_application_entry(payload);
    ASSERT_FALSE(exhausted.has_value());
    EXPECT_EQ(exhausted.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_NE(exhausted.error().message().find("record sequence"), std::string::npos);
    EXPECT_EQ(exhausted.error().message().find("segment sequence"), std::string::npos);
    EXPECT_FALSE(writer.is_failed());
  }
  {
    test::ScriptedWalSyscalls syscalls;
    test::FixedWalIdGenerator generator{test::make_wal_id()};
    common::Result<WalWriter> created = detail::WalWriterTestAccess::create_new(
        {.directory_path = "/database/wal",
         .target_segment_size = kSegmentHeaderSize + 64U,
         .maximum_application_payload = kApplicationEnvelopeSize},
        generator, syscalls);
    ASSERT_TRUE(created.has_value()) << created.error().to_string();
    detail::WalWriterTestAccess::set_active_segment_number(
        *created, std::numeric_limits<std::uint64_t>::max());
    const std::vector<std::byte> payload = test::make_application_payload();
    ASSERT_TRUE(created->append_application_entry(payload).has_value());

    const common::Result<WalAppendResult> exhausted = created->append_application_entry(payload);
    ASSERT_FALSE(exhausted.has_value());
    EXPECT_EQ(exhausted.error().code(), common::StatusCode::kResourceExhausted);
    EXPECT_NE(exhausted.error().message().find("segment sequence"), std::string::npos);
    EXPECT_EQ(exhausted.error().message().find("record sequence"), std::string::npos);
    EXPECT_FALSE(created->is_failed());
  }
}

TEST(WalWriterFailureTest, InvalidInternalPositionPoisonsAndPreservesTheRootCause) {
  test::ScriptedWalSyscalls syscalls;
  WalWriter writer = make_injected_writer(syscalls);
  ASSERT_TRUE(writer.is_open());
  detail::WalWriterTestAccess::set_active_end_offset(writer, kSegmentSizeLimit + 8U);
  const std::size_t event_count = syscalls.events.size();

  const common::Result<WalAppendResult> failed =
      writer.append_application_entry(test::make_application_payload());
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kInternal);
  EXPECT_NE(failed.error().message().find("active WAL segment end"), std::string::npos);
  EXPECT_TRUE(writer.is_failed());
  EXPECT_EQ(syscalls.events.size(), event_count);

  const common::Result<PhysicalWalPosition> later = writer.synchronize();
  ASSERT_FALSE(later.has_value());
  EXPECT_EQ(later.error(), failed.error());
  EXPECT_EQ(syscalls.events.size(), event_count);
}

TEST(WalWriterFailureTest, SynchronizeRejectsInvalidInternalPositionBeforeIo) {
  test::ScriptedWalSyscalls syscalls;
  test::FixedWalIdGenerator generator{test::make_wal_id()};
  common::Result<WalWriter> created = detail::WalWriterTestAccess::create_new(
      {.directory_path = "/database/wal"}, generator, syscalls);
  ASSERT_TRUE(created.has_value()) << created.error().to_string();
  WalWriter writer = std::move(*created);
  detail::WalWriterTestAccess::set_active_end_offset(writer, kSegmentHeaderSize + 1U);
  syscalls.events.clear();

  const common::Result<PhysicalWalPosition> synchronized = writer.synchronize();
  ASSERT_FALSE(synchronized.has_value());
  EXPECT_EQ(synchronized.error().code(), common::StatusCode::kInternal);
  EXPECT_TRUE(writer.is_failed());
  EXPECT_EQ(writer.durable_position().byte_offset, kSegmentHeaderSize);
  EXPECT_TRUE(syscalls.events.empty());

  const common::Result<PhysicalWalPosition> repeated = writer.synchronize();
  ASSERT_FALSE(repeated.has_value());
  EXPECT_EQ(repeated.error(), synchronized.error());
  EXPECT_TRUE(syscalls.events.empty());
}

} // namespace
} // namespace chronos::wal
