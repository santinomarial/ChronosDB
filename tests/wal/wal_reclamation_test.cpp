#include "chronos/wal/wal_writer.hpp"
#include "io/posix_syscalls.hpp"
#include "wal/wal_recovery_test_support.hpp"
#include "wal/wal_writer_internal.hpp"

#include <cerrno>
#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <vector>

namespace chronos::wal {
namespace {

class FailingDirectorySyncSyscalls final : public io::detail::PosixSyscalls {
public:
  explicit FailingDirectorySyncSyscalls(const std::size_t fail_call)
      : delegate_(io::detail::system_posix_syscalls()), fail_call_(fail_call) {}

  int open_directory(const char* path, const int flags) override {
    return delegate_.open_directory(path, flags);
  }
  int open_at(const io::detail::OpenAtRequest& request) override {
    return delegate_.open_at(request);
  }
  int mkdir_at(const io::detail::MkdirAtRequest& request) override {
    return delegate_.mkdir_at(request);
  }
  ssize_t pread(const io::detail::ReadAtRequest& request) override {
    return delegate_.pread(request);
  }
  ssize_t pwrite(const io::detail::WriteAtRequest& request) override {
    return delegate_.pwrite(request);
  }
  int fstat(const int descriptor, struct stat* metadata) override {
    return delegate_.fstat(descriptor, metadata);
  }
  int ftruncate(const io::detail::TruncateRequest& request) override {
    return delegate_.ftruncate(request);
  }
  int fdatasync(const int descriptor) override {
    return delegate_.fdatasync(descriptor);
  }
  int fsync(const int descriptor) override {
    ++fsync_calls_;
    if (fsync_calls_ == fail_call_) {
      errno = EIO;
      return -1;
    }
    return delegate_.fsync(descriptor);
  }
  int rename_no_replace(const io::detail::RenameAtRequest& request) override {
    return delegate_.rename_no_replace(request);
  }
  int try_lock_exclusive(const int descriptor) override {
    return delegate_.try_lock_exclusive(descriptor);
  }
  int list_directory_entries(const int descriptor,
                             std::vector<io::DirectoryEntry>& entries) override {
    return delegate_.list_directory_entries(descriptor, entries);
  }
  int unlink_at(const int directory_descriptor, const char* const name) override {
    return delegate_.unlink_at(directory_descriptor, name);
  }
  int close(const int descriptor) override {
    return delegate_.close(descriptor);
  }

private:
  io::detail::PosixSyscalls& delegate_;
  std::size_t fail_call_{};
  std::size_t fsync_calls_{};
};

TEST(WalReclamationTest, RemovesOnlyCoveredClosedSegmentsAndConvergesIdempotently) {
  test::TemporaryDirectory temporary{"chronos-wal-reclaim-prefix"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(),
                   {.record_count = 5U, .target_segment_size = kSegmentHeaderSize + 64U});
  test::CollectingReplaySink sink;
  common::Result<WalWriter> writer =
      WalWriter::open_existing_from_checkpoint({.directory_path = temporary.path().string()}, {},
                                               {.wal_id = test::make_wal_id(),
                                                .record_sequence = 0U,
                                                .segment_number = kFirstSegmentNumber,
                                                .byte_offset = kSegmentHeaderSize},
                                               sink);
  ASSERT_TRUE(writer.has_value()) << writer.error().to_string();

  const WalReplayCheckpoint checkpoint{.wal_id = test::make_wal_id(),
                                       .record_sequence = 3U,
                                       .segment_number = 3U,
                                       .byte_offset = kSegmentHeaderSize + 64U};
  const common::Result<WalSegmentReclamationReport> reclaimed =
      writer->reclaim_checkpointed_segments(checkpoint);
  ASSERT_TRUE(reclaimed.has_value()) << reclaimed.error().to_string();
  EXPECT_EQ(reclaimed->removed_segment_count, 3U);
  EXPECT_EQ(reclaimed->removed_physical_bytes, 3U * (kSegmentHeaderSize + 64U));
  EXPECT_EQ(reclaimed->directory_sync_count, 1U);
  for (std::uint64_t number = 1U; number <= 3U; ++number) {
    EXPECT_FALSE(std::filesystem::exists(
        temporary.path() / ("wal-0000000000000000000" + std::to_string(number) + ".cwal")));
  }
  EXPECT_TRUE(std::filesystem::exists(temporary.path() / "wal-00000000000000000004.cwal"));
  EXPECT_TRUE(std::filesystem::exists(temporary.path() / "wal-00000000000000000005.cwal"));

  const common::Result<WalSegmentReclamationReport> repeated =
      writer->reclaim_checkpointed_segments(checkpoint);
  ASSERT_TRUE(repeated.has_value()) << repeated.error().to_string();
  EXPECT_EQ(repeated->removed_segment_count, 0U);
  EXPECT_EQ(repeated->directory_sync_count, 0U);
  EXPECT_EQ(writer->reclamation_metrics(),
            (WalSegmentReclamationMetrics{.attempts = 2U,
                                          .failures = 0U,
                                          .removed_segment_count = 3U,
                                          .removed_physical_bytes = 3U * (kSegmentHeaderSize + 64U),
                                          .directory_sync_count = 1U}));
  EXPECT_TRUE(writer->close().is_ok());

  test::CollectingReplaySink recovered;
  common::Result<WalWriter> reopened = WalWriter::open_existing_from_checkpoint(
      {.directory_path = temporary.path().string()}, {}, checkpoint, recovered);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(recovered.replay_sequences, (std::vector<std::uint64_t>{4U, 5U}));
  EXPECT_TRUE(reopened->close().is_ok());
}

TEST(WalReclamationTest, RetainsPartiallyCoveredAndActiveSegments) {
  test::TemporaryDirectory temporary{"chronos-wal-reclaim-partial"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(),
                   {.record_count = 5U, .target_segment_size = kSegmentHeaderSize + 128U});
  test::CollectingReplaySink sink;
  common::Result<WalWriter> writer =
      WalWriter::open_existing({.directory_path = temporary.path().string()}, {}, sink);
  ASSERT_TRUE(writer.has_value()) << writer.error().to_string();

  const common::Result<WalSegmentReclamationReport> partial =
      writer->reclaim_checkpointed_segments({.wal_id = test::make_wal_id(),
                                             .record_sequence = 1U,
                                             .segment_number = 1U,
                                             .byte_offset = kSegmentHeaderSize + 64U});
  ASSERT_TRUE(partial.has_value()) << partial.error().to_string();
  EXPECT_EQ(partial->removed_segment_count, 0U);
  EXPECT_TRUE(std::filesystem::exists(temporary.path() / "wal-00000000000000000001.cwal"));

  const common::Result<WalSegmentReclamationReport> complete =
      writer->reclaim_checkpointed_segments({.wal_id = test::make_wal_id(),
                                             .record_sequence = 2U,
                                             .segment_number = 1U,
                                             .byte_offset = kSegmentHeaderSize + 128U});
  ASSERT_TRUE(complete.has_value()) << complete.error().to_string();
  EXPECT_EQ(complete->removed_segment_count, 1U);
  EXPECT_FALSE(std::filesystem::exists(temporary.path() / "wal-00000000000000000001.cwal"));

  const common::Result<WalSegmentReclamationReport> through_active =
      writer->reclaim_checkpointed_segments({.wal_id = test::make_wal_id(),
                                             .record_sequence = 5U,
                                             .segment_number = 3U,
                                             .byte_offset = kSegmentHeaderSize + 64U});
  ASSERT_TRUE(through_active.has_value()) << through_active.error().to_string();
  EXPECT_EQ(through_active->removed_segment_count, 1U);
  EXPECT_TRUE(std::filesystem::exists(temporary.path() / "wal-00000000000000000003.cwal"));
  EXPECT_EQ(writer->active_segment().header.segment_number, 3U);
  EXPECT_TRUE(writer->close().is_ok());
}

TEST(WalReclamationTest, RejectsUnprovedCheckpointBeforeAnyNamespaceMutation) {
  test::TemporaryDirectory temporary{"chronos-wal-reclaim-invalid"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(),
                   {.record_count = 3U, .target_segment_size = kSegmentHeaderSize + 64U});
  test::CollectingReplaySink sink;
  common::Result<WalWriter> writer =
      WalWriter::open_existing({.directory_path = temporary.path().string()}, {}, sink);
  ASSERT_TRUE(writer.has_value()) << writer.error().to_string();
  WalId wrong = test::make_wal_id();
  wrong.bytes.front() ^= std::byte{0x01U};

  const common::Result<WalSegmentReclamationReport> rejected =
      writer->reclaim_checkpointed_segments({.wal_id = wrong,
                                             .record_sequence = 2U,
                                             .segment_number = 2U,
                                             .byte_offset = kSegmentHeaderSize + 64U});
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kInvalidArgument);
  EXPECT_FALSE(writer->is_failed());
  EXPECT_TRUE(std::filesystem::exists(temporary.path() / "wal-00000000000000000001.cwal"));
  EXPECT_TRUE(writer->append_application_entry(test::make_application_payload()).has_value());
  EXPECT_TRUE(writer->close().is_ok());
}

TEST(WalReclamationTest, CorruptCandidateFailsClosedBeforeDeletingAnySegment) {
  test::TemporaryDirectory temporary{"chronos-wal-reclaim-corrupt"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(),
                   {.record_count = 4U, .target_segment_size = kSegmentHeaderSize + 64U});
  test::CollectingReplaySink sink;
  common::Result<WalWriter> writer =
      WalWriter::open_existing({.directory_path = temporary.path().string()}, {}, sink);
  ASSERT_TRUE(writer.has_value()) << writer.error().to_string();
  const std::filesystem::path first = temporary.path() / "wal-00000000000000000001.cwal";
  std::vector<std::byte> bytes = test::read_file(first);
  ASSERT_EQ(bytes.size(), kSegmentHeaderSize + 64U);
  bytes.back() ^= std::byte{0x01U};
  test::write_file(first, bytes);

  const common::Result<WalSegmentReclamationReport> rejected =
      writer->reclaim_checkpointed_segments({.wal_id = test::make_wal_id(),
                                             .record_sequence = 2U,
                                             .segment_number = 2U,
                                             .byte_offset = kSegmentHeaderSize + 64U});
  ASSERT_FALSE(rejected.has_value());
  EXPECT_EQ(rejected.error().code(), common::StatusCode::kCorruption);
  EXPECT_TRUE(writer->is_failed());
  EXPECT_TRUE(std::filesystem::exists(first));
  EXPECT_TRUE(std::filesystem::exists(temporary.path() / "wal-00000000000000000002.cwal"));
  EXPECT_EQ(writer->reclamation_metrics().removed_segment_count, 0U);
  static_cast<void>(writer->close());
}

TEST(WalReclamationTest, DirectorySyncFailurePoisonsWithoutAdvancingDurabilityMetrics) {
  test::TemporaryDirectory temporary{"chronos-wal-reclaim-sync-failure"};
  ASSERT_TRUE(temporary.valid());
  test::create_wal(temporary.path(),
                   {.record_count = 3U, .target_segment_size = kSegmentHeaderSize + 64U});
  FailingDirectorySyncSyscalls syscalls{3U};
  test::CollectingReplaySink sink;
  common::Result<WalWriter> writer = detail::WalWriterTestAccess::open_existing(
      {.directory_path = temporary.path().string()}, {}, sink, syscalls);
  ASSERT_TRUE(writer.has_value()) << writer.error().to_string();
  const WalReplayCheckpoint checkpoint{.wal_id = test::make_wal_id(),
                                       .record_sequence = 2U,
                                       .segment_number = 2U,
                                       .byte_offset = kSegmentHeaderSize + 64U};

  const common::Result<WalSegmentReclamationReport> failed =
      writer->reclaim_checkpointed_segments(checkpoint);
  ASSERT_FALSE(failed.has_value());
  EXPECT_EQ(failed.error().code(), common::StatusCode::kIoError);
  EXPECT_TRUE(writer->is_failed());
  EXPECT_EQ(writer->reclamation_metrics().removed_segment_count, 0U);
  EXPECT_EQ(writer->reclamation_metrics().directory_sync_count, 0U);
  EXPECT_FALSE(writer->append_application_entry(test::make_application_payload()).has_value());
  static_cast<void>(writer->close());

  test::CollectingReplaySink recovered;
  common::Result<WalWriter> reopened = WalWriter::open_existing_from_checkpoint(
      {.directory_path = temporary.path().string()}, {}, checkpoint, recovered);
  ASSERT_TRUE(reopened.has_value()) << reopened.error().to_string();
  EXPECT_EQ(recovered.replay_sequences, (std::vector<std::uint64_t>{3U}));
  EXPECT_TRUE(reopened->close().is_ok());
}

} // namespace
} // namespace chronos::wal
