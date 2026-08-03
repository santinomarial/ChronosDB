#ifndef CHRONOS_TESTS_WAL_WAL_CRASH_TEST_SUPPORT_HPP_
#define CHRONOS_TESTS_WAL_WAL_CRASH_TEST_SUPPORT_HPP_

#include "chronos/common/result.hpp"
#include "chronos/io/posix_io.hpp"
#include "chronos/wal/wal_paths.hpp"
#include "chronos/wal/wal_recovery.hpp"
#include "chronos/wal/wal_scan.hpp"
#include "wal/wal_crash_protocol.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <set>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::wal::test {

class CrashWalDirectory {
public:
  explicit CrashWalDirectory(const std::string_view prefix) {
    std::string pattern =
        (std::filesystem::temp_directory_path() / (std::string{prefix} + "-XXXXXX")).string();
    char* const created = ::mkdtemp(pattern.data());
    if (created == nullptr) {
      return;
    }
    root_ = created;
    wal_ = root_ / "wal";
    std::error_code error;
    if (!std::filesystem::create_directory(wal_, error) || error) {
      return;
    }
    common::Result<io::PosixDirectory> root = io::PosixDirectory::open(root_.string());
    if (!root.has_value()) {
      return;
    }
    const common::Status sync_status = root->sync();
    const common::Status close_status = root->close();
    valid_ = sync_status.is_ok() && close_status.is_ok();
  }

  ~CrashWalDirectory() {
    if (!root_.empty()) {
      std::error_code ignored;
      std::filesystem::remove_all(root_, ignored);
    }
  }

  CrashWalDirectory(const CrashWalDirectory&) = delete;
  CrashWalDirectory& operator=(const CrashWalDirectory&) = delete;

  [[nodiscard]] bool valid() const noexcept {
    return valid_;
  }

  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return wal_;
  }

private:
  std::filesystem::path root_;
  std::filesystem::path wal_;
  bool valid_{false};
};

struct RecoveredCrashRecord {
  std::uint64_t sequence{};
  std::uint64_t request_id{};

  friend bool operator==(const RecoveredCrashRecord&, const RecoveredCrashRecord&) = default;
};

class CrashReplaySink final : public WalReplaySink {
public:
  [[nodiscard]] common::Status preflight(const WalReplayRecord& record) override {
    const common::Result<std::uint64_t> request_id = crash_payload_request_id(record.payload);
    return request_id.has_value() ? common::Status::ok() : request_id.error();
  }

  [[nodiscard]] common::Status replay(const WalReplayRecord& record) override {
    const common::Result<std::uint64_t> request_id = crash_payload_request_id(record.payload);
    if (!request_id.has_value()) {
      return request_id.error();
    }
    records.push_back(
        RecoveredCrashRecord{.sequence = record.header.record_sequence, .request_id = *request_id});
    return common::Status::ok();
  }

  std::vector<RecoveredCrashRecord> records;
};

struct CrashRecoveryResult {
  WalRecoveryReport report;
  std::vector<RecoveredCrashRecord> records;
};

[[nodiscard]] inline common::Result<CrashRecoveryResult>
inspect_crash_wal(const std::filesystem::path& directory) {
  CrashReplaySink sink;
  const common::Result<WalRecoveryReport> report = inspect_wal(directory.string(), sink);
  if (!report.has_value()) {
    return common::make_unexpected(report.error());
  }
  return CrashRecoveryResult{.report = *report, .records = std::move(sink.records)};
}

[[nodiscard]] inline common::Result<CrashRecoveryResult>
recover_crash_wal(const std::filesystem::path& directory, const bool repair_tail) {
  CrashReplaySink sink;
  const WalWriterConfig config{.directory_path = directory.string(),
                               .maximum_application_payload = kCrashPayloadSize};
  const common::Result<WalRecoveryReport> report =
      recover_wal(config, {.repair_incomplete_final_tail = repair_tail}, sink);
  if (!report.has_value()) {
    return common::make_unexpected(report.error());
  }
  return CrashRecoveryResult{.report = *report, .records = std::move(sink.records)};
}

[[nodiscard]] inline common::Status
validate_crash_prefix(const std::vector<RecoveredCrashRecord>& records) {
  std::set<std::uint64_t> identities;
  for (std::size_t index = 0; index < records.size(); ++index) {
    const std::uint64_t expected_sequence = static_cast<std::uint64_t>(index) + 1U;
    if (records[index].sequence != expected_sequence) {
      return common::Status{common::StatusCode::kCorruption,
                            "recovered crash records have a sequence gap"};
    }
    if (!identities.insert(records[index].request_id).second) {
      return common::Status{common::StatusCode::kCorruption,
                            "recovered crash records contain a duplicate request identity"};
    }
  }
  return common::Status::ok();
}

[[nodiscard]] inline std::vector<std::byte> read_crash_file(const std::filesystem::path& path) {
  std::ifstream input{path, std::ios::binary};
  const std::vector<char> characters{std::istreambuf_iterator<char>{input},
                                     std::istreambuf_iterator<char>{}};
  std::vector<std::byte> bytes;
  bytes.reserve(characters.size());
  for (const char character : characters) {
    bytes.push_back(static_cast<std::byte>(static_cast<unsigned char>(character)));
  }
  return bytes;
}

struct CrashFileImage {
  std::string name;
  std::vector<std::byte> bytes;

  friend bool operator==(const CrashFileImage&, const CrashFileImage&) = default;
};

[[nodiscard]] inline std::vector<CrashFileImage>
snapshot_crash_wal(const std::filesystem::path& directory) {
  std::vector<CrashFileImage> image;
  for (const std::filesystem::directory_entry& entry :
       std::filesystem::directory_iterator{directory}) {
    if (entry.is_regular_file() && entry.path().filename() != kWalLockFileName) {
      image.push_back(CrashFileImage{.name = entry.path().filename().string(),
                                     .bytes = read_crash_file(entry.path())});
    }
  }
  std::sort(image.begin(), image.end(),
            [](const CrashFileImage& left, const CrashFileImage& right) {
              return left.name < right.name;
            });
  return image;
}

inline void flip_crash_file_byte(const std::filesystem::path& path, const std::uint64_t offset) {
  std::fstream file{path, std::ios::binary | std::ios::in | std::ios::out};
  file.seekg(static_cast<std::streamoff>(offset));
  char value = 0;
  file.read(&value, 1);
  value = static_cast<char>(static_cast<unsigned char>(value) ^ 0x01U);
  file.seekp(static_cast<std::streamoff>(offset));
  file.write(&value, 1);
  file.flush();
}

} // namespace chronos::wal::test

#endif // CHRONOS_TESTS_WAL_WAL_CRASH_TEST_SUPPORT_HPP_
