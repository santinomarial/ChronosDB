#include "chronos/wal/wal_scan.hpp"

#include "chronos/wal/codec.hpp"
#include "chronos/wal/wal_paths.hpp"
#include "io/posix_syscalls.hpp"
#include "wal/codec_internal.hpp"
#include "wal/wal_scan_internal.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::wal {
namespace {

[[nodiscard]] common::Status with_context(const std::string_view context,
                                          const common::Status& status) {
  std::string message{context};
  message.append(": ");
  message.append(status.message());
  return common::Status{status.code(), std::move(message)};
}

[[nodiscard]] common::Status corruption(std::string message) {
  return common::Status{common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Result<std::uint64_t> parse_segment_number(const std::string_view name) {
  constexpr std::string_view kPrefix = "wal-";
  constexpr std::string_view kSuffix = ".cwal";
  if (name.size() != kPrefix.size() + 20U + kSuffix.size() || !name.starts_with(kPrefix) ||
      !name.ends_with(kSuffix)) {
    return common::make_unexpected(
        corruption("malformed final WAL segment name: " + std::string{name}));
  }
  std::uint64_t value = 0;
  for (const char character : name.substr(kPrefix.size(), 20U)) {
    if (character < '0' || character > '9') {
      return common::make_unexpected(
          corruption("malformed final WAL segment name: " + std::string{name}));
    }
    const std::uint64_t digit = static_cast<std::uint64_t>(character - '0');
    if (value > (std::numeric_limits<std::uint64_t>::max() - digit) / 10U) {
      return common::make_unexpected(
          corruption("WAL segment filename number overflows uint64: " + std::string{name}));
    }
    value = (value * 10U) + digit;
  }
  if (value == 0U) {
    return common::make_unexpected(corruption("WAL segment filename number must be nonzero"));
  }
  return value;
}

[[nodiscard]] bool is_lower_hex(const char character) {
  return (character >= '0' && character <= '9') || (character >= 'a' && character <= 'f');
}

[[nodiscard]] bool is_temporary_segment_name(const std::string_view name) {
  constexpr std::string_view kPrefix = ".wal-";
  constexpr std::string_view kMiddle = ".cwal.tmp-";
  constexpr std::size_t kNonceLength = 32U;
  if (name.size() != kPrefix.size() + 20U + kMiddle.size() + kNonceLength ||
      !name.starts_with(kPrefix) || name.substr(kPrefix.size() + 20U, kMiddle.size()) != kMiddle) {
    return false;
  }
  const common::Result<std::uint64_t> number =
      parse_segment_number(name.substr(1U, kPrefix.size() - 1U + 20U + 5U));
  if (!number.has_value()) {
    return false;
  }
  return std::all_of(name.end() - static_cast<std::ptrdiff_t>(kNonceLength), name.end(),
                     is_lower_hex);
}

[[nodiscard]] common::Result<detail::WalDiscovery>
discover_wal(io::PosixDirectory& directory, const bool require_complete_prefix) {
  const common::Result<std::vector<io::DirectoryEntry>> entries = directory.list_entries();
  if (!entries.has_value()) {
    return common::make_unexpected(with_context("list WAL directory", entries.error()));
  }

  detail::WalDiscovery discovery;
  bool saw_lock = false;
  try {
    for (const io::DirectoryEntry& entry : *entries) {
      if (entry.name == kWalLockFileName) {
        if (entry.type != io::DirectoryEntryType::kRegularFile) {
          return common::make_unexpected(corruption("WAL LOCK entry is not a regular file"));
        }
        saw_lock = true;
        continue;
      }
      if (entry.name.starts_with("wal-")) {
        const common::Result<std::uint64_t> number = parse_segment_number(entry.name);
        if (!number.has_value()) {
          return common::make_unexpected(number.error());
        }
        if (entry.type != io::DirectoryEntryType::kRegularFile) {
          return common::make_unexpected(
              corruption("final WAL segment is not a regular file: " + entry.name));
        }
        discovery.segments.push_back(
            detail::DiscoveredWalSegment{.number = *number, .file_name = entry.name});
        continue;
      }
      if (is_temporary_segment_name(entry.name)) {
        if (entry.type != io::DirectoryEntryType::kRegularFile) {
          return common::make_unexpected(
              corruption("temporary WAL segment is not a regular file: " + entry.name));
        }
        discovery.temporary_file_names.push_back(entry.name);
        continue;
      }
      if (entry.name.starts_with(".wal-")) {
        return common::make_unexpected(
            corruption("malformed entry uses the reserved WAL namespace: " + entry.name));
      }
      return common::make_unexpected(
          corruption("unrecognized entry in dedicated WAL directory: " + entry.name));
    }
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "cannot allocate WAL discovery snapshot"});
  }

  if (!saw_lock) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "WAL LOCK entry is missing"});
  }
  if (discovery.segments.empty()) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotFound, "existing WAL history has no final segment"});
  }
  std::sort(discovery.segments.begin(), discovery.segments.end(),
            [](const auto& left, const auto& right) { return left.number < right.number; });
  if (require_complete_prefix) {
    for (std::size_t index = 0; index < discovery.segments.size(); ++index) {
      const std::uint64_t expected = static_cast<std::uint64_t>(index) + 1U;
      if (discovery.segments[index].number != expected) {
        return common::make_unexpected(
            corruption("WAL segment sequence has a gap or does not begin at 1"));
      }
    }
  }
  return discovery;
}

[[nodiscard]] common::Status read_exact(const io::PosixFile& file, const std::uint64_t offset,
                                        const common::MutableByteView destination,
                                        const std::string_view context) {
  const common::Result<std::size_t> count = file.read_at(offset, destination);
  if (!count.has_value()) {
    return with_context(context, count.error());
  }
  if (*count != destination.size()) {
    return common::Status{common::StatusCode::kIoError,
                          std::string{context} + ": file changed or returned premature EOF"};
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status validate_report_increment(std::uint64_t& value,
                                                       const std::string_view what) {
  if (value == std::numeric_limits<std::uint64_t>::max()) {
    return common::Status{common::StatusCode::kResourceExhausted,
                          std::string{what} + " count exceeds uint64"};
  }
  ++value;
  return common::Status::ok();
}

[[nodiscard]] common::Status validate_checkpoint(const WalReplayCheckpoint& checkpoint) {
  if (!checkpoint.wal_id.is_valid() || checkpoint.segment_number == 0U ||
      checkpoint.byte_offset < kSegmentHeaderSize || checkpoint.byte_offset > kSegmentSizeLimit) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "WAL replay checkpoint has an invalid identity or physical coordinate"};
  }
  if (checkpoint.record_sequence == 0U && (checkpoint.segment_number != kFirstSegmentNumber ||
                                           checkpoint.byte_offset != kSegmentHeaderSize)) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "empty WAL checkpoint must be segment 1 byte offset 64"};
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status
validate_covered_headers(io::PosixDirectory& directory,
                         const std::span<const detail::DiscoveredWalSegment> segments,
                         const WalReplayCheckpoint& checkpoint) {
  for (const detail::DiscoveredWalSegment& discovered : segments) {
    common::Result<io::PosixFile> file =
        directory.open_regular_file(discovered.file_name, io::FileOpenMode::kReadOnly);
    if (!file.has_value()) {
      return with_context("open covered WAL segment " + discovered.file_name, file.error());
    }
    const common::Result<std::uint64_t> size = file->size();
    if (!size.has_value()) {
      return with_context("size covered WAL segment " + discovered.file_name, size.error());
    }
    if (*size < kSegmentHeaderSize) {
      return corruption("covered WAL segment has an incomplete header: " + discovered.file_name);
    }
    common::Status status = validate_segment_size(*size);
    if (!status.is_ok()) {
      return with_context("validate covered WAL segment size", status);
    }
    EncodedSegmentHeader encoded{};
    status = read_exact(*file, 0U, encoded, "read covered WAL segment header");
    if (!status.is_ok()) {
      return status;
    }
    const common::Result<SegmentHeader> header = decode_segment_header(encoded);
    if (!header.has_value()) {
      return with_context("decode covered WAL segment header", header.error());
    }
    if (header->segment_number != discovered.number || header->wal_id != checkpoint.wal_id) {
      return corruption("covered WAL segment name or identity disagrees with checkpoint");
    }
  }
  return common::Status::ok();
}

} // namespace

namespace detail {

common::Result<LockedWalDirectory> open_locked_wal_directory(const std::string_view directory_path,
                                                             const std::uint16_t lock_permissions,
                                                             const bool create_lock,
                                                             io::detail::PosixSyscalls& syscalls) {
  common::Result<io::PosixDirectory> directory =
      io::detail::PosixHandleFactory::open_directory(directory_path, syscalls);
  if (!directory.has_value()) {
    return common::make_unexpected(with_context("open WAL directory", directory.error()));
  }
  common::Result<io::PosixAdvisoryLock> lock =
      create_lock ? directory->acquire_exclusive_lock(kWalLockFileName, lock_permissions)
                  : directory->acquire_existing_exclusive_lock(kWalLockFileName);
  if (!lock.has_value()) {
    return common::make_unexpected(with_context("acquire WAL writer lock", lock.error()));
  }
  common::Result<WalDiscovery> discovery = discover_wal(*directory, true);
  if (!discovery.has_value()) {
    return common::make_unexpected(discovery.error());
  }
  return LockedWalDirectory{.directory = std::move(*directory),
                            .lock = std::move(*lock),
                            .discovery = std::move(*discovery)};
}

common::Result<LockedWalDirectory>
open_locked_wal_directory_for_checkpoint(const std::string_view directory_path,
                                         io::detail::PosixSyscalls& syscalls) {
  common::Result<io::PosixDirectory> directory =
      io::detail::PosixHandleFactory::open_directory(directory_path, syscalls);
  if (!directory.has_value()) {
    return common::make_unexpected(with_context("open WAL directory", directory.error()));
  }
  common::Result<io::PosixAdvisoryLock> lock =
      directory->acquire_existing_exclusive_lock(kWalLockFileName);
  if (!lock.has_value()) {
    return common::make_unexpected(with_context("acquire WAL writer lock", lock.error()));
  }
  common::Result<WalDiscovery> discovery = discover_wal(*directory, false);
  if (!discovery.has_value()) {
    return common::make_unexpected(discovery.error());
  }
  return LockedWalDirectory{.directory = std::move(*directory),
                            .lock = std::move(*lock),
                            .discovery = std::move(*discovery)};
}

common::Result<WalRecoveryReport>
scan_discovered_wal(io::PosixDirectory& directory, const WalDiscovery& discovery,
                    const ScanPass pass, WalReplaySink* const sink,
                    const std::optional<WalReplayCheckpoint> checkpoint) {
  if ((pass == ScanPass::kPreflight || pass == ScanPass::kReplay) && sink == nullptr) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInternal, "WAL scan callback pass has no sink"});
  }
  WalRecoveryReport report;
  report.segment_count = static_cast<std::uint64_t>(discovery.segments.size());
  report.temporary_file_count = static_cast<std::uint64_t>(discovery.temporary_file_names.size());
  std::uint64_t expected_record_sequence = kFirstRecordSequence;
  bool have_wal_id = false;
  bool checkpoint_boundary_seen = !checkpoint.has_value();
  if (checkpoint.has_value()) {
    report.last_record_sequence = checkpoint->record_sequence;
  }

  try {
    for (std::size_t segment_index = 0; segment_index < discovery.segments.size();
         ++segment_index) {
      const DiscoveredWalSegment& discovered = discovery.segments[segment_index];
      const bool is_final = segment_index + 1U == discovery.segments.size();
      common::Result<io::PosixFile> file =
          directory.open_regular_file(discovered.file_name, io::FileOpenMode::kReadOnly);
      if (!file.has_value()) {
        return common::make_unexpected(
            with_context("open WAL segment " + discovered.file_name, file.error()));
      }
      const common::Result<std::uint64_t> size = file->size();
      if (!size.has_value()) {
        return common::make_unexpected(
            with_context("size WAL segment " + discovered.file_name, size.error()));
      }
      if (*size < kSegmentHeaderSize) {
        return common::make_unexpected(
            corruption("installed WAL segment has an incomplete header: " + discovered.file_name));
      }
      const common::Status size_status = validate_segment_size(*size);
      if (!size_status.is_ok()) {
        return common::make_unexpected(with_context("validate WAL segment size", size_status));
      }
      if (report.physical_bytes > std::numeric_limits<std::uint64_t>::max() - *size) {
        return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                      "total WAL physical bytes exceed uint64"});
      }
      report.physical_bytes += *size;

      EncodedSegmentHeader encoded_header{};
      common::Status status = read_exact(*file, 0U, encoded_header, "read WAL segment header");
      if (!status.is_ok()) {
        return common::make_unexpected(status);
      }
      const common::Result<SegmentHeader> header = decode_segment_header(encoded_header);
      if (!header.has_value()) {
        return common::make_unexpected(
            with_context("decode WAL segment header " + discovered.file_name, header.error()));
      }
      if (header->segment_number != discovered.number) {
        return common::make_unexpected(
            corruption("WAL segment filename/header number mismatch: " + discovered.file_name));
      }
      if (!have_wal_id) {
        if (!checkpoint.has_value()) {
          if (discovered.number != kFirstSegmentNumber ||
              header->first_record_sequence != kFirstRecordSequence) {
            return common::make_unexpected(
                corruption("first WAL segment does not begin segment and record sequence 1"));
          }
        } else {
          if (header->wal_id != checkpoint->wal_id) {
            return common::make_unexpected(
                corruption("required WAL suffix identity disagrees with checkpoint"));
          }
          if (discovered.number == checkpoint->segment_number) {
            if (checkpoint->byte_offset < kSegmentHeaderSize || checkpoint->byte_offset > *size) {
              return common::make_unexpected(
                  corruption("WAL checkpoint offset is outside its coordinate segment"));
            }
            checkpoint_boundary_seen =
                checkpoint->record_sequence == 0U && checkpoint->byte_offset == kSegmentHeaderSize;
            if (checkpoint->record_sequence == 0U &&
                header->first_record_sequence != kFirstRecordSequence) {
              return common::make_unexpected(
                  corruption("empty WAL checkpoint segment does not begin record sequence 1"));
            }
          } else {
            if (checkpoint->segment_number == std::numeric_limits<std::uint64_t>::max() ||
                discovered.number != checkpoint->segment_number + 1U ||
                checkpoint->record_sequence == std::numeric_limits<std::uint64_t>::max() ||
                header->first_record_sequence != checkpoint->record_sequence + 1U) {
              return common::make_unexpected(
                  corruption("WAL suffix does not begin immediately after checkpoint"));
            }
            checkpoint_boundary_seen = true;
          }
          expected_record_sequence = header->first_record_sequence;
        }
        report.wal_id = header->wal_id;
        have_wal_id = true;
      } else if (header->wal_id != report.wal_id) {
        return common::make_unexpected(corruption("WAL segments contain mixed identities"));
      }
      if (header->first_record_sequence != expected_record_sequence) {
        return common::make_unexpected(
            corruption("WAL segment first-record sequence is not contiguous"));
      }

      std::uint64_t offset = kSegmentHeaderSize;
      std::uint64_t records_in_segment = 0;
      while (offset < *size) {
        const std::uint64_t remaining = *size - offset;
        if (remaining < kRecordHeaderSize) {
          if (!is_final) {
            return common::make_unexpected(
                corruption("incomplete record header appears in a non-final WAL segment"));
          }
          if (checkpoint.has_value() && !checkpoint_boundary_seen) {
            return common::make_unexpected(
                corruption("incomplete WAL tail occurs before the checkpoint boundary"));
          }
          report.classification = WalScanClassification::kIncompleteFinalTail;
          report.valid_end = PhysicalWalPosition{
              .wal_id = report.wal_id, .segment_number = discovered.number, .byte_offset = offset};
          report.observed_final_size = *size;
          return report;
        }

        EncodedRecordHeader encoded_record_header{};
        status = read_exact(*file, offset, encoded_record_header, "read WAL record header");
        if (!status.is_ok()) {
          return common::make_unexpected(status);
        }
        const common::Result<RecordHeader> record_header =
            decode_record_header_for_physical_scan(encoded_record_header);
        if (!record_header.has_value()) {
          return common::make_unexpected(
              with_context("decode WAL record header at offset " + std::to_string(offset),
                           record_header.error()));
        }
        if (record_header->record_sequence != expected_record_sequence) {
          return common::make_unexpected(corruption(
              "WAL record sequence is not contiguous at offset " + std::to_string(offset)));
        }
        if (record_header->total_length > remaining) {
          if (record_header->record_flags != 0U) {
            return common::make_unexpected(common::Status{
                common::StatusCode::kNotSupported,
                "WAL record required flags are not supported at offset " + std::to_string(offset)});
          }
          if (!is_final) {
            return common::make_unexpected(
                corruption("incomplete record appears in a non-final WAL segment"));
          }
          if (checkpoint.has_value() && !checkpoint_boundary_seen) {
            return common::make_unexpected(
                corruption("incomplete WAL record occurs before the checkpoint boundary"));
          }
          report.classification = WalScanClassification::kIncompleteFinalTail;
          report.valid_end = PhysicalWalPosition{
              .wal_id = report.wal_id, .segment_number = discovered.number, .byte_offset = offset};
          report.observed_final_size = *size;
          return report;
        }

        std::vector<std::byte> encoded_record(record_header->total_length);
        status = read_exact(*file, offset, encoded_record, "read complete WAL record");
        if (!status.is_ok()) {
          return common::make_unexpected(status);
        }
        const common::Result<DecodedRecord> decoded = decode_record(encoded_record);
        if (!decoded.has_value()) {
          return common::make_unexpected(with_context(
              "decode WAL record at offset " + std::to_string(offset), decoded.error()));
        }

        const PhysicalWalPosition record_start{
            .wal_id = report.wal_id, .segment_number = discovered.number, .byte_offset = offset};
        offset += record_header->total_length;
        const PhysicalWalPosition record_end{
            .wal_id = report.wal_id, .segment_number = discovered.number, .byte_offset = offset};
        if (checkpoint.has_value() && discovered.number == checkpoint->segment_number &&
            record_header->record_sequence == checkpoint->record_sequence) {
          if (offset != checkpoint->byte_offset) {
            return common::make_unexpected(
                corruption("WAL checkpoint does not equal its record end boundary"));
          }
          checkpoint_boundary_seen = true;
        }
        if (checkpoint.has_value() && !checkpoint_boundary_seen &&
            offset >= checkpoint->byte_offset) {
          return common::make_unexpected(
              corruption("WAL checkpoint is not a complete record boundary"));
        }
        const bool after_checkpoint =
            !checkpoint.has_value() || record_header->record_sequence > checkpoint->record_sequence;
        if (pass != ScanPass::kVerify && after_checkpoint) {
          const WalReplayRecord callback_record{.header = decoded->header,
                                                .record_start = record_start,
                                                .record_end = record_end,
                                                .payload = decoded->payload};
          status = pass == ScanPass::kPreflight ? sink->preflight(callback_record)
                                                : sink->replay(callback_record);
          if (!status.is_ok()) {
            return common::make_unexpected(with_context(
                pass == ScanPass::kPreflight ? "preflight WAL record" : "replay WAL record",
                status));
          }
        }

        status = validate_report_increment(report.record_count, "WAL record");
        if (!status.is_ok()) {
          return common::make_unexpected(status);
        }
        ++records_in_segment;
        report.last_record_sequence = expected_record_sequence;
        if (expected_record_sequence == std::numeric_limits<std::uint64_t>::max()) {
          if (!is_final || offset != *size) {
            return common::make_unexpected(
                corruption("terminal WAL record sequence is followed by bytes or another segment"));
          }
          report.sequence_exhausted = true;
        } else {
          ++expected_record_sequence;
        }
      }

      if (!is_final && records_in_segment == 0U) {
        return common::make_unexpected(corruption("non-final WAL segment is empty"));
      }
      if (report.sequence_exhausted && !is_final) {
        return common::make_unexpected(
            corruption("terminal WAL record sequence is followed by another segment"));
      }
      if (is_final) {
        report.valid_end = PhysicalWalPosition{
            .wal_id = report.wal_id, .segment_number = discovered.number, .byte_offset = offset};
        report.observed_final_size = *size;
      }
    }
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "cannot allocate bounded WAL record buffer"});
  }
  if (!checkpoint_boundary_seen) {
    return common::make_unexpected(
        corruption("WAL checkpoint record boundary is absent from its coordinate segment"));
  }
  return report;
}

common::Status require_same_verified_history(const WalRecoveryReport& expected,
                                             const WalRecoveryReport& observed) {
  if (expected.classification != WalScanClassification::kClean ||
      observed.classification != WalScanClassification::kClean ||
      expected.wal_id != observed.wal_id || expected.segment_count != observed.segment_count ||
      expected.temporary_file_count != observed.temporary_file_count ||
      expected.record_count != observed.record_count ||
      expected.physical_bytes != observed.physical_bytes ||
      expected.valid_end != observed.valid_end ||
      expected.last_record_sequence != observed.last_record_sequence ||
      expected.sequence_exhausted != observed.sequence_exhausted) {
    return common::Status{common::StatusCode::kCorruption,
                          "WAL bytes changed between verification passes"};
  }
  return common::Status::ok();
}

} // namespace detail

common::Result<WalRecoveryReport> scan_wal(const std::string_view directory_path) {
  common::Result<detail::LockedWalDirectory> locked = detail::open_locked_wal_directory(
      directory_path, 0600U, false, io::detail::system_posix_syscalls());
  if (!locked.has_value()) {
    return common::make_unexpected(locked.error());
  }
  return detail::scan_discovered_wal(locked->directory, locked->discovery);
}

common::Result<WalRecoveryReport> inspect_wal_suffix(const std::string_view directory_path,
                                                     const WalReplayCheckpoint& checkpoint,
                                                     WalReplaySink& sink) {
  common::Status status = validate_checkpoint(checkpoint);
  if (!status.is_ok()) {
    return common::make_unexpected(status);
  }
  common::Result<detail::LockedWalDirectory> locked =
      detail::open_locked_wal_directory_for_checkpoint(directory_path,
                                                       io::detail::system_posix_syscalls());
  if (!locked.has_value()) {
    return common::make_unexpected(locked.error());
  }
  const auto first_required = std::ranges::lower_bound(
      locked->discovery.segments, checkpoint.segment_number, {},
      [](const detail::DiscoveredWalSegment& segment) { return segment.number; });
  std::size_t required_index =
      static_cast<std::size_t>(first_required - locked->discovery.segments.begin());
  if (first_required == locked->discovery.segments.end() ||
      (first_required->number != checkpoint.segment_number &&
       (checkpoint.segment_number == std::numeric_limits<std::uint64_t>::max() ||
        first_required->number != checkpoint.segment_number + 1U))) {
    return common::make_unexpected(
        corruption("WAL checkpoint coordinate segment or immediate successor is missing"));
  }
  status = validate_covered_headers(
      locked->directory,
      std::span<const detail::DiscoveredWalSegment>{locked->discovery.segments}.first(
          required_index),
      checkpoint);
  if (!status.is_ok()) {
    return common::make_unexpected(status);
  }
  for (std::size_t index = required_index + 1U; index < locked->discovery.segments.size();
       ++index) {
    if (locked->discovery.segments[index - 1U].number ==
            std::numeric_limits<std::uint64_t>::max() ||
        locked->discovery.segments[index].number !=
            locked->discovery.segments[index - 1U].number + 1U) {
      return common::make_unexpected(corruption("required WAL suffix contains a segment gap"));
    }
  }
  locked->discovery.segments.erase(locked->discovery.segments.begin(), first_required);

  common::Result<WalRecoveryReport> verified = detail::scan_discovered_wal(
      locked->directory, locked->discovery, detail::ScanPass::kVerify, nullptr, checkpoint);
  if (!verified.has_value() ||
      verified->classification == WalScanClassification::kIncompleteFinalTail) {
    return verified;
  }
  common::Result<WalRecoveryReport> preflight = detail::scan_discovered_wal(
      locked->directory, locked->discovery, detail::ScanPass::kPreflight, &sink, checkpoint);
  if (!preflight.has_value()) {
    return common::make_unexpected(preflight.error());
  }
  status = detail::require_same_verified_history(*verified, *preflight);
  if (!status.is_ok()) {
    return common::make_unexpected(status);
  }
  common::Result<WalRecoveryReport> replay = detail::scan_discovered_wal(
      locked->directory, locked->discovery, detail::ScanPass::kReplay, &sink, checkpoint);
  if (!replay.has_value()) {
    return common::make_unexpected(replay.error());
  }
  status = detail::require_same_verified_history(*verified, *replay);
  if (!status.is_ok()) {
    return common::make_unexpected(status);
  }
  return replay;
}

} // namespace chronos::wal
