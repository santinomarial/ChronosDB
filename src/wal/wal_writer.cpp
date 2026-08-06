#include "chronos/wal/wal_writer.hpp"

#include "chronos/io/posix_io.hpp"
#include "chronos/wal/codec.hpp"
#include "chronos/wal/wal_paths.hpp"
#include "io/posix_syscalls.hpp"
#include "wal/wal_recovery_internal.hpp"
#include "wal/wal_scan_internal.hpp"
#include "wal/wal_segment_internal.hpp"
#include "wal/wal_writer_config_internal.hpp"
#include "wal/wal_writer_internal.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::wal {
namespace {

[[nodiscard]] common::Status invalid_writer(std::string_view operation) {
  std::string message{operation};
  message.append(" requires an open WAL writer");
  return common::Status{common::StatusCode::kInvalidArgument, std::move(message)};
}

[[nodiscard]] common::Status with_context(std::string_view context, const common::Status& status) {
  std::string message{context};
  message.append(": ");
  message.append(status.message());
  return common::Status{status.code(), std::move(message)};
}

[[nodiscard]] common::Status sequence_exhausted() {
  return common::Status{common::StatusCode::kResourceExhausted,
                        "WAL record sequence UINT64_MAX has no successor"};
}

[[nodiscard]] common::Status segment_sequence_exhausted() {
  return common::Status{common::StatusCode::kResourceExhausted,
                        "WAL segment sequence UINT64_MAX has no successor"};
}

[[nodiscard]] bool parse_nonzero_segment_digits(const std::string_view digits) {
  if (digits.size() != 20U) {
    return false;
  }
  std::uint64_t value = 0;
  for (const char character : digits) {
    if (character < '0' || character > '9') {
      return false;
    }
    const std::uint64_t digit = static_cast<std::uint64_t>(character - '0');
    if (value > (std::numeric_limits<std::uint64_t>::max() - digit) / 10U) {
      return false;
    }
    value = (value * 10U) + digit;
  }
  return value != 0U;
}

[[nodiscard]] bool is_final_segment_name(const std::string_view name) {
  constexpr std::string_view kPrefix = "wal-";
  constexpr std::string_view kSuffix = ".cwal";
  return name.size() == kPrefix.size() + 20U + kSuffix.size() && name.starts_with(kPrefix) &&
         name.ends_with(kSuffix) && parse_nonzero_segment_digits(name.substr(kPrefix.size(), 20U));
}

[[nodiscard]] bool is_lower_hex(const char character) {
  return (character >= '0' && character <= '9') || (character >= 'a' && character <= 'f');
}

[[nodiscard]] bool is_temporary_segment_name(const std::string_view name) {
  constexpr std::string_view kPrefix = ".wal-";
  constexpr std::string_view kMiddle = ".cwal.tmp-";
  constexpr std::size_t kNonceLength = 32U;
  if (name.size() != kPrefix.size() + 20U + kMiddle.size() + kNonceLength ||
      !name.starts_with(kPrefix) || name.substr(kPrefix.size() + 20U, kMiddle.size()) != kMiddle ||
      !parse_nonzero_segment_digits(name.substr(kPrefix.size(), 20U))) {
    return false;
  }
  const std::string_view nonce = name.substr(name.size() - kNonceLength);
  return std::all_of(nonce.begin(), nonce.end(), is_lower_hex);
}

[[nodiscard]] common::Status
validate_new_directory_contents(const std::vector<io::DirectoryEntry>& entries) {
  // Diagnose reserved-namespace/type corruption before ordinary creation conflicts or unrelated
  // entries, independent of filesystem enumeration order.
  for (const io::DirectoryEntry& entry : entries) {
    if (entry.name == kWalLockFileName) {
      if (entry.type != io::DirectoryEntryType::kRegularFile) {
        return common::Status{common::StatusCode::kCorruption,
                              "WAL LOCK entry exists but is not a regular file"};
      }
      continue;
    }
    if (is_final_segment_name(entry.name)) {
      if (entry.type != io::DirectoryEntryType::kRegularFile) {
        return common::Status{common::StatusCode::kCorruption,
                              "reserved final WAL segment entry is not a regular file: " +
                                  entry.name};
      }
      continue;
    }
    if (is_temporary_segment_name(entry.name)) {
      if (entry.type != io::DirectoryEntryType::kRegularFile) {
        return common::Status{common::StatusCode::kCorruption,
                              "recognized temporary WAL entry is not a regular file: " +
                                  entry.name};
      }
      continue;
    }
    if (entry.name.starts_with("wal-") || entry.name.starts_with(".wal-")) {
      return common::Status{common::StatusCode::kCorruption,
                            "malformed entry uses the reserved WAL namespace: " + entry.name};
    }
  }
  for (const io::DirectoryEntry& entry : entries) {
    if (is_final_segment_name(entry.name)) {
      return common::Status{common::StatusCode::kAlreadyExists,
                            "new WAL directory already contains installed history: " + entry.name};
    }
    if (is_temporary_segment_name(entry.name)) {
      return common::Status{
          common::StatusCode::kAlreadyExists,
          "new WAL directory contains an orphan temporary segment requiring recovery: " +
              entry.name};
    }
  }
  for (const io::DirectoryEntry& entry : entries) {
    if (entry.name != kWalLockFileName) {
      return common::Status{common::StatusCode::kInvalidArgument,
                            "dedicated new WAL directory contains an unrelated entry: " +
                                entry.name};
    }
  }
  return common::Status::ok();
}

[[nodiscard]] common::Status inspect_new_directory(io::PosixDirectory& directory,
                                                   bool& has_regular_lock) {
  const common::Result<std::vector<io::DirectoryEntry>> entries = directory.list_entries();
  if (!entries.has_value()) {
    return with_context("inspect new WAL directory", entries.error());
  }
  has_regular_lock = std::any_of(entries->begin(), entries->end(), [](const auto& entry) {
    return entry.name == kWalLockFileName && entry.type == io::DirectoryEntryType::kRegularFile;
  });
  return validate_new_directory_contents(*entries);
}

[[nodiscard]] common::Status corruption(std::string message) {
  return common::Status{common::StatusCode::kCorruption, std::move(message)};
}

[[nodiscard]] common::Result<WalRecoveryReport>
validate_closed_segment_for_reclamation(io::PosixDirectory& directory,
                                        const detail::DiscoveredWalSegment& segment) {
  detail::WalDiscovery isolated{.segments = {segment}, .temporary_file_names = {}};
  std::optional<WalReplayCheckpoint> predecessor;
  if (segment.number != kFirstSegmentNumber) {
    common::Result<io::PosixFile> file =
        directory.open_regular_file(segment.file_name, io::FileOpenMode::kReadOnly);
    if (!file.has_value()) {
      return common::make_unexpected(
          with_context("open closed WAL segment for reclamation", file.error()));
    }
    EncodedSegmentHeader encoded{};
    const common::Result<std::size_t> count = file->read_at(0U, encoded);
    if (!count.has_value()) {
      return common::make_unexpected(
          with_context("read closed WAL segment header for reclamation", count.error()));
    }
    if (*count != encoded.size()) {
      return common::make_unexpected(
          corruption("closed WAL segment has an incomplete header during reclamation"));
    }
    const common::Result<SegmentHeader> header = decode_segment_header(encoded);
    if (!header.has_value()) {
      return common::make_unexpected(
          with_context("decode closed WAL segment header for reclamation", header.error()));
    }
    if (header->first_record_sequence == 0U) {
      return common::make_unexpected(
          corruption("closed WAL segment begins with record sequence zero"));
    }
    predecessor = WalReplayCheckpoint{.wal_id = header->wal_id,
                                      .record_sequence = header->first_record_sequence - 1U,
                                      .segment_number = segment.number - 1U,
                                      .byte_offset = kSegmentHeaderSize};
  }
  common::Result<WalRecoveryReport> report = detail::scan_discovered_wal(
      directory, isolated, detail::ScanPass::kVerify, nullptr, predecessor);
  if (!report.has_value()) {
    return common::make_unexpected(report.error());
  }
  if (report->classification != WalScanClassification::kClean || report->record_count == 0U ||
      report->valid_end.segment_number != segment.number) {
    return common::make_unexpected(
        corruption("closed WAL segment is empty or incomplete during reclamation"));
  }
  return report;
}

} // namespace

class WalWriter::Impl {
public:
  Impl(io::PosixDirectory directory, io::PosixAdvisoryLock lock,
       detail::ActiveWalSegment active_segment, const WalWriterConfig& config) noexcept
      : directory_(std::move(directory)), lock_(std::move(lock)),
        active_segment_(std::move(active_segment)), file_permissions_(config.file_permissions),
        target_segment_size_(config.target_segment_size),
        maximum_application_payload_(config.maximum_application_payload),
        written_position_{.wal_id = active_segment_.metadata.header.wal_id,
                          .segment_number = active_segment_.metadata.header.segment_number,
                          .byte_offset = active_segment_.metadata.end_offset},
        durable_position_(written_position_) {}

  Impl(io::PosixDirectory directory, io::PosixAdvisoryLock lock,
       detail::ActiveWalSegment active_segment, const WalWriterConfig& config,
       const WalRecoveryReport& report) noexcept
      : directory_(std::move(directory)), lock_(std::move(lock)),
        active_segment_(std::move(active_segment)), file_permissions_(config.file_permissions),
        target_segment_size_(config.target_segment_size),
        maximum_application_payload_(config.maximum_application_payload),
        written_position_(report.valid_end), durable_position_(report.valid_end),
        next_record_sequence_(report.sequence_exhausted ? std::numeric_limits<std::uint64_t>::max()
                                                        : report.last_record_sequence + 1U),
        written_record_sequence_(report.last_record_sequence),
        durable_record_sequence_(report.last_record_sequence),
        sequence_exhausted_(report.sequence_exhausted) {}

  [[nodiscard]] common::Status poison(common::Status status) {
    if (failure_.is_ok()) {
      failure_ = std::move(status);
    }
    return failure_;
  }

  [[nodiscard]] common::Result<std::vector<std::byte>>
  encode_application_record(const common::ByteView application_payload) const {
    const common::Result<RecordHeader> header =
        make_record_header({.record_type = kApplicationEntryRecordType,
                            .record_sequence = next_record_sequence_,
                            .payload_length = application_payload.size()});
    if (!header.has_value()) {
      return common::make_unexpected(header.error());
    }

    try {
      std::vector<std::byte> encoded(header->total_length);
      const common::Result<std::size_t> encoded_size =
          encode_record(*header, application_payload, encoded);
      if (!encoded_size.has_value()) {
        return common::make_unexpected(encoded_size.error());
      }
      return encoded;
    } catch (const std::bad_alloc&) {
      return common::make_unexpected(common::Status{
          common::StatusCode::kResourceExhausted, "cannot allocate the bounded WAL record buffer"});
    }
  }

  [[nodiscard]] common::Status rotate() {
    if (active_segment_.metadata.header.segment_number ==
        std::numeric_limits<std::uint64_t>::max()) {
      return segment_sequence_exhausted();
    }

    common::Status status = active_segment_.file.sync_data();
    if (!status.is_ok()) {
      return poison(with_context("synchronize prior WAL segment before rotation", status));
    }
    durable_position_ = written_position_;
    durable_record_sequence_ = written_record_sequence_;

    const std::uint64_t next_segment_number = active_segment_.metadata.header.segment_number + 1U;
    common::Result<detail::ActiveWalSegment> next_segment =
        detail::install_segment(directory_, {.wal_id = active_segment_.metadata.header.wal_id,
                                             .segment_number = next_segment_number,
                                             .first_record_sequence = next_record_sequence_,
                                             .file_permissions = file_permissions_});
    if (!next_segment.has_value()) {
      return poison(with_context("rotate WAL segment", next_segment.error()));
    }

    io::PosixFile prior_file = std::move(active_segment_.file);
    active_segment_ = std::move(*next_segment);
    written_position_ = PhysicalWalPosition{.wal_id = active_segment_.metadata.header.wal_id,
                                            .segment_number = next_segment_number,
                                            .byte_offset = kSegmentHeaderSize};
    // The installed empty header and all preceding records have crossed their required syncs.
    durable_position_ = written_position_;

    status = prior_file.close();
    if (!status.is_ok()) {
      return poison(with_context("close prior WAL segment after rotation", status));
    }
    return common::Status::ok();
  }

  [[nodiscard]] common::Status validate_active_state() const {
    const WalSegment& segment = active_segment_.metadata;
    if (segment.end_offset < kSegmentHeaderSize || segment.end_offset > kSegmentSizeLimit ||
        (segment.end_offset % 8U) != 0U) {
      return common::Status{common::StatusCode::kInternal,
                            "active WAL segment end is outside configured record boundaries"};
    }
    if (written_position_.wal_id != segment.header.wal_id ||
        written_position_.segment_number != segment.header.segment_number ||
        written_position_.byte_offset != segment.end_offset) {
      return common::Status{common::StatusCode::kInternal,
                            "written WAL position does not match the active segment end"};
    }
    return common::Status::ok();
  }

  io::PosixDirectory directory_;
  io::PosixAdvisoryLock lock_;
  detail::ActiveWalSegment active_segment_;
  std::uint16_t file_permissions_;
  std::uint64_t target_segment_size_;
  std::size_t maximum_application_payload_;
  PhysicalWalPosition written_position_;
  PhysicalWalPosition durable_position_;
  std::uint64_t next_record_sequence_{kFirstRecordSequence};
  std::uint64_t written_record_sequence_{};
  std::uint64_t durable_record_sequence_{};
  bool sequence_exhausted_{false};
  WalSegmentReclamationMetrics reclamation_metrics_;
  common::Status failure_;
};

WalWriter::WalWriter() noexcept = default;

WalWriter::~WalWriter() = default;

WalWriter::WalWriter(std::unique_ptr<Impl> implementation) noexcept
    : implementation_(std::move(implementation)) {}

WalWriter::WalWriter(WalWriter&&) noexcept = default;

WalWriter& WalWriter::operator=(WalWriter&&) noexcept = default;

common::Result<WalWriter> WalWriter::create_new(const WalWriterConfig& config) {
  SystemWalLogIdGenerator id_generator;
  return create_new(config, id_generator);
}

common::Result<WalWriter> WalWriter::create_new(const WalWriterConfig& config,
                                                WalLogIdGenerator& id_generator) {
  return create_new_with(config, id_generator, io::detail::system_posix_syscalls());
}

common::Result<WalWriter> WalWriter::create_new_with(const WalWriterConfig& config,
                                                     WalLogIdGenerator& id_generator,
                                                     io::detail::PosixSyscalls& syscalls) {
  const common::Status config_status = detail::validate_writer_config(config);
  if (!config_status.is_ok()) {
    return common::make_unexpected(config_status);
  }
  common::Result<io::PosixDirectory> directory =
      io::detail::PosixHandleFactory::open_directory(config.directory_path, syscalls);
  if (!directory.has_value()) {
    return common::make_unexpected(with_context("open new WAL directory", directory.error()));
  }

  bool has_regular_lock = false;
  common::Status directory_status = inspect_new_directory(*directory, has_regular_lock);
  if (!directory_status.is_ok()) {
    if (has_regular_lock) {
      const common::Result<io::PosixAdvisoryLock> existing_lock =
          directory->acquire_exclusive_lock(kWalLockFileName, config.file_permissions);
      if (!existing_lock.has_value()) {
        return common::make_unexpected(
            with_context("acquire WAL writer ownership", existing_lock.error()));
      }
    }
    return common::make_unexpected(directory_status);
  }
  common::Result<WalId> wal_id = id_generator.generate();
  if (!wal_id.has_value()) {
    return common::make_unexpected(with_context("generate WAL identity", wal_id.error()));
  }
  if (!wal_id->is_valid()) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInvalidArgument, "WAL identity generator returned an all-zero value"});
  }
  common::Result<io::PosixAdvisoryLock> lock =
      directory->acquire_exclusive_lock(kWalLockFileName, config.file_permissions);
  if (!lock.has_value()) {
    return common::make_unexpected(with_context("acquire WAL writer ownership", lock.error()));
  }
  directory_status = inspect_new_directory(*directory, has_regular_lock);
  if (!directory_status.is_ok()) {
    return common::make_unexpected(directory_status);
  }

  common::Result<detail::ActiveWalSegment> active_segment =
      detail::install_segment(*directory, {.wal_id = *wal_id,
                                           .segment_number = kFirstSegmentNumber,
                                           .first_record_sequence = kFirstRecordSequence,
                                           .file_permissions = config.file_permissions});
  if (!active_segment.has_value()) {
    return common::make_unexpected(
        with_context("install initial WAL segment", active_segment.error()));
  }

  try {
    return WalWriter{std::make_unique<Impl>(std::move(*directory), std::move(*lock),
                                            std::move(*active_segment), config)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "cannot allocate WAL writer ownership state"});
  }
}

common::Result<WalAppendResult>
WalWriter::append_application_entry(const common::ByteView application_payload) {
  if (implementation_ == nullptr || !implementation_->active_segment_.file.is_open()) {
    return common::make_unexpected(invalid_writer("append_application_entry"));
  }
  if (!implementation_->failure_.is_ok()) {
    return common::make_unexpected(implementation_->failure_);
  }
  if (application_payload.size() > implementation_->maximum_application_payload_) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kOutOfRange,
                       "WAL application payload exceeds the configured writer maximum"});
  }
  if (implementation_->sequence_exhausted_) {
    return common::make_unexpected(sequence_exhausted());
  }

  const common::Status state_status = implementation_->validate_active_state();
  if (!state_status.is_ok()) {
    return common::make_unexpected(implementation_->poison(state_status));
  }

  common::Result<std::vector<std::byte>> encoded =
      implementation_->encode_application_record(application_payload);
  if (!encoded.has_value()) {
    return common::make_unexpected(encoded.error());
  }
  const auto record_length = static_cast<std::uint64_t>(encoded->size());
  if (implementation_->active_segment_.metadata.end_offset >=
          implementation_->target_segment_size_ ||
      record_length > implementation_->target_segment_size_ -
                          implementation_->active_segment_.metadata.end_offset) {
    const common::Status rotation_status = implementation_->rotate();
    if (!rotation_status.is_ok()) {
      return common::make_unexpected(rotation_status);
    }
  }

  const PhysicalWalPosition record_start = implementation_->written_position_;
  const common::Status write_status = implementation_->active_segment_.file.write_all_at(
      record_start.byte_offset, common::ByteView{*encoded});
  if (!write_status.is_ok()) {
    return common::make_unexpected(
        implementation_->poison(with_context("append complete WAL record", write_status)));
  }

  implementation_->active_segment_.metadata.end_offset += record_length;
  implementation_->written_position_.byte_offset =
      implementation_->active_segment_.metadata.end_offset;
  const std::uint64_t appended_sequence = implementation_->next_record_sequence_;
  implementation_->written_record_sequence_ = appended_sequence;
  if (appended_sequence == std::numeric_limits<std::uint64_t>::max()) {
    implementation_->sequence_exhausted_ = true;
  } else {
    ++implementation_->next_record_sequence_;
  }
  return WalAppendResult{.record_sequence = appended_sequence,
                         .record_start = record_start,
                         .record_end = implementation_->written_position_};
}

common::Result<WalWriter> WalWriter::from_recovered_state(const WalWriterConfig& config,
                                                          detail::RecoveredWalState state) {
  try {
    return WalWriter{std::make_unique<Impl>(std::move(state.directory), std::move(state.lock),
                                            std::move(state.active_segment), config, state.report)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "cannot allocate recovered WAL writer state"});
  }
}

void detail::WalWriterTestAccess::set_sequence_state(WalWriter& writer,
                                                     const std::uint64_t next_record_sequence,
                                                     const bool sequence_exhausted) {
  writer.implementation_->next_record_sequence_ = next_record_sequence;
  writer.implementation_->sequence_exhausted_ = sequence_exhausted;
}

void detail::WalWriterTestAccess::set_active_segment_number(WalWriter& writer,
                                                            const std::uint64_t segment_number) {
  writer.implementation_->active_segment_.metadata.header.segment_number = segment_number;
  writer.implementation_->written_position_.segment_number = segment_number;
  writer.implementation_->durable_position_.segment_number = segment_number;
}

void detail::WalWriterTestAccess::set_active_end_offset(WalWriter& writer,
                                                        const std::uint64_t end_offset) {
  writer.implementation_->active_segment_.metadata.end_offset = end_offset;
  writer.implementation_->written_position_.byte_offset = end_offset;
}

common::Result<PhysicalWalPosition> WalWriter::synchronize() {
  if (implementation_ == nullptr || !implementation_->active_segment_.file.is_open()) {
    return common::make_unexpected(invalid_writer("synchronize"));
  }
  if (!implementation_->failure_.is_ok()) {
    return common::make_unexpected(implementation_->failure_);
  }
  const common::Status state_status = implementation_->validate_active_state();
  if (!state_status.is_ok()) {
    return common::make_unexpected(implementation_->poison(state_status));
  }
  const common::Status status = implementation_->active_segment_.file.sync_data();
  if (!status.is_ok()) {
    return common::make_unexpected(
        implementation_->poison(with_context("synchronize active WAL segment", status)));
  }
  implementation_->durable_position_ = implementation_->written_position_;
  implementation_->durable_record_sequence_ = implementation_->written_record_sequence_;
  return implementation_->durable_position_;
}

common::Result<WalSegmentReclamationReport>
WalWriter::reclaim_checkpointed_segments(const WalReplayCheckpoint& checkpoint) {
  if (implementation_ == nullptr || !implementation_->active_segment_.file.is_open()) {
    return common::make_unexpected(invalid_writer("reclaim_checkpointed_segments"));
  }
  if (!implementation_->failure_.is_ok()) {
    return common::make_unexpected(implementation_->failure_);
  }
  ++implementation_->reclamation_metrics_.attempts;
  const auto fail = [this](common::Status status,
                           const bool poison) -> common::Result<WalSegmentReclamationReport> {
    ++implementation_->reclamation_metrics_.failures;
    if (poison) {
      status = implementation_->poison(std::move(status));
    }
    return common::make_unexpected(std::move(status));
  };

  common::Status status = detail::validate_replay_checkpoint(checkpoint);
  if (!status.is_ok()) {
    return fail(std::move(status), false);
  }
  if (checkpoint.wal_id != implementation_->active_segment_.metadata.header.wal_id ||
      checkpoint.record_sequence > implementation_->durable_record_sequence_) {
    return fail(common::Status{common::StatusCode::kInvalidArgument,
                               "WAL reclamation checkpoint is not covered by this durable writer"},
                false);
  }

  common::Result<detail::WalDiscovery> discovered =
      detail::discover_wal_directory(implementation_->directory_, false);
  if (!discovered.has_value()) {
    return fail(discovered.error(), true);
  }
  if (discovered->segments.empty() ||
      discovered->segments.back().number !=
          implementation_->active_segment_.metadata.header.segment_number ||
      discovered->segments.back().file_name !=
          implementation_->active_segment_.metadata.file_name) {
    return fail(corruption("active WAL segment is not the highest installed final segment"), true);
  }

  std::optional<detail::WalDiscovery> checkpoint_view;
  try {
    checkpoint_view.emplace(*discovered);
  } catch (const std::bad_alloc&) {
    return fail(common::Status{common::StatusCode::kResourceExhausted,
                               "cannot allocate WAL checkpoint validation state"},
                false);
  }
  status = detail::prepare_discovery_for_checkpoint(implementation_->directory_, *checkpoint_view,
                                                    checkpoint);
  if (!status.is_ok()) {
    return fail(std::move(status), true);
  }
  common::Result<WalRecoveryReport> required =
      detail::scan_discovered_wal(implementation_->directory_, *checkpoint_view,
                                  detail::ScanPass::kVerify, nullptr, checkpoint);
  if (!required.has_value()) {
    return fail(required.error(), true);
  }
  if (required->classification != WalScanClassification::kClean ||
      required->wal_id != implementation_->active_segment_.metadata.header.wal_id ||
      required->valid_end != implementation_->written_position_ ||
      required->last_record_sequence != implementation_->written_record_sequence_) {
    return fail(corruption("WAL namespace changed or disagrees with the live writer"), true);
  }

  struct RemovalCandidate {
    std::string file_name;
  };
  std::vector<RemovalCandidate> candidates;
  std::uint64_t removed_bytes = 0U;
  bool retained_closed_segment_seen = false;
  try {
    for (const detail::DiscoveredWalSegment& segment : discovered->segments) {
      if (segment.number >= implementation_->active_segment_.metadata.header.segment_number) {
        break;
      }
      const common::Result<WalRecoveryReport> validated =
          validate_closed_segment_for_reclamation(implementation_->directory_, segment);
      if (!validated.has_value()) {
        return fail(validated.error(), true);
      }
      if (validated->wal_id != checkpoint.wal_id) {
        return fail(corruption("closed WAL segment identity disagrees with checkpoint"), true);
      }
      if (validated->last_record_sequence <= checkpoint.record_sequence) {
        if (retained_closed_segment_seen) {
          return fail(corruption("checkpoint-covered WAL segments do not form a prefix"), true);
        }
        if (validated->physical_bytes > std::numeric_limits<std::uint64_t>::max() - removed_bytes) {
          return fail(common::Status{common::StatusCode::kResourceExhausted,
                                     "reclaimed WAL byte count exceeds uint64"},
                      false);
        }
        removed_bytes += validated->physical_bytes;
        candidates.push_back(RemovalCandidate{.file_name = segment.file_name});
      } else {
        retained_closed_segment_seen = true;
      }
    }
  } catch (const std::bad_alloc&) {
    return fail(common::Status{common::StatusCode::kResourceExhausted,
                               "cannot allocate WAL reclamation candidate state"},
                false);
  }

  WalSegmentReclamationReport report{.checkpoint = checkpoint};
  if (candidates.empty()) {
    return report;
  }
  if (removed_bytes > std::numeric_limits<std::uint64_t>::max() -
                          implementation_->reclamation_metrics_.removed_physical_bytes ||
      candidates.size() > std::numeric_limits<std::uint64_t>::max() -
                              implementation_->reclamation_metrics_.removed_segment_count) {
    return fail(common::Status{common::StatusCode::kResourceExhausted,
                               "cumulative WAL reclamation metrics exceed uint64"},
                false);
  }
  for (const RemovalCandidate& candidate : candidates) {
    status = implementation_->directory_.remove_file(candidate.file_name);
    if (!status.is_ok()) {
      return fail(with_context("remove checkpoint-covered WAL segment", status), true);
    }
  }
  status = implementation_->directory_.sync();
  if (!status.is_ok()) {
    return fail(with_context("synchronize WAL directory after checkpoint reclamation", status),
                true);
  }

  report.removed_segment_count = static_cast<std::uint64_t>(candidates.size());
  report.removed_physical_bytes = removed_bytes;
  report.directory_sync_count = 1U;
  implementation_->reclamation_metrics_.removed_segment_count += report.removed_segment_count;
  implementation_->reclamation_metrics_.removed_physical_bytes += report.removed_physical_bytes;
  ++implementation_->reclamation_metrics_.directory_sync_count;
  return report;
}

bool WalWriter::is_open() const noexcept {
  return implementation_ != nullptr && implementation_->active_segment_.file.is_open() &&
         implementation_->lock_.is_held() && implementation_->directory_.is_open();
}

bool WalWriter::is_failed() const noexcept {
  return implementation_ != nullptr && !implementation_->failure_.is_ok();
}

common::Status WalWriter::failure_status() const {
  if (implementation_ == nullptr) {
    return invalid_writer("failure_status");
  }
  return implementation_->failure_;
}

WalId WalWriter::wal_id() const noexcept {
  return implementation_ == nullptr ? WalId{}
                                    : implementation_->active_segment_.metadata.header.wal_id;
}

WalSegment WalWriter::active_segment() const {
  return implementation_ == nullptr ? WalSegment{} : implementation_->active_segment_.metadata;
}

PhysicalWalPosition WalWriter::written_position() const noexcept {
  return implementation_ == nullptr ? PhysicalWalPosition{} : implementation_->written_position_;
}

PhysicalWalPosition WalWriter::durable_position() const noexcept {
  return implementation_ == nullptr ? PhysicalWalPosition{} : implementation_->durable_position_;
}

std::uint64_t WalWriter::written_record_sequence() const noexcept {
  return implementation_ == nullptr ? 0U : implementation_->written_record_sequence_;
}

std::uint64_t WalWriter::durable_record_sequence() const noexcept {
  return implementation_ == nullptr ? 0U : implementation_->durable_record_sequence_;
}

WalSegmentReclamationMetrics WalWriter::reclamation_metrics() const noexcept {
  return implementation_ == nullptr ? WalSegmentReclamationMetrics{}
                                    : implementation_->reclamation_metrics_;
}

common::Result<std::uint64_t> WalWriter::next_record_sequence() const {
  if (implementation_ == nullptr) {
    return common::make_unexpected(invalid_writer("next_record_sequence"));
  }
  if (implementation_->sequence_exhausted_) {
    return common::make_unexpected(sequence_exhausted());
  }
  return implementation_->next_record_sequence_;
}

common::Status WalWriter::close() {
  if (implementation_ == nullptr) {
    return common::Status::ok();
  }

  common::Status first_error = implementation_->failure_;
  const auto preserve_first = [&first_error](const common::Status& status) {
    if (first_error.is_ok() && !status.is_ok()) {
      first_error = status;
    }
  };
  preserve_first(implementation_->active_segment_.file.close());
  preserve_first(implementation_->lock_.close());
  preserve_first(implementation_->directory_.close());
  implementation_.reset();
  return first_error;
}

} // namespace chronos::wal
