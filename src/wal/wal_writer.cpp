#include "chronos/wal/wal_writer.hpp"

#include "chronos/io/posix_io.hpp"
#include "chronos/wal/codec.hpp"
#include "chronos/wal/wal_paths.hpp"
#include "io/posix_syscalls.hpp"
#include "wal/wal_segment_internal.hpp"
#include "wal/wal_writer_internal.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
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

[[nodiscard]] common::Status validate_writer_config(const WalWriterConfig& config) {
  if (config.directory_path.empty()) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "WAL directory path must not be empty"};
  }
  if (config.directory_path.find('\0') != std::string::npos) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "WAL directory path contains an embedded NUL byte"};
  }
  if ((config.file_permissions & static_cast<std::uint16_t>(~0777U)) != 0U) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "WAL file permissions contain bits outside 0777"};
  }
  if (config.maximum_application_payload < kApplicationEnvelopeSize) {
    return common::Status{
        common::StatusCode::kInvalidArgument,
        "configured WAL payload maximum is smaller than the application envelope"};
  }
  if (config.maximum_application_payload > kMaximumPayloadLength) {
    return common::Status{common::StatusCode::kOutOfRange,
                          "configured WAL payload maximum exceeds the v1 format limit"};
  }
  const common::Result<RecordLayout> maximum_layout =
      calculate_record_layout(config.maximum_application_payload);
  if (!maximum_layout.has_value()) {
    return with_context("validate configured WAL payload maximum", maximum_layout.error());
  }
  if (config.target_segment_size <= kSegmentHeaderSize) {
    return common::Status{common::StatusCode::kInvalidArgument,
                          "target WAL segment size must exceed the segment header size"};
  }
  if (config.target_segment_size > kSegmentSizeLimit) {
    return common::Status{common::StatusCode::kOutOfRange,
                          "target WAL segment size exceeds the 64 MiB v1 format limit"};
  }
  const std::uint64_t required_size =
      static_cast<std::uint64_t>(kSegmentHeaderSize) + maximum_layout->total_length;
  if (config.target_segment_size < required_size) {
    return common::Status{
        common::StatusCode::kInvalidArgument,
        "target WAL segment size cannot hold one maximum configured application record"};
  }
  return common::Status::ok();
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
    if (segment.end_offset < kSegmentHeaderSize || segment.end_offset > target_segment_size_ ||
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
  const common::Status config_status = validate_writer_config(config);
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
  if (record_length > implementation_->target_segment_size_ -
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
