#include "chronos/wal/wal_writer.hpp"

#include "chronos/io/posix_io.hpp"
#include "chronos/wal/codec.hpp"
#include "chronos/wal/wal_paths.hpp"

#include "io/posix_syscalls.hpp"
#include "wal/wal_segment_internal.hpp"

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

} // namespace

class WalWriter::Impl {
public:
  Impl(io::PosixDirectory directory, io::PosixAdvisoryLock lock,
       detail::ActiveWalSegment active_segment, const std::uint16_t file_permissions) noexcept
      : directory_(std::move(directory)), lock_(std::move(lock)),
        active_segment_(std::move(active_segment)), file_permissions_(file_permissions),
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
    const common::Result<RecordHeader> header = make_record_header(
        {.record_type = kApplicationEntryRecordType,
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
      return sequence_exhausted();
    }

    common::Status status = active_segment_.file.sync_data();
    if (!status.is_ok()) {
      return poison(with_context("synchronize prior WAL segment before rotation", status));
    }
    durable_position_ = written_position_;
    durable_record_sequence_ = written_record_sequence_;

    const std::uint64_t next_segment_number =
        active_segment_.metadata.header.segment_number + 1U;
    common::Result<detail::ActiveWalSegment> next_segment = detail::install_segment(
        directory_, active_segment_.metadata.header.wal_id, next_segment_number,
        next_record_sequence_, file_permissions_);
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

  io::PosixDirectory directory_;
  io::PosixAdvisoryLock lock_;
  detail::ActiveWalSegment active_segment_;
  std::uint16_t file_permissions_;
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

common::Result<WalWriter>
WalWriter::create_new_with(const WalWriterConfig& config, WalLogIdGenerator& id_generator,
                           io::detail::PosixSyscalls& syscalls) {
  common::Result<io::PosixDirectory> directory =
      io::detail::PosixHandleFactory::open_directory(config.directory_path, syscalls);
  if (!directory.has_value()) {
    return common::make_unexpected(with_context("open new WAL directory", directory.error()));
  }
  common::Result<io::PosixAdvisoryLock> lock =
      directory->acquire_exclusive_lock(kWalLockFileName, config.file_permissions);
  if (!lock.has_value()) {
    return common::make_unexpected(with_context("acquire WAL writer ownership", lock.error()));
  }
  common::Result<WalId> wal_id = id_generator.generate();
  if (!wal_id.has_value()) {
    return common::make_unexpected(with_context("generate WAL identity", wal_id.error()));
  }
  if (!wal_id->is_valid()) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kInvalidArgument, "WAL identity generator returned an all-zero value"});
  }

  common::Result<detail::ActiveWalSegment> active_segment = detail::install_segment(
      *directory, *wal_id, kFirstSegmentNumber, kFirstRecordSequence, config.file_permissions);
  if (!active_segment.has_value()) {
    return common::make_unexpected(with_context("install initial WAL segment", active_segment.error()));
  }

  try {
    return WalWriter{std::make_unique<Impl>(std::move(*directory), std::move(*lock),
                                            std::move(*active_segment), config.file_permissions)};
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(common::Status{
        common::StatusCode::kResourceExhausted, "cannot allocate WAL writer ownership state"});
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
  if (implementation_->sequence_exhausted_) {
    return common::make_unexpected(sequence_exhausted());
  }

  common::Result<std::vector<std::byte>> encoded =
      implementation_->encode_application_record(application_payload);
  if (!encoded.has_value()) {
    return common::make_unexpected(encoded.error());
  }
  const auto record_length = static_cast<std::uint64_t>(encoded->size());
  if (record_length > kSegmentSizeLimit - implementation_->active_segment_.metadata.end_offset) {
    const common::Status rotation_status = implementation_->rotate();
    if (!rotation_status.is_ok()) {
      return common::make_unexpected(rotation_status);
    }
  }

  const PhysicalWalPosition record_start = implementation_->written_position_;
  const common::Status write_status = implementation_->active_segment_.file.write_all_at(
      record_start.byte_offset, common::ByteView{*encoded});
  if (!write_status.is_ok()) {
    return common::make_unexpected(implementation_->poison(
        with_context("append complete WAL record", write_status)));
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

common::Result<PhysicalWalPosition> WalWriter::synchronize() {
  if (implementation_ == nullptr || !implementation_->active_segment_.file.is_open()) {
    return common::make_unexpected(invalid_writer("synchronize"));
  }
  if (!implementation_->failure_.is_ok()) {
    return common::make_unexpected(implementation_->failure_);
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
  return implementation_ == nullptr ? WalId{} : implementation_->active_segment_.metadata.header.wal_id;
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
