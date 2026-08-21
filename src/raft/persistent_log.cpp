#include "chronos/raft/persistent_log.hpp"

#include "chronos/common/byte_reader.hpp"
#include "chronos/common/byte_writer.hpp"
#include "chronos/common/crc32c.hpp"
#include "chronos/io/posix_io.hpp"
#include "chronos/raft/multiplexed_log.hpp"
#include "io/posix_syscalls.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace chronos::raft {
namespace {

inline constexpr std::array<std::byte, 8U> kSegmentMagic{
    std::byte{0x43}, std::byte{0x48}, std::byte{0x52}, std::byte{0x4e},
    std::byte{0x52}, std::byte{0x53}, std::byte{0x47}, std::byte{0x00},
};
inline constexpr std::array<std::byte, 8U> kRecoveryAnchorMagic{
    std::byte{0x43}, std::byte{0x48}, std::byte{0x52}, std::byte{0x4e},
    std::byte{0x52}, std::byte{0x42}, std::byte{0x41}, std::byte{0x00},
};
inline constexpr std::uint16_t kSegmentMajor = 1U;
inline constexpr std::uint16_t kSegmentMinor = 0U;
inline constexpr std::uint16_t kRecoveryAnchorMajor = 1U;
inline constexpr std::uint16_t kRecoveryAnchorMinor = 0U;
inline constexpr std::size_t kRecoveryAnchorSize = 64U;
inline constexpr std::string_view kLockName = "LOCK";
inline constexpr std::string_view kSegmentPrefix = "raft-";
inline constexpr std::string_view kSegmentSuffix = ".rlog";
inline constexpr std::string_view kTemporarySuffix = ".tmp";
inline constexpr std::string_view kRecoveryAnchorPrefix = "raft-base-";
inline constexpr std::string_view kRecoveryAnchorSuffix = ".rbase";
inline constexpr std::string_view kRecoveryAnchorTemporarySuffix = ".rbase.tmp";
inline constexpr std::size_t kSegmentDigits = 20U;
inline constexpr std::uint64_t kMinimumTargetSize =
    kRaftSegmentHeaderSize + kMultiplexedLogHeaderSize + kMultiplexedLogTrailerSize + 96U;

[[nodiscard]] common::Status invalid(const char* message) {
  return common::Status{common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status corrupt(const char* message) {
  return common::Status{common::StatusCode::kCorruption, message};
}

[[nodiscard]] common::Status validate_config(const RaftPersistentLogConfig& config) {
  if (config.directory_path.empty() || (config.file_permissions & ~0777U) != 0U ||
      config.target_segment_size < kMinimumTargetSize ||
      config.target_segment_size > kMaximumRaftSegmentSize || config.maximum_segments == 0U ||
      config.maximum_records == 0U || config.maximum_groups == 0U) {
    return invalid("Raft persistent-log path, permissions, size, or limits are invalid");
  }
  return common::Status::ok();
}

[[nodiscard]] std::string segment_name(const std::uint64_t number, const std::string_view suffix) {
  std::array<char, kSegmentDigits> digits{};
  digits.fill('0');
  std::array<char, 32U> encoded{};
  const auto result = std::to_chars(encoded.data(), encoded.data() + encoded.size(), number);
  const std::size_t length = static_cast<std::size_t>(result.ptr - encoded.data());
  std::copy_n(encoded.data(), length, digits.data() + (digits.size() - length));
  std::string name{kSegmentPrefix};
  name.append(digits.data(), digits.size());
  name.append(suffix);
  return name;
}

[[nodiscard]] common::Result<std::uint64_t> parse_segment_name(const std::string_view name) {
  const std::size_t expected = kSegmentPrefix.size() + kSegmentDigits + kSegmentSuffix.size();
  if (name.size() != expected || !name.starts_with(kSegmentPrefix) ||
      !name.ends_with(kSegmentSuffix)) {
    return common::make_unexpected(invalid("Raft segment filename is invalid"));
  }
  const std::string_view digits = name.substr(kSegmentPrefix.size(), kSegmentDigits);
  std::uint64_t number{};
  const auto parsed = std::from_chars(digits.data(), digits.data() + digits.size(), number);
  if (parsed.ec != std::errc{} || parsed.ptr != digits.data() + digits.size() || number == 0U) {
    return common::make_unexpected(invalid("Raft segment number is invalid"));
  }
  return number;
}

[[nodiscard]] bool is_temporary_name(const std::string_view name) {
  const std::size_t expected = kSegmentPrefix.size() + kSegmentDigits + kTemporarySuffix.size();
  if (name.size() != expected || !name.starts_with(kSegmentPrefix) ||
      !name.ends_with(kTemporarySuffix)) {
    return false;
  }
  const std::string_view digits = name.substr(kSegmentPrefix.size(), kSegmentDigits);
  return std::ranges::all_of(digits, [](const char value) { return value >= '0' && value <= '9'; });
}

[[nodiscard]] std::string recovery_anchor_name(const std::uint64_t number,
                                               const std::string_view suffix) {
  std::array<char, kSegmentDigits> digits{};
  digits.fill('0');
  std::array<char, 32U> encoded{};
  const auto result = std::to_chars(encoded.data(), encoded.data() + encoded.size(), number);
  const std::size_t length = static_cast<std::size_t>(result.ptr - encoded.data());
  std::copy_n(encoded.data(), length, digits.data() + (digits.size() - length));
  std::string name{kRecoveryAnchorPrefix};
  name.append(digits.data(), digits.size());
  name.append(suffix);
  return name;
}

[[nodiscard]] common::Result<std::uint64_t>
parse_recovery_anchor_name(const std::string_view name, const std::string_view suffix) {
  const std::size_t expected = kRecoveryAnchorPrefix.size() + kSegmentDigits + suffix.size();
  if (name.size() != expected || !name.starts_with(kRecoveryAnchorPrefix) ||
      !name.ends_with(suffix)) {
    return common::make_unexpected(invalid("Raft recovery-anchor filename is invalid"));
  }
  const std::string_view digits = name.substr(kRecoveryAnchorPrefix.size(), kSegmentDigits);
  std::uint64_t number{};
  const auto parsed = std::from_chars(digits.data(), digits.data() + digits.size(), number);
  if (parsed.ec != std::errc{} || parsed.ptr != digits.data() + digits.size() || number == 0U) {
    return common::make_unexpected(invalid("Raft recovery-anchor number is invalid"));
  }
  return number;
}

[[nodiscard]] bool is_recovery_anchor_temporary_name(const std::string_view name) {
  return parse_recovery_anchor_name(name, kRecoveryAnchorTemporarySuffix).has_value();
}

struct RecoveryAnchor {
  std::uint64_t base_segment_number{};
  std::uint64_t checkpoint_first_sequence{};
  std::uint64_t checkpoint_last_sequence{};
  std::uint64_t checkpoint_group_count{};
};

[[nodiscard]] common::Result<std::array<std::byte, kRecoveryAnchorSize>>
encode_recovery_anchor(const RecoveryAnchor& anchor) {
  if (anchor.base_segment_number == 0U || anchor.checkpoint_first_sequence == 0U ||
      anchor.checkpoint_last_sequence < anchor.checkpoint_first_sequence ||
      anchor.checkpoint_group_count == 0U ||
      anchor.checkpoint_last_sequence - anchor.checkpoint_first_sequence + 1U !=
          anchor.checkpoint_group_count) {
    return common::make_unexpected(invalid("Raft recovery anchor is invalid"));
  }
  std::array<std::byte, kRecoveryAnchorSize> bytes{};
  common::ByteWriter writer{bytes};
  common::Status status = writer.write_exact(kRecoveryAnchorMagic);
  if (status.is_ok())
    status = writer.write_u16_le(kRecoveryAnchorMajor);
  if (status.is_ok())
    status = writer.write_u16_le(kRecoveryAnchorMinor);
  if (status.is_ok())
    status = writer.write_u32_le(kRecoveryAnchorSize);
  if (status.is_ok())
    status = writer.write_u64_le(anchor.base_segment_number);
  if (status.is_ok())
    status = writer.write_u64_le(anchor.checkpoint_first_sequence);
  if (status.is_ok())
    status = writer.write_u64_le(anchor.checkpoint_last_sequence);
  if (status.is_ok())
    status = writer.write_u64_le(anchor.checkpoint_group_count);
  if (status.is_ok())
    status = writer.write_u32_le(0U);
  if (status.is_ok())
    status = writer.zero_fill(12U);
  if (!status.is_ok() || !writer.full()) {
    return common::make_unexpected(
        status.is_ok()
            ? common::Status{common::StatusCode::kInternal, "Raft recovery-anchor layout mismatch"}
            : status);
  }
  common::ByteWriter checksum_writer{common::MutableByteView{bytes}.subspan(48U, 4U)};
  status = checksum_writer.write_u32_le(common::crc32c(bytes));
  if (!status.is_ok())
    return common::make_unexpected(status);
  return bytes;
}

[[nodiscard]] common::Result<RecoveryAnchor>
decode_recovery_anchor(const common::ByteView encoded, const std::uint64_t expected_number) {
  if (encoded.size() != kRecoveryAnchorSize)
    return common::make_unexpected(corrupt("Raft recovery anchor has an invalid size"));
  std::array<std::byte, kRecoveryAnchorSize> checked{};
  std::copy(encoded.begin(), encoded.end(), checked.begin());
  common::ByteReader checksum_reader{common::ByteView{checked}.subspan(48U, 4U)};
  auto stored_checksum = checksum_reader.read_u32_le();
  std::fill(checked.begin() + 48, checked.begin() + 52, std::byte{0});
  if (!stored_checksum.has_value() || common::crc32c(checked) != *stored_checksum)
    return common::make_unexpected(corrupt("Raft recovery-anchor checksum mismatch"));
  common::ByteReader reader{encoded};
  auto magic = reader.read_exact(8U);
  auto major = reader.read_u16_le();
  auto minor = reader.read_u16_le();
  auto header_size = reader.read_u32_le();
  auto base_segment = reader.read_u64_le();
  auto first_sequence = reader.read_u64_le();
  auto last_sequence = reader.read_u64_le();
  auto group_count = reader.read_u64_le();
  auto ignored_checksum = reader.read_u32_le();
  auto reserved = reader.read_exact(12U);
  static_cast<void>(ignored_checksum);
  if (!magic || !major || !minor || !header_size || !base_segment || !first_sequence ||
      !last_sequence || !group_count || !reserved ||
      !std::ranges::equal(*magic, kRecoveryAnchorMagic)) {
    return common::make_unexpected(corrupt("Raft recovery anchor is invalid"));
  }
  if (*major != kRecoveryAnchorMajor || *minor > kRecoveryAnchorMinor) {
    return common::make_unexpected(common::Status{common::StatusCode::kNotSupported,
                                                  "Raft recovery-anchor version is unsupported"});
  }
  if (*header_size != kRecoveryAnchorSize || *base_segment != expected_number ||
      *first_sequence == 0U || *last_sequence < *first_sequence || *group_count == 0U ||
      *last_sequence - *first_sequence + 1U != *group_count ||
      std::ranges::any_of(*reserved, [](const std::byte value) { return value != std::byte{0}; })) {
    return common::make_unexpected(corrupt("Raft recovery-anchor fields are invalid"));
  }
  return RecoveryAnchor{*base_segment, *first_sequence, *last_sequence, *group_count};
}

[[nodiscard]] common::Result<std::array<std::byte, kRaftSegmentHeaderSize>>
encode_segment_header(const std::uint64_t segment_number, const std::uint64_t first_sequence) {
  if (segment_number == 0U || first_sequence == 0U) {
    return common::make_unexpected(invalid("Raft segment identity is invalid"));
  }
  std::array<std::byte, kRaftSegmentHeaderSize> bytes{};
  common::ByteWriter writer{bytes};
  common::Status status = writer.write_exact(kSegmentMagic);
  if (status.is_ok())
    status = writer.write_u16_le(kSegmentMajor);
  if (status.is_ok())
    status = writer.write_u16_le(kSegmentMinor);
  if (status.is_ok())
    status = writer.write_u32_le(kRaftSegmentHeaderSize);
  if (status.is_ok())
    status = writer.write_u64_le(segment_number);
  if (status.is_ok())
    status = writer.write_u64_le(first_sequence);
  if (status.is_ok())
    status = writer.write_u32_le(0U);
  if (status.is_ok())
    status = writer.zero_fill(28U);
  if (!status.is_ok() || !writer.full()) {
    return common::make_unexpected(
        status.is_ok()
            ? common::Status{common::StatusCode::kInternal, "Raft segment header layout mismatch"}
            : status);
  }
  const std::uint32_t checksum = common::crc32c(bytes);
  common::ByteWriter checksum_writer{common::MutableByteView{bytes}.subspan(32U, 4U)};
  status = checksum_writer.write_u32_le(checksum);
  if (!status.is_ok())
    return common::make_unexpected(status);
  return bytes;
}

struct SegmentHeader {
  std::uint64_t number{};
  std::uint64_t first_sequence{};
};

[[nodiscard]] common::Result<SegmentHeader>
decode_segment_header(const common::ByteView encoded, const std::uint64_t expected_number) {
  if (encoded.size() != kRaftSegmentHeaderSize) {
    return common::make_unexpected(corrupt("Raft segment header is truncated"));
  }
  std::array<std::byte, kRaftSegmentHeaderSize> checked{};
  std::copy(encoded.begin(), encoded.end(), checked.begin());
  common::ByteReader checksum_reader{common::ByteView{checked}.subspan(32U, 4U)};
  auto stored_checksum = checksum_reader.read_u32_le();
  std::fill(checked.begin() + 32, checked.begin() + 36, std::byte{0});
  if (!stored_checksum.has_value() || common::crc32c(checked) != *stored_checksum) {
    return common::make_unexpected(corrupt("Raft segment header checksum mismatch"));
  }
  common::ByteReader reader{encoded};
  auto magic = reader.read_exact(8U);
  auto major = reader.read_u16_le();
  auto minor = reader.read_u16_le();
  auto header_size = reader.read_u32_le();
  auto number = reader.read_u64_le();
  auto first_sequence = reader.read_u64_le();
  auto ignored_checksum = reader.read_u32_le();
  auto reserved = reader.read_exact(28U);
  static_cast<void>(ignored_checksum);
  if (!magic || !major || !minor || !header_size || !number || !first_sequence || !reserved ||
      !std::ranges::equal(*magic, kSegmentMagic)) {
    return common::make_unexpected(corrupt("Raft segment header is invalid"));
  }
  if (*major != kSegmentMajor || *minor > kSegmentMinor) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kNotSupported, "Raft segment version is unsupported"});
  }
  if (*header_size != kRaftSegmentHeaderSize || *number != expected_number ||
      *first_sequence == 0U ||
      std::ranges::any_of(*reserved, [](const std::byte value) { return value != std::byte{0}; })) {
    return common::make_unexpected(corrupt("Raft segment identity or reserved bytes are invalid"));
  }
  return SegmentHeader{*number, *first_sequence};
}

[[nodiscard]] common::Status read_exact(const io::PosixFile& file, const std::uint64_t offset,
                                        const common::MutableByteView destination) {
  auto count = file.read_at(offset, destination);
  if (!count.has_value())
    return count.error();
  if (*count != destination.size())
    return corrupt("Raft persistent log is truncated");
  return common::Status::ok();
}

struct InstalledSegment {
  io::PosixFile file;
  std::uint64_t end_offset{kRaftSegmentHeaderSize};
};

[[nodiscard]] common::Result<InstalledSegment>
install_segment(const io::PosixDirectory& directory, const RaftPersistentLogConfig& config,
                const std::uint64_t segment_number, const std::uint64_t first_sequence) {
  auto header = encode_segment_header(segment_number, first_sequence);
  if (!header.has_value())
    return common::make_unexpected(header.error());
  const std::string temporary = segment_name(segment_number, kTemporarySuffix);
  const std::string final = segment_name(segment_number, kSegmentSuffix);
  auto file = directory.create_exclusive_regular_file(temporary, config.file_permissions);
  if (!file.has_value())
    return common::make_unexpected(file.error());
  common::Status status = file->write_all_at(0U, *header);
  if (status.is_ok())
    status = file->sync_all();
  if (status.is_ok())
    status = directory.rename_no_replace({temporary, final});
  if (status.is_ok())
    status = directory.sync();
  if (!status.is_ok())
    return common::make_unexpected(status);
  return InstalledSegment{std::move(*file), kRaftSegmentHeaderSize};
}

[[nodiscard]] common::Status install_recovery_anchor(const io::PosixDirectory& directory,
                                                     const RaftPersistentLogConfig& config,
                                                     const RecoveryAnchor& anchor) {
  auto encoded = encode_recovery_anchor(anchor);
  if (!encoded.has_value())
    return encoded.error();
  const std::string temporary =
      recovery_anchor_name(anchor.base_segment_number, kRecoveryAnchorTemporarySuffix);
  const std::string final = recovery_anchor_name(anchor.base_segment_number, kRecoveryAnchorSuffix);
  auto file = directory.create_exclusive_regular_file(temporary, config.file_permissions);
  if (!file.has_value())
    return file.error();
  common::Status status = file->write_all_at(0U, *encoded);
  if (status.is_ok())
    status = file->sync_all();
  if (status.is_ok())
    status = directory.rename_no_replace({temporary, final});
  if (status.is_ok())
    status = directory.sync();
  const common::Status close_status = file->close();
  if (status.is_ok() && !close_status.is_ok())
    status = close_status;
  return status;
}

} // namespace

class RaftPersistentLog::Impl {
public:
  RaftPersistentLogConfig config;
  io::PosixDirectory directory;
  io::PosixAdvisoryLock lock;
  io::PosixFile active_file;
  RaftPersistentLogRecovery recovered;
  std::set<GroupId> known_groups;
  std::map<std::uint64_t, std::uint64_t> segment_record_counts;
  std::set<std::uint64_t> recovery_anchors;
  common::Status failure;
  bool open{};

  [[nodiscard]] common::Status fail(common::Status status) {
    if (failure.is_ok())
      failure = std::move(status);
    return failure;
  }
};

RaftPersistentLog::RaftPersistentLog() noexcept = default;
RaftPersistentLog::RaftPersistentLog(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
RaftPersistentLog::~RaftPersistentLog() = default;
RaftPersistentLog::RaftPersistentLog(RaftPersistentLog&&) noexcept = default;
RaftPersistentLog& RaftPersistentLog::operator=(RaftPersistentLog&&) noexcept = default;

common::Result<RaftPersistentLog>
RaftPersistentLog::create_new(const RaftPersistentLogConfig& config) {
  return create_new_with(config, io::detail::system_posix_syscalls());
}

common::Result<RaftPersistentLog>
RaftPersistentLog::create_new_with(const RaftPersistentLogConfig& config,
                                   io::detail::PosixSyscalls& syscalls) {
  const common::Status valid = validate_config(config);
  if (!valid.is_ok())
    return common::make_unexpected(valid);
  auto directory = io::detail::PosixHandleFactory::open_directory(config.directory_path, syscalls);
  if (!directory.has_value())
    return common::make_unexpected(directory.error());
  auto initial_entries = directory->list_entries();
  if (!initial_entries.has_value())
    return common::make_unexpected(initial_entries.error());
  for (const io::DirectoryEntry& entry : *initial_entries) {
    if (entry.name != kLockName || entry.type != io::DirectoryEntryType::kRegularFile) {
      return common::make_unexpected(invalid("new Raft persistent-log directory is not empty"));
    }
  }
  auto lock = directory->acquire_exclusive_lock(kLockName, config.file_permissions);
  if (!lock.has_value())
    return common::make_unexpected(lock.error());
  auto locked_entries = directory->list_entries();
  if (!locked_entries.has_value())
    return common::make_unexpected(locked_entries.error());
  if (locked_entries->size() != 1U || locked_entries->front().name != kLockName ||
      locked_entries->front().type != io::DirectoryEntryType::kRegularFile) {
    return common::make_unexpected(invalid("new Raft persistent-log directory changed"));
  }
  common::Status status = directory->sync();
  if (!status.is_ok())
    return common::make_unexpected(status);
  auto installed = install_segment(*directory, config, 1U, 1U);
  if (!installed.has_value())
    return common::make_unexpected(installed.error());
  auto impl = std::make_unique<Impl>();
  impl->config = config;
  impl->directory = std::move(*directory);
  impl->lock = std::move(*lock);
  impl->active_file = std::move(installed->file);
  impl->recovered.written_position = {1U, installed->end_offset, 0U};
  impl->recovered.segment_count = 1U;
  impl->segment_record_counts.emplace(1U, 0U);
  impl->open = true;
  return RaftPersistentLog{std::move(impl)};
}

common::Result<RaftPersistentLog>
RaftPersistentLog::open_existing(const RaftPersistentLogConfig& config,
                                 const RaftPersistentLogOpenOptions& options) {
  const common::Status valid = validate_config(config);
  if (!valid.is_ok())
    return common::make_unexpected(valid);
  auto directory = io::PosixDirectory::open(config.directory_path);
  if (!directory.has_value())
    return common::make_unexpected(directory.error());
  auto lock = directory->acquire_existing_exclusive_lock(kLockName);
  if (!lock.has_value())
    return common::make_unexpected(lock.error());
  auto entries = directory->list_entries();
  if (!entries.has_value())
    return common::make_unexpected(entries.error());

  std::vector<std::pair<std::uint64_t, std::string>> segments;
  std::vector<std::pair<std::uint64_t, std::string>> anchors;
  std::vector<std::string> temporaries;
  for (const io::DirectoryEntry& entry : *entries) {
    if (entry.name == kLockName) {
      if (entry.type != io::DirectoryEntryType::kRegularFile)
        return common::make_unexpected(corrupt("Raft LOCK is not a regular file"));
      continue;
    }
    if (entry.type != io::DirectoryEntryType::kRegularFile) {
      return common::make_unexpected(corrupt("Raft directory contains a non-regular entry"));
    }
    if (is_temporary_name(entry.name) || is_recovery_anchor_temporary_name(entry.name)) {
      temporaries.push_back(entry.name);
      continue;
    }
    auto anchor_number = parse_recovery_anchor_name(entry.name, kRecoveryAnchorSuffix);
    if (anchor_number.has_value()) {
      anchors.emplace_back(*anchor_number, entry.name);
      continue;
    }
    auto number = parse_segment_name(entry.name);
    if (!number.has_value())
      return common::make_unexpected(corrupt("Raft directory contains an unknown entry"));
    segments.emplace_back(*number, entry.name);
  }
  if (segments.empty())
    return common::make_unexpected(corrupt("Raft segment set is empty"));
  std::ranges::sort(segments);
  std::ranges::sort(anchors);

  std::optional<RecoveryAnchor> recovery_anchor;
  if (!anchors.empty()) {
    const auto& [number, name] = anchors.back();
    auto file = directory->open_regular_file(name, io::FileOpenMode::kReadOnly);
    if (!file.has_value())
      return common::make_unexpected(file.error());
    auto size = file->size();
    if (!size.has_value())
      return common::make_unexpected(size.error());
    if (*size != kRecoveryAnchorSize)
      return common::make_unexpected(corrupt("Raft recovery anchor has an invalid size"));
    std::array<std::byte, kRecoveryAnchorSize> bytes{};
    common::Status status = read_exact(*file, 0U, bytes);
    if (!status.is_ok())
      return common::make_unexpected(status);
    auto decoded = decode_recovery_anchor(bytes, number);
    if (!decoded.has_value())
      return common::make_unexpected(decoded.error());
    recovery_anchor = *decoded;
  }
  if (recovery_anchor.has_value() &&
      recovery_anchor->checkpoint_group_count > config.maximum_groups) {
    return common::make_unexpected(corrupt("Raft recovery checkpoint group count exceeds limits"));
  }

  const std::uint64_t base_segment =
      recovery_anchor.has_value() ? recovery_anchor->base_segment_number : 1U;
  const auto retained_begin = std::ranges::lower_bound(
      segments, base_segment, {},
      [](const std::pair<std::uint64_t, std::string>& segment) { return segment.first; });
  if (retained_begin == segments.end() || retained_begin->first != base_segment) {
    return common::make_unexpected(corrupt("Raft recovery base segment is absent"));
  }
  std::vector<std::pair<std::uint64_t, std::string>> retained_segments{retained_begin,
                                                                       segments.end()};
  if (retained_segments.size() > config.maximum_segments)
    return common::make_unexpected(corrupt("Raft retained segment set exceeds limits"));
  for (std::size_t index = 0U; index < retained_segments.size(); ++index) {
    if (retained_segments[index].first != base_segment + index)
      return common::make_unexpected(corrupt("Raft retained segment numbering is not contiguous"));
  }

  std::map<GroupId, GroupPersistentState> latest;
  std::map<std::uint64_t, std::uint64_t> segment_record_counts;
  std::set<GroupId> checkpoint_groups;
  RaftPersistentLogRecovery recovery;
  recovery.base_segment_number = base_segment;
  std::uint64_t last_sequence =
      recovery_anchor.has_value() ? recovery_anchor->checkpoint_first_sequence - 1U : 0U;
  std::uint64_t active_end = kRaftSegmentHeaderSize;
  for (std::size_t segment_index = 0U; segment_index < retained_segments.size(); ++segment_index) {
    const auto& [number, name] = retained_segments[segment_index];
    auto file = directory->open_regular_file(name, segment_index + 1U == retained_segments.size()
                                                       ? io::FileOpenMode::kReadWrite
                                                       : io::FileOpenMode::kReadOnly);
    if (!file.has_value())
      return common::make_unexpected(file.error());
    auto file_size = file->size();
    if (!file_size.has_value())
      return common::make_unexpected(file_size.error());
    if (*file_size < kRaftSegmentHeaderSize || *file_size > kMaximumRaftSegmentSize) {
      return common::make_unexpected(corrupt("Raft segment size is invalid"));
    }
    std::array<std::byte, kRaftSegmentHeaderSize> header_bytes{};
    common::Status status = read_exact(*file, 0U, header_bytes);
    if (!status.is_ok())
      return common::make_unexpected(status);
    auto header = decode_segment_header(header_bytes, number);
    if (!header.has_value())
      return common::make_unexpected(header.error());
    if (last_sequence == std::numeric_limits<std::uint64_t>::max() ||
        header->first_sequence != last_sequence + 1U) {
      return common::make_unexpected(corrupt("Raft segment first sequence is not contiguous"));
    }

    std::uint64_t offset = kRaftSegmentHeaderSize;
    std::uint64_t segment_record_count = 0U;
    while (offset < *file_size) {
      const std::uint64_t remaining = *file_size - offset;
      bool incomplete = remaining < kMultiplexedLogHeaderSize;
      std::array<std::byte, kMultiplexedLogHeaderSize> record_header{};
      std::optional<MultiplexedLogRecordHeader> inspected;
      if (!incomplete) {
        status = read_exact(*file, offset, record_header);
        if (!status.is_ok())
          return common::make_unexpected(status);
        auto parsed = inspect_multiplexed_log_record_header_v1(record_header);
        if (!parsed.has_value())
          return common::make_unexpected(parsed.error());
        inspected = *parsed;
        incomplete = inspected->encoded_size > remaining;
      }
      if (incomplete) {
        if (recovery_anchor.has_value() &&
            last_sequence < recovery_anchor->checkpoint_last_sequence) {
          return common::make_unexpected(
              corrupt("Raft recovery checkpoint contains an incomplete record"));
        }
        if (segment_index + 1U != retained_segments.size() ||
            !options.repair_incomplete_final_tail) {
          return common::make_unexpected(corrupt("Raft final record is incomplete"));
        }
        recovery.repaired_bytes = *file_size - offset;
        status = file->truncate(offset);
        if (status.is_ok())
          status = file->sync_all();
        if (status.is_ok())
          status = directory->sync();
        if (!status.is_ok())
          return common::make_unexpected(status);
        file_size = offset;
        break;
      }
      std::vector<std::byte> record(inspected->encoded_size);
      status = read_exact(*file, offset, record);
      if (!status.is_ok())
        return common::make_unexpected(status);
      auto decoded = decode_multiplexed_log_record_v1(record);
      if (!decoded.has_value())
        return common::make_unexpected(decoded.error());
      if (last_sequence == std::numeric_limits<std::uint64_t>::max() ||
          decoded->persistent.physical_sequence != last_sequence + 1U) {
        return common::make_unexpected(corrupt("Raft physical sequence is not contiguous"));
      }
      if (++recovery.record_count > config.maximum_records) {
        return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                      "Raft recovery record limit exceeded"});
      }
      ++segment_record_count;
      if (recovery_anchor.has_value() &&
          decoded->persistent.physical_sequence <= recovery_anchor->checkpoint_last_sequence) {
        if (!checkpoint_groups.insert(decoded->persistent.group_id).second) {
          return common::make_unexpected(
              corrupt("Raft recovery checkpoint repeats a logical group"));
        }
      }
      latest[decoded->persistent.group_id] = std::move(decoded->persistent);
      if (latest.size() > config.maximum_groups) {
        return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                      "Raft recovery group limit exceeded"});
      }
      last_sequence = decoded->persistent.physical_sequence;
      offset += inspected->encoded_size;
    }
    segment_record_counts.emplace(number, segment_record_count);
    active_end = offset;
  }

  if (recovery_anchor.has_value() &&
      (last_sequence < recovery_anchor->checkpoint_last_sequence ||
       checkpoint_groups.size() != recovery_anchor->checkpoint_group_count)) {
    return common::make_unexpected(corrupt("Raft recovery checkpoint is incomplete"));
  }

  bool removed_stale = false;
  for (const std::string& temporary : temporaries) {
    common::Status status = directory->remove_file(temporary);
    if (!status.is_ok())
      return common::make_unexpected(status);
    removed_stale = true;
  }
  for (const auto& [number, name] : segments) {
    if (number >= base_segment)
      break;
    common::Status status = directory->remove_file(name);
    if (!status.is_ok())
      return common::make_unexpected(status);
    removed_stale = true;
  }
  for (const auto& [number, name] : anchors) {
    if (number == base_segment)
      continue;
    common::Status status = directory->remove_file(name);
    if (!status.is_ok())
      return common::make_unexpected(status);
    removed_stale = true;
  }
  if (removed_stale) {
    common::Status status = directory->sync();
    if (!status.is_ok())
      return common::make_unexpected(status);
  }
  auto active =
      directory->open_regular_file(retained_segments.back().second, io::FileOpenMode::kReadWrite);
  if (!active.has_value())
    return common::make_unexpected(active.error());
  common::Status status = active->sync_all();
  if (status.is_ok())
    status = directory->sync();
  if (!status.is_ok())
    return common::make_unexpected(status);

  recovery.segment_count = retained_segments.size();
  recovery.written_position = {retained_segments.back().first, active_end, last_sequence};
  recovery.durable_physical_sequence = last_sequence;
  recovery.latest_group_states.reserve(latest.size());
  for (auto& [group, persistent] : latest) {
    static_cast<void>(group);
    recovery.latest_group_states.push_back(std::move(persistent));
  }
  auto impl = std::make_unique<Impl>();
  impl->config = config;
  impl->directory = std::move(*directory);
  impl->lock = std::move(*lock);
  impl->active_file = std::move(*active);
  impl->recovered = std::move(recovery);
  impl->segment_record_counts = std::move(segment_record_counts);
  if (recovery_anchor.has_value())
    impl->recovery_anchors.insert(recovery_anchor->base_segment_number);
  for (const GroupPersistentState& persistent : impl->recovered.latest_group_states)
    impl->known_groups.insert(persistent.group_id);
  impl->open = true;
  return RaftPersistentLog{std::move(impl)};
}

common::Result<RaftPhysicalPosition>
RaftPersistentLog::append(const GroupPersistentState& persistent) {
  if (!impl_ || !impl_->open) {
    return common::make_unexpected(invalid("Raft persistent log is not open"));
  }
  if (!impl_->failure.is_ok())
    return common::make_unexpected(impl_->failure);
  const std::uint64_t current_sequence = impl_->recovered.written_position.physical_sequence;
  if (current_sequence == std::numeric_limits<std::uint64_t>::max() ||
      persistent.physical_sequence != current_sequence + 1U) {
    return common::make_unexpected(invalid("Raft physical sequence is not the next sequence"));
  }
  if (impl_->recovered.record_count >= impl_->config.maximum_records) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "Raft persistent-log record limit exceeded"});
  }
  if (!impl_->known_groups.contains(persistent.group_id) &&
      impl_->known_groups.size() >= impl_->config.maximum_groups) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "Raft persistent-log group limit exceeded"});
  }
  auto encoded = encode_multiplexed_log_record_v1(persistent);
  if (!encoded.has_value())
    return common::make_unexpected(encoded.error());
  if (encoded->size() + kRaftSegmentHeaderSize > impl_->config.target_segment_size) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "Raft record exceeds segment target size"});
  }
  auto& position = impl_->recovered.written_position;
  const bool rotate = position.end_offset > impl_->config.target_segment_size - encoded->size();
  if (rotate && (impl_->recovered.segment_count >= impl_->config.maximum_segments ||
                 position.segment_number == std::numeric_limits<std::uint64_t>::max())) {
    return common::make_unexpected(common::Status{common::StatusCode::kResourceExhausted,
                                                  "Raft segment capacity is exhausted"});
  }
  // Allocate group bookkeeping before the first syscall. If later I/O fails the owner is poisoned,
  // so an in-memory entry for an unpersisted group can never influence another append.
  impl_->known_groups.insert(persistent.group_id);
  if (rotate) {
    common::Status status = impl_->active_file.sync_data();
    if (status.is_ok()) {
      impl_->recovered.durable_physical_sequence = position.physical_sequence;
      status = impl_->active_file.close();
    }
    if (!status.is_ok())
      return common::make_unexpected(impl_->fail(status));
    auto installed = install_segment(impl_->directory, impl_->config, position.segment_number + 1U,
                                     persistent.physical_sequence);
    if (!installed.has_value())
      return common::make_unexpected(impl_->fail(installed.error()));
    impl_->active_file = std::move(installed->file);
    ++position.segment_number;
    position.end_offset = installed->end_offset;
    ++impl_->recovered.segment_count;
    impl_->segment_record_counts.emplace(position.segment_number, 0U);
  }
  common::Status status = impl_->active_file.write_all_at(position.end_offset, *encoded);
  if (!status.is_ok())
    return common::make_unexpected(impl_->fail(status));
  position.end_offset += encoded->size();
  position.physical_sequence = persistent.physical_sequence;
  ++impl_->recovered.record_count;
  ++impl_->segment_record_counts[position.segment_number];
  return position;
}

common::Result<RaftPhysicalPosition> RaftPersistentLog::synchronize() {
  if (!impl_ || !impl_->open) {
    return common::make_unexpected(invalid("Raft persistent log is not open"));
  }
  if (!impl_->failure.is_ok())
    return common::make_unexpected(impl_->failure);
  common::Status status = impl_->active_file.sync_data();
  if (!status.is_ok())
    return common::make_unexpected(impl_->fail(status));
  impl_->recovered.durable_physical_sequence = impl_->recovered.written_position.physical_sequence;
  return impl_->recovered.written_position;
}

common::Result<RaftPersistentLogReclamation>
RaftPersistentLog::checkpoint_and_reclaim(const std::vector<GroupPersistentState>& checkpoint) {
  if (!impl_ || !impl_->open)
    return common::make_unexpected(invalid("Raft persistent log is not open"));
  if (!impl_->failure.is_ok())
    return common::make_unexpected(impl_->failure);
  if (checkpoint.empty() || checkpoint.size() < impl_->known_groups.size() ||
      checkpoint.size() > impl_->config.maximum_groups) {
    return common::make_unexpected(invalid("Raft reclamation checkpoint group count is invalid"));
  }
  if (checkpoint.size() > impl_->config.maximum_records - impl_->recovered.record_count) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "Raft reclamation checkpoint exceeds the transitional record limit"});
  }

  std::set<GroupId> checkpoint_groups;
  std::vector<std::size_t> encoded_sizes;
  encoded_sizes.reserve(checkpoint.size());
  std::uint64_t expected_sequence = impl_->recovered.written_position.physical_sequence;
  for (const GroupPersistentState& persistent : checkpoint) {
    if (expected_sequence == std::numeric_limits<std::uint64_t>::max()) {
      return common::make_unexpected(
          invalid("Raft reclamation checkpoint identity or sequence is invalid"));
    }
    const std::uint64_t next_sequence = expected_sequence + 1U;
    if (persistent.physical_sequence != next_sequence ||
        !checkpoint_groups.insert(persistent.group_id).second) {
      return common::make_unexpected(
          invalid("Raft reclamation checkpoint identity or sequence is invalid"));
    }
    expected_sequence = next_sequence;
    auto encoded = encode_multiplexed_log_record_v1(persistent);
    if (!encoded.has_value())
      return common::make_unexpected(encoded.error());
    if (encoded->size() + kRaftSegmentHeaderSize > impl_->config.target_segment_size) {
      return common::make_unexpected(
          common::Status{common::StatusCode::kResourceExhausted,
                         "Raft reclamation checkpoint record exceeds segment target size"});
    }
    encoded_sizes.push_back(encoded->size());
  }
  if (!std::ranges::includes(checkpoint_groups, impl_->known_groups)) {
    return common::make_unexpected(invalid("Raft reclamation checkpoint group set is incomplete"));
  }

  std::uint64_t additional_segments = 1U;
  std::uint64_t simulated_end = kRaftSegmentHeaderSize;
  for (const std::size_t encoded_size : encoded_sizes) {
    if (simulated_end > impl_->config.target_segment_size - encoded_size) {
      ++additional_segments;
      simulated_end = kRaftSegmentHeaderSize;
    }
    simulated_end += encoded_size;
  }
  const auto& old_position = impl_->recovered.written_position;
  if (old_position.segment_number == std::numeric_limits<std::uint64_t>::max() ||
      additional_segments > impl_->config.maximum_segments - impl_->recovered.segment_count) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kResourceExhausted,
                       "Raft reclamation checkpoint exceeds the transitional segment limit"});
  }

  common::Status status = impl_->active_file.sync_data();
  if (status.is_ok()) {
    impl_->recovered.durable_physical_sequence = old_position.physical_sequence;
    status = impl_->active_file.close();
  }
  if (!status.is_ok())
    return common::make_unexpected(impl_->fail(status));

  const std::uint64_t base_segment = old_position.segment_number + 1U;
  auto installed = install_segment(impl_->directory, impl_->config, base_segment,
                                   checkpoint.front().physical_sequence);
  if (!installed.has_value())
    return common::make_unexpected(impl_->fail(installed.error()));
  impl_->active_file = std::move(installed->file);
  impl_->recovered.written_position.segment_number = base_segment;
  impl_->recovered.written_position.end_offset = installed->end_offset;
  ++impl_->recovered.segment_count;
  impl_->segment_record_counts.emplace(base_segment, 0U);

  for (const GroupPersistentState& persistent : checkpoint) {
    auto appended = append(persistent);
    if (!appended.has_value())
      return common::make_unexpected(impl_->fail(appended.error()));
  }
  auto synchronized = synchronize();
  if (!synchronized.has_value())
    return common::make_unexpected(impl_->fail(synchronized.error()));

  const RecoveryAnchor anchor{base_segment, checkpoint.front().physical_sequence,
                              checkpoint.back().physical_sequence, checkpoint.size()};
  status = install_recovery_anchor(impl_->directory, impl_->config, anchor);
  if (!status.is_ok())
    return common::make_unexpected(impl_->fail(status));
  impl_->recovery_anchors.insert(base_segment);

  std::uint64_t reclaimed_segments = 0U;
  std::uint64_t reclaimed_records = 0U;
  for (auto record_count = impl_->segment_record_counts.begin();
       record_count != impl_->segment_record_counts.end() && record_count->first < base_segment;) {
    status = impl_->directory.remove_file(segment_name(record_count->first, kSegmentSuffix));
    if (!status.is_ok())
      return common::make_unexpected(impl_->fail(status));
    ++reclaimed_segments;
    reclaimed_records += record_count->second;
    record_count = impl_->segment_record_counts.erase(record_count);
  }
  status = impl_->directory.sync();
  if (!status.is_ok())
    return common::make_unexpected(impl_->fail(status));

  for (auto anchor_number = impl_->recovery_anchors.begin();
       anchor_number != impl_->recovery_anchors.end() && *anchor_number < base_segment;) {
    status =
        impl_->directory.remove_file(recovery_anchor_name(*anchor_number, kRecoveryAnchorSuffix));
    if (!status.is_ok())
      return common::make_unexpected(impl_->fail(status));
    anchor_number = impl_->recovery_anchors.erase(anchor_number);
  }
  status = impl_->directory.sync();
  if (!status.is_ok())
    return common::make_unexpected(impl_->fail(status));

  impl_->recovered.base_segment_number = base_segment;
  impl_->recovered.segment_count -= reclaimed_segments;
  impl_->recovered.record_count -= reclaimed_records;
  return RaftPersistentLogReclamation{
      .base_segment_number = base_segment,
      .checkpoint_first_physical_sequence = checkpoint.front().physical_sequence,
      .checkpoint_last_physical_sequence = checkpoint.back().physical_sequence,
      .reclaimed_segments = reclaimed_segments,
      .reclaimed_records = reclaimed_records};
}

const RaftPersistentLogRecovery& RaftPersistentLog::recovery() const noexcept {
  static const RaftPersistentLogRecovery empty;
  return impl_ ? impl_->recovered : empty;
}

RaftPhysicalPosition RaftPersistentLog::written_position() const noexcept {
  return impl_ ? impl_->recovered.written_position : RaftPhysicalPosition{};
}

std::uint64_t RaftPersistentLog::durable_physical_sequence() const noexcept {
  return impl_ ? impl_->recovered.durable_physical_sequence : 0U;
}

bool RaftPersistentLog::is_open() const noexcept {
  return impl_ && impl_->open;
}

bool RaftPersistentLog::is_failed() const noexcept {
  return impl_ && !impl_->failure.is_ok();
}

common::Status RaftPersistentLog::failure_status() const {
  return impl_ ? impl_->failure : invalid("Raft persistent log has no state");
}

common::Status RaftPersistentLog::close() {
  if (!impl_ || !impl_->open)
    return common::Status::ok();
  common::Status first = impl_->active_file.close();
  const common::Status lock_status = impl_->lock.close();
  if (first.is_ok() && !lock_status.is_ok())
    first = lock_status;
  const common::Status directory_status = impl_->directory.close();
  if (first.is_ok() && !directory_status.is_ok())
    first = directory_status;
  impl_->open = false;
  return first;
}

} // namespace chronos::raft
