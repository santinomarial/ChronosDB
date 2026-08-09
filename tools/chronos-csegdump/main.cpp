#include "chronos/cseg/inspection.hpp"
#include "chronos/schema/logical_type.hpp"

#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

namespace {

constexpr std::uint64_t kDefaultMaximumInputBytes = std::uint64_t{1U} << 30U;

struct Options {
  std::filesystem::path path;
  std::uint64_t maximum_input_bytes{kDefaultMaximumInputBytes};
  bool descriptors{};
};

void print_usage(const std::string_view program) {
  std::cerr << "Usage: " << program << " [--max-bytes N] [--descriptors] <cseg-file>\n";
}

[[nodiscard]] std::optional<Options> parse_options(const std::span<const char* const> arguments) {
  Options options;
  for (std::size_t index = 1U; index < arguments.size(); ++index) {
    const std::string_view argument{arguments[index]};
    if (argument == "--descriptors") {
      options.descriptors = true;
      continue;
    }
    if (argument == "--max-bytes") {
      if (++index >= arguments.size()) {
        return std::nullopt;
      }
      const std::string_view value{arguments[index]};
      const auto [end, error] =
          std::from_chars(value.data(), value.data() + value.size(), options.maximum_input_bytes);
      if (error != std::errc{} || end != value.data() + value.size() ||
          options.maximum_input_bytes == 0U) {
        return std::nullopt;
      }
      continue;
    }
    if (!argument.empty() && argument.front() == '-') {
      return std::nullopt;
    }
    if (!options.path.empty()) {
      return std::nullopt;
    }
    options.path = argument;
  }
  return options.path.empty() ? std::nullopt : std::optional<Options>{std::move(options)};
}

[[nodiscard]] chronos::common::Result<std::vector<std::byte>> read_file(const Options& options) {
  std::error_code error;
  const std::uintmax_t file_size = std::filesystem::file_size(options.path, error);
  if (error) {
    return chronos::common::make_unexpected(chronos::common::Status{
        chronos::common::StatusCode::kIoError, "cannot inspect CSEG input file size"});
  }
  if (file_size > options.maximum_input_bytes ||
      file_size > std::numeric_limits<std::size_t>::max() ||
      file_size > static_cast<std::uintmax_t>(std::numeric_limits<std::streamsize>::max())) {
    return chronos::common::make_unexpected(chronos::common::Status{
        chronos::common::StatusCode::kResourceExhausted,
        "CSEG input file exceeds the configured in-memory inspection limit"});
  }
  std::ifstream input{options.path, std::ios::binary};
  if (!input.is_open()) {
    return chronos::common::make_unexpected(chronos::common::Status{
        chronos::common::StatusCode::kIoError, "cannot open CSEG input file"});
  }
  std::vector<std::byte> bytes(static_cast<std::size_t>(file_size));
  if (!bytes.empty()) {
    // Character streams are permitted to access an object's byte representation.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    input.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
  }
  if (!input || input.peek() != std::ifstream::traits_type::eof()) {
    return chronos::common::make_unexpected(
        chronos::common::Status{chronos::common::StatusCode::kIoError,
                                "CSEG input changed or could not be read as one exact snapshot"});
  }
  return bytes;
}

template <typename Identifier>
[[nodiscard]] std::array<char, 33> identifier_hex(const Identifier& identifier) {
  constexpr std::array<char, 16> kHex{'0', '1', '2', '3', '4', '5', '6', '7',
                                      '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  std::array<char, 33> result{};
  std::size_t output = 0U;
  for (const std::byte byte : identifier.bytes()) {
    const std::uint8_t value = std::to_integer<std::uint8_t>(byte);
    result[output] = kHex[value >> 4U];
    result[output + 1U] = kHex[value & 0x0fU];
    output += 2U;
  }
  return result;
}

[[nodiscard]] std::string_view storage_kind_name(const chronos::cseg::StorageKind kind) noexcept {
  using chronos::cseg::StorageKind;
  switch (kind) {
  case StorageKind::kUser:
    return "USER";
  case StorageKind::kWalId:
    return "WAL_ID";
  case StorageKind::kRecordSequence:
    return "RECORD_SEQUENCE";
  case StorageKind::kRowOrdinal:
    return "ROW_ORDINAL";
  case StorageKind::kOperation:
    return "OPERATION";
  case StorageKind::kTemporalOperation:
    return "TEMPORAL_OPERATION";
  case StorageKind::kLogicalIdentity:
    return "LOGICAL_IDENTITY";
  case StorageKind::kReceiveTime:
    return "RECEIVE_TIME";
  case StorageKind::kSystemCommitTime:
    return "SYSTEM_COMMIT_TIME";
  }
  return "INVALID";
}

[[nodiscard]] std::string_view
compression_name(const chronos::cseg::PageCompression compression) noexcept {
  using chronos::cseg::PageCompression;
  switch (compression) {
  case PageCompression::kNone:
    return "NONE";
  case PageCompression::kZstd:
    return "ZSTD";
  }
  return "INVALID";
}

template <typename Integer> void print_optional(const std::optional<Integer> value) {
  if (value.has_value()) {
    std::cout << *value;
  } else {
    std::cout << "none";
  }
}

void print_report(const chronos::cseg::CsegInspectionReport& report, const bool descriptors) {
  const auto part_id = identifier_hex(report.part_id);
  const auto table_id = identifier_hex(report.table_id);
  const auto tablet_id = identifier_hex(report.tablet_id);
  const auto schema_id = identifier_hex(report.schema_id);
  std::cout << "classification=VALID validation=STRUCTURAL_AND_SCHEMA_INDEPENDENT_SEMANTIC"
            << " part_id=" << part_id.data() << " table_id=" << table_id.data()
            << " tablet_id=" << tablet_id.data() << " schema_id=" << schema_id.data()
            << " schema_version=" << report.schema_version.value()
            << " total_length=" << report.total_length << " rows=" << report.row_count
            << " columns=" << report.columns.size() << " granules=" << report.granules.size()
            << " pages=" << report.pages.size()
            << " event_time_ordinal=" << report.event_time_column_ordinal
            << " ordering_columns=" << report.ordering_column_count
            << " event_time_min=" << report.minimum_event_time
            << " event_time_max=" << report.maximum_event_time << '\n';
  std::cout << "storage stored_page_bytes=" << report.stored_page_bytes
            << " uncompressed_page_bytes=" << report.uncompressed_page_bytes
            << " raw_pages=" << report.raw_page_count << " zstd_pages=" << report.zstd_page_count
            << '\n';
  if (!descriptors) {
    return;
  }
  for (std::size_t ordinal = 0U; ordinal < report.columns.size(); ++ordinal) {
    const chronos::cseg::CsegColumnDescriptor& column = report.columns[ordinal];
    std::cout << "column ordinal=" << ordinal
              << " storage=" << storage_kind_name(column.storage_kind)
              << " type=" << chronos::schema::logical_type_kind_name(column.logical_type.kind())
              << " type_parameter_0=" << column.logical_type.parameter_0()
              << " type_parameter_1=" << column.logical_type.parameter_1()
              << " nullable=" << column.nullable << " event_time=" << column.event_time
              << " schema_ordinal=";
    print_optional(column.schema_ordinal);
    std::cout << " ordering_ordinal=";
    print_optional(column.ordering_ordinal);
    if (column.column_id.has_value()) {
      const auto column_id = identifier_hex(*column.column_id);
      std::cout << " column_id=" << column_id.data();
    } else {
      std::cout << " column_id=none";
    }
    std::cout << '\n';
  }
  for (std::size_t ordinal = 0U; ordinal < report.granules.size(); ++ordinal) {
    const chronos::cseg::CsegGranuleDescriptor& granule = report.granules[ordinal];
    std::cout << "granule ordinal=" << ordinal << " first_row=" << granule.first_row
              << " rows=" << granule.row_count << " first_page=" << granule.first_page_index
              << " event_time_min=" << granule.minimum_event_time
              << " event_time_max=" << granule.maximum_event_time << '\n';
  }
  for (std::size_t ordinal = 0U; ordinal < report.pages.size(); ++ordinal) {
    const chronos::cseg::CsegPageDescriptor& page = report.pages[ordinal];
    std::cout << "page ordinal=" << ordinal << " granule=" << page.granule_ordinal
              << " column=" << page.stored_column_ordinal
              << " compression=" << compression_name(page.compression) << " rows=" << page.row_count
              << " nulls=" << page.null_count << " offset=" << page.page_offset
              << " stored_length=" << page.stored_length
              << " uncompressed_length=" << page.uncompressed_length
              << " validity_length=" << page.validity_length
              << " offsets_length=" << page.offsets_length
              << " values_length=" << page.values_length << " crc32c=" << page.page_crc32c << '\n';
  }
}

[[nodiscard]] int exit_code(const chronos::cseg::CsegInspectionErrorKind kind) noexcept {
  switch (kind) {
  case chronos::cseg::CsegInspectionErrorKind::kIncomplete:
    return 3;
  case chronos::cseg::CsegInspectionErrorKind::kUnsupported:
    return 4;
  case chronos::cseg::CsegInspectionErrorKind::kCorruption:
  case chronos::cseg::CsegInspectionErrorKind::kResourceLimit:
  case chronos::cseg::CsegInspectionErrorKind::kInvalidArgument:
    return 1;
  }
  return 1;
}

[[nodiscard]] int run(const std::span<const char* const> arguments) {
  const std::optional<Options> options = parse_options(arguments);
  if (!options.has_value()) {
    print_usage(!arguments.empty() ? std::string_view{arguments.front()}
                                   : std::string_view{"chronos-csegdump"});
    return 2;
  }
  const chronos::common::Result<std::vector<std::byte>> bytes = read_file(*options);
  if (!bytes.has_value()) {
    std::cerr << bytes.error().to_string() << '\n';
    return 1;
  }
  const chronos::cseg::CsegInspectionResult report = chronos::cseg::inspect_cseg_v1_part(
      *bytes, {.decode = {.max_file_length = options->maximum_input_bytes,
                          .max_metadata_length = options->maximum_input_bytes},
               .validation = {}});
  if (!report.has_value()) {
    std::cerr << report.error().status().to_string();
    if (report.error().kind() == chronos::cseg::CsegInspectionErrorKind::kIncomplete) {
      std::cerr << " required_size=" << report.error().required_size();
    }
    std::cerr << '\n';
    return exit_code(report.error().kind());
  }
  print_report(*report, options->descriptors);
  return 0;
}

} // namespace

int main(const int argc, const char* const argv[]) {
  try {
    return run({argv, static_cast<std::size_t>(argc)});
  } catch (const std::exception& error) {
    std::cerr << "INTERNAL: " << error.what() << '\n';
  } catch (...) {
    std::cerr << "INTERNAL: unknown exception\n";
  }
  return 1;
}
