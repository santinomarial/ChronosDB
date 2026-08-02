#include "chronos/wal/wal_recovery.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
#include <string_view>

namespace {

class DumpSink final : public chronos::wal::WalReplaySink {
public:
  chronos::common::Status preflight(const chronos::wal::WalReplayRecord&) override {
    return chronos::common::Status::ok();
  }

  chronos::common::Status replay(const chronos::wal::WalReplayRecord& record) override {
    std::cout << "record sequence=" << record.header.record_sequence
              << " segment=" << record.record_start.segment_number
              << " offset=" << record.record_start.byte_offset
              << " total_length=" << record.header.total_length
              << " format=" << record.header.record_format << " type=" << record.header.record_type
              << " payload_length=" << record.header.payload_length << '\n';
    return chronos::common::Status::ok();
  }
};

[[nodiscard]] std::string wal_id_hex(const chronos::wal::WalId& id) {
  constexpr std::array<char, 16> kHex{'0', '1', '2', '3', '4', '5', '6', '7',
                                      '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  std::string result;
  result.reserve(id.bytes.size() * 2U);
  for (const std::byte byte : id.bytes) {
    const std::uint8_t value = std::to_integer<std::uint8_t>(byte);
    result.push_back(kHex[value >> 4U]);
    result.push_back(kHex[value & 0x0fU]);
  }
  return result;
}

void print_usage(const std::string_view program) {
  std::cerr << "Usage: " << program << " <wal-directory>\n";
}

} // namespace

int main(const int argc, const char* const argv[]) {
  if (argc != 2) {
    print_usage(argc > 0 ? std::string_view{argv[0]} : std::string_view{"chronos-waldump"});
    return 2;
  }

  DumpSink sink;
  const chronos::common::Result<chronos::wal::WalRecoveryReport> report =
      chronos::wal::inspect_wal(argv[1], sink);
  if (!report.has_value()) {
    std::cerr << report.error().to_string() << '\n';
    return 1;
  }
  const bool incomplete =
      report->classification == chronos::wal::WalScanClassification::kIncompleteFinalTail;
  std::cout << "classification=" << (incomplete ? "INCOMPLETE_FINAL_TAIL" : "CLEAN")
            << " wal_id=" << wal_id_hex(report->wal_id) << " segments=" << report->segment_count
            << " temporary_files=" << report->temporary_file_count
            << " records=" << report->record_count << " bytes=" << report->physical_bytes
            << " valid_end_segment=" << report->valid_end.segment_number
            << " valid_end_offset=" << report->valid_end.byte_offset << '\n';
  return incomplete ? 3 : 0;
}
