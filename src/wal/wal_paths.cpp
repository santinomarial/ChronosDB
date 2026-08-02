#include "chronos/wal/wal_paths.hpp"

#include "chronos/common/status.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>

namespace chronos::wal {
namespace {

[[nodiscard]] common::Result<std::array<char, 20>>
segment_digits(const std::uint64_t segment_number) {
  if (segment_number == 0U) {
    return common::make_unexpected(
        common::Status{common::StatusCode::kInvalidArgument, "WAL segment number must be nonzero"});
  }

  std::array<char, 20> digits{};
  std::uint64_t remaining = segment_number;
  for (std::size_t index = digits.size(); index > 0U; --index) {
    digits[index - 1U] = static_cast<char>('0' + (remaining % 10U));
    remaining /= 10U;
  }
  return digits;
}

} // namespace

common::Result<std::string> wal_segment_file_name(const std::uint64_t segment_number) {
  const common::Result<std::array<char, 20>> digits = segment_digits(segment_number);
  if (!digits.has_value()) {
    return common::make_unexpected(digits.error());
  }

  std::string name{"wal-"};
  name.append(digits->data(), digits->size());
  name.append(".cwal");
  return name;
}

common::Result<std::string> wal_temporary_segment_file_name(const std::uint64_t segment_number,
                                                            const WalId& nonce) {
  if (!nonce.is_valid()) {
    return common::make_unexpected(common::Status{common::StatusCode::kInvalidArgument,
                                                  "WAL temporary-name nonce must be nonzero"});
  }
  const common::Result<std::string> final_name = wal_segment_file_name(segment_number);
  if (!final_name.has_value()) {
    return common::make_unexpected(final_name.error());
  }

  constexpr std::array<char, 16> kHexDigits{'0', '1', '2', '3', '4', '5', '6', '7',
                                            '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  std::string name{"."};
  name.append(*final_name);
  name.append(".tmp-");
  name.reserve(name.size() + (nonce.bytes.size() * 2U));
  for (const std::byte byte : nonce.bytes) {
    const std::uint8_t value = std::to_integer<std::uint8_t>(byte);
    name.push_back(kHexDigits[value >> 4U]);
    name.push_back(kHexDigits[value & 0x0fU]);
  }
  return name;
}

} // namespace chronos::wal
