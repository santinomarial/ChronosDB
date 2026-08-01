#include "chronos/common/byte_reader.hpp"
#include "chronos/common/bytes.hpp"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <span>

namespace {

template <typename T> void verify_failure_is_atomic(const chronos::common::Result<T>& result,
                                                    const chronos::common::ByteReader& reader,
                                                    const std::size_t offset_before) {
  if (!result.has_value() && reader.offset() != offset_before) {
    std::abort();
  }
}

void verify_failure_is_atomic(const chronos::common::Status& status,
                              const chronos::common::ByteReader& reader,
                              const std::size_t offset_before) {
  if (!status.is_ok() && reader.offset() != offset_before) {
    std::abort();
  }
}

[[nodiscard]] std::size_t bounded_size(const std::uint64_t value) {
  if constexpr (sizeof(std::size_t) < sizeof(std::uint64_t)) {
    if (value > std::numeric_limits<std::size_t>::max()) {
      return std::numeric_limits<std::size_t>::max();
    }
  }
  return static_cast<std::size_t>(value);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  using chronos::common::ByteReader;
  using chronos::common::byte_view;

  ByteReader reader{byte_view(std::span<const std::uint8_t>{data, size})};
  while (!reader.empty()) {
    const auto operation = reader.read_u8();
    if (!operation.has_value()) {
      std::abort();
    }
    const std::size_t offset_before = reader.offset();

    switch (*operation & 0x0fU) {
    case 0:
      verify_failure_is_atomic(reader.read_u8(), reader, offset_before);
      break;
    case 1:
      verify_failure_is_atomic(reader.read_u16_le(), reader, offset_before);
      break;
    case 2:
      verify_failure_is_atomic(reader.read_u32_le(), reader, offset_before);
      break;
    case 3:
      verify_failure_is_atomic(reader.read_u64_le(), reader, offset_before);
      break;
    case 4:
      verify_failure_is_atomic(reader.read_i8(), reader, offset_before);
      break;
    case 5:
      verify_failure_is_atomic(reader.read_i16_le(), reader, offset_before);
      break;
    case 6:
      verify_failure_is_atomic(reader.read_i32_le(), reader, offset_before);
      break;
    case 7:
      verify_failure_is_atomic(reader.read_i64_le(), reader, offset_before);
      break;
    case 8:
      verify_failure_is_atomic(reader.read_float32_le(), reader, offset_before);
      break;
    case 9:
      verify_failure_is_atomic(reader.read_float64_le(), reader, offset_before);
      break;
    case 10:
      verify_failure_is_atomic(reader.read_exact(*operation), reader, offset_before);
      break;
    case 11:
      verify_failure_is_atomic(reader.peek_exact(*operation), reader, offset_before);
      break;
    case 12:
      verify_failure_is_atomic(reader.skip(*operation), reader, offset_before);
      break;
    case 13:
      verify_failure_is_atomic(reader.read_subreader(*operation), reader, offset_before);
      break;
    case 14: {
      const auto declared_size = reader.read_u64_le();
      verify_failure_is_atomic(declared_size, reader, offset_before);
      if (declared_size.has_value()) {
        const std::size_t payload_offset = reader.offset();
        verify_failure_is_atomic(reader.read_exact(bounded_size(*declared_size)), reader,
                                 payload_offset);
      }
      break;
    }
    case 15:
      verify_failure_is_atomic(reader.read_exact(std::numeric_limits<std::size_t>::max()), reader,
                               offset_before);
      break;
    default:
      std::abort();
    }

    if (reader.offset() > size || reader.remaining() != size - reader.offset()) {
      std::abort();
    }
  }
  return 0;
}
