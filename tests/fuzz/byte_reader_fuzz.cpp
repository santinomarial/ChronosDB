#include "chronos/common/byte_reader.hpp"
#include "chronos/common/bytes.hpp"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <span>

namespace {

template <typename T>
void verify_cursor_transition(const chronos::common::Result<T>& result,
                              const chronos::common::ByteReader& reader,
                              const std::size_t offset_before, const std::size_t success_advance) {
  if (result.has_value() &&
      success_advance > std::numeric_limits<std::size_t>::max() - offset_before) {
    std::abort();
  }
  const std::size_t expected_offset =
      result.has_value() ? offset_before + success_advance : offset_before;
  if (reader.offset() != expected_offset) {
    std::abort();
  }
}

void verify_cursor_transition(const chronos::common::Status& status,
                              const chronos::common::ByteReader& reader,
                              const std::size_t offset_before, const std::size_t success_advance) {
  if (status.is_ok() && success_advance > std::numeric_limits<std::size_t>::max() - offset_before) {
    std::abort();
  }
  const std::size_t expected_offset =
      status.is_ok() ? offset_before + success_advance : offset_before;
  if (reader.offset() != expected_offset) {
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
  using chronos::common::byte_view;
  using chronos::common::ByteReader;

  ByteReader reader{byte_view(std::span<const std::uint8_t>{data, size})};
  while (!reader.empty()) {
    const auto operation = reader.read_u8();
    if (!operation.has_value()) {
      std::abort();
    }
    const std::size_t offset_before = reader.offset();

    switch (*operation & 0x0fU) {
    case 0:
      verify_cursor_transition(reader.read_u8(), reader, offset_before, sizeof(std::uint8_t));
      break;
    case 1:
      verify_cursor_transition(reader.read_u16_le(), reader, offset_before, sizeof(std::uint16_t));
      break;
    case 2:
      verify_cursor_transition(reader.read_u32_le(), reader, offset_before, sizeof(std::uint32_t));
      break;
    case 3:
      verify_cursor_transition(reader.read_u64_le(), reader, offset_before, sizeof(std::uint64_t));
      break;
    case 4:
      verify_cursor_transition(reader.read_i8(), reader, offset_before, sizeof(std::int8_t));
      break;
    case 5:
      verify_cursor_transition(reader.read_i16_le(), reader, offset_before, sizeof(std::int16_t));
      break;
    case 6:
      verify_cursor_transition(reader.read_i32_le(), reader, offset_before, sizeof(std::int32_t));
      break;
    case 7:
      verify_cursor_transition(reader.read_i64_le(), reader, offset_before, sizeof(std::int64_t));
      break;
    case 8:
      verify_cursor_transition(reader.read_float32_le(), reader, offset_before, sizeof(float));
      break;
    case 9:
      verify_cursor_transition(reader.read_float64_le(), reader, offset_before, sizeof(double));
      break;
    case 10:
      verify_cursor_transition(reader.read_exact(*operation), reader, offset_before, *operation);
      break;
    case 11:
      verify_cursor_transition(reader.peek_exact(*operation), reader, offset_before, 0U);
      break;
    case 12:
      verify_cursor_transition(reader.skip(*operation), reader, offset_before, *operation);
      break;
    case 13:
      verify_cursor_transition(reader.read_subreader(*operation), reader, offset_before,
                               *operation);
      break;
    case 14: {
      const auto declared_size = reader.read_u64_le();
      verify_cursor_transition(declared_size, reader, offset_before, sizeof(std::uint64_t));
      if (declared_size.has_value()) {
        const std::size_t payload_offset = reader.offset();
        const std::size_t payload_size = bounded_size(*declared_size);
        verify_cursor_transition(reader.read_exact(payload_size), reader, payload_offset,
                                 payload_size);
      }
      break;
    }
    case 15:
      verify_cursor_transition(reader.read_exact(std::numeric_limits<std::size_t>::max()), reader,
                               offset_before, std::numeric_limits<std::size_t>::max());
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
