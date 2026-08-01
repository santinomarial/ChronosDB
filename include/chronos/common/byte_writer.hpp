#ifndef CHRONOS_COMMON_BYTE_WRITER_HPP_
#define CHRONOS_COMMON_BYTE_WRITER_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/status.hpp"

#include <cstddef>
#include <cstdint>

namespace chronos::common {

// ByteWriter borrows its destination. The backing bytes must outlive the writer, and callers must
// provide exclusive access while a write is in progress. The class is not internally synchronized.
class ByteWriter {
public:
  explicit ByteWriter(MutableByteView bytes) noexcept;

  [[nodiscard]] std::size_t offset() const noexcept;
  [[nodiscard]] std::size_t remaining() const noexcept;
  [[nodiscard]] bool full() const noexcept;

  [[nodiscard]] Status write_u8(std::uint8_t value);
  [[nodiscard]] Status write_u16_le(std::uint16_t value);
  [[nodiscard]] Status write_u32_le(std::uint32_t value);
  [[nodiscard]] Status write_u64_le(std::uint64_t value);
  [[nodiscard]] Status write_i8(std::int8_t value);
  [[nodiscard]] Status write_i16_le(std::int16_t value);
  [[nodiscard]] Status write_i32_le(std::int32_t value);
  [[nodiscard]] Status write_i64_le(std::int64_t value);
  [[nodiscard]] Status write_float32_le(float value);
  [[nodiscard]] Status write_float64_le(double value);

  [[nodiscard]] Status write_exact(ByteView bytes);
  [[nodiscard]] Status zero_fill(std::size_t size);

private:
  [[nodiscard]] Status write_unsigned_le(std::uint64_t value, std::size_t width,
                                         const char* operation);
  [[nodiscard]] Status bounds_error(std::size_t requested, const char* operation) const;

  MutableByteView bytes_;
  std::size_t offset_{0};
};

} // namespace chronos::common

#endif // CHRONOS_COMMON_BYTE_WRITER_HPP_
