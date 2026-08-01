#ifndef CHRONOS_COMMON_BYTE_READER_HPP_
#define CHRONOS_COMMON_BYTE_READER_HPP_

#include "chronos/common/bytes.hpp"
#include "chronos/common/result.hpp"

#include <cstddef>
#include <cstdint>

namespace chronos::common {

// ByteReader borrows its input. The backing bytes must outlive the reader and every ByteView or
// sub-reader returned from it. The class is not internally synchronized.
class ByteReader {
public:
  explicit ByteReader(ByteView bytes) noexcept;

  [[nodiscard]] std::size_t offset() const noexcept;
  [[nodiscard]] std::size_t remaining() const noexcept;
  [[nodiscard]] bool empty() const noexcept;

  [[nodiscard]] Result<std::uint8_t> read_u8();
  [[nodiscard]] Result<std::uint16_t> read_u16_le();
  [[nodiscard]] Result<std::uint32_t> read_u32_le();
  [[nodiscard]] Result<std::uint64_t> read_u64_le();
  [[nodiscard]] Result<std::int8_t> read_i8();
  [[nodiscard]] Result<std::int16_t> read_i16_le();
  [[nodiscard]] Result<std::int32_t> read_i32_le();
  [[nodiscard]] Result<std::int64_t> read_i64_le();
  [[nodiscard]] Result<float> read_float32_le();
  [[nodiscard]] Result<double> read_float64_le();

  [[nodiscard]] Result<ByteView> read_exact(std::size_t size);
  [[nodiscard]] Result<ByteView> peek_exact(std::size_t size) const;
  [[nodiscard]] Status skip(std::size_t size);
  [[nodiscard]] Result<ByteReader> read_subreader(std::size_t size);

private:
  [[nodiscard]] Result<std::uint64_t> read_unsigned_le(std::size_t width, const char* operation);
  [[nodiscard]] Status bounds_error(std::size_t requested, const char* operation) const;

  ByteView bytes_;
  std::size_t offset_{0};
};

} // namespace chronos::common

#endif // CHRONOS_COMMON_BYTE_READER_HPP_
