#include "chronos/common/byte_writer.hpp"

#include <algorithm>
#include <bit>
#include <cstring>
#include <string>
#include <utility>

namespace chronos::common {

ByteWriter::ByteWriter(const MutableByteView bytes) noexcept : bytes_(bytes) {}

std::size_t ByteWriter::offset() const noexcept {
  return offset_;
}

std::size_t ByteWriter::remaining() const noexcept {
  return bytes_.size() - offset_;
}

bool ByteWriter::full() const noexcept {
  return remaining() == 0;
}

Status ByteWriter::bounds_error(const std::size_t requested, const char* const operation) const {
  std::string message{"byte writer "};
  message.append(operation);
  message.append(" requires ");
  message.append(std::to_string(requested));
  message.append(" bytes at offset ");
  message.append(std::to_string(offset_));
  message.append(", but only ");
  message.append(std::to_string(remaining()));
  message.append(" remain");
  return Status{StatusCode::kOutOfRange, std::move(message)};
}

Status ByteWriter::write_unsigned_le(const std::uint64_t value, const FixedWidth fixed_width,
                                     const char* const operation) {
  const std::size_t width = static_cast<std::size_t>(fixed_width);
  if (width > remaining()) {
    return bounds_error(width, operation);
  }

  for (std::size_t index = 0; index < width; ++index) {
    bytes_[offset_ + index] = static_cast<std::byte>((value >> (index * 8U)) & 0xffU);
  }
  offset_ += width;
  return Status::ok();
}

Status ByteWriter::write_u8(const std::uint8_t value) {
  return write_unsigned_le(value, FixedWidth::k1, "write_u8");
}

Status ByteWriter::write_u16_le(const std::uint16_t value) {
  return write_unsigned_le(value, FixedWidth::k2, "write_u16_le");
}

Status ByteWriter::write_u32_le(const std::uint32_t value) {
  return write_unsigned_le(value, FixedWidth::k4, "write_u32_le");
}

Status ByteWriter::write_u64_le(const std::uint64_t value) {
  return write_unsigned_le(value, FixedWidth::k8, "write_u64_le");
}

Status ByteWriter::write_i8(const std::int8_t value) {
  return write_u8(std::bit_cast<std::uint8_t>(value));
}

Status ByteWriter::write_i16_le(const std::int16_t value) {
  return write_u16_le(std::bit_cast<std::uint16_t>(value));
}

Status ByteWriter::write_i32_le(const std::int32_t value) {
  return write_u32_le(std::bit_cast<std::uint32_t>(value));
}

Status ByteWriter::write_i64_le(const std::int64_t value) {
  return write_u64_le(std::bit_cast<std::uint64_t>(value));
}

Status ByteWriter::write_float32_le(const float value) {
  return write_u32_le(std::bit_cast<std::uint32_t>(value));
}

Status ByteWriter::write_float64_le(const double value) {
  return write_u64_le(std::bit_cast<std::uint64_t>(value));
}

Status ByteWriter::write_exact(const ByteView bytes) {
  if (bytes.size() > remaining()) {
    return bounds_error(bytes.size(), "write_exact");
  }
  if (!bytes.empty()) {
    std::memmove(bytes_.data() + offset_, bytes.data(), bytes.size());
  }
  offset_ += bytes.size();
  return Status::ok();
}

Status ByteWriter::zero_fill(const std::size_t size) {
  if (size > remaining()) {
    return bounds_error(size, "zero_fill");
  }
  if (size != 0) {
    std::fill_n(bytes_.data() + offset_, size, std::byte{0});
  }
  offset_ += size;
  return Status::ok();
}

} // namespace chronos::common
