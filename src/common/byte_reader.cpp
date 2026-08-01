#include "chronos/common/byte_reader.hpp"

#include <bit>
#include <string>
#include <utility>

namespace chronos::common {

ByteReader::ByteReader(const ByteView bytes) noexcept : bytes_(bytes) {}

std::size_t ByteReader::offset() const noexcept {
  return offset_;
}

std::size_t ByteReader::remaining() const noexcept {
  return bytes_.size() - offset_;
}

bool ByteReader::empty() const noexcept {
  return remaining() == 0;
}

Status ByteReader::bounds_error(const std::size_t requested, const char* const operation) const {
  std::string message{"byte reader "};
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

Result<std::uint64_t> ByteReader::read_unsigned_le(const std::size_t width,
                                                   const char* const operation) {
  if (width > remaining()) {
    return make_unexpected(bounds_error(width, operation));
  }

  std::uint64_t value = 0;
  for (std::size_t index = 0; index < width; ++index) {
    const auto byte = std::to_integer<std::uint64_t>(bytes_[offset_ + index]);
    value |= byte << (index * 8U);
  }
  offset_ += width;
  return value;
}

Result<std::uint8_t> ByteReader::read_u8() {
  const Result<std::uint64_t> value = read_unsigned_le(sizeof(std::uint8_t), "read_u8");
  if (!value) {
    return make_unexpected(value.error());
  }
  return static_cast<std::uint8_t>(*value);
}

Result<std::uint16_t> ByteReader::read_u16_le() {
  const Result<std::uint64_t> value = read_unsigned_le(sizeof(std::uint16_t), "read_u16_le");
  if (!value) {
    return make_unexpected(value.error());
  }
  return static_cast<std::uint16_t>(*value);
}

Result<std::uint32_t> ByteReader::read_u32_le() {
  const Result<std::uint64_t> value = read_unsigned_le(sizeof(std::uint32_t), "read_u32_le");
  if (!value) {
    return make_unexpected(value.error());
  }
  return static_cast<std::uint32_t>(*value);
}

Result<std::uint64_t> ByteReader::read_u64_le() {
  return read_unsigned_le(sizeof(std::uint64_t), "read_u64_le");
}

Result<std::int8_t> ByteReader::read_i8() {
  const Result<std::uint8_t> value = read_u8();
  if (!value) {
    return make_unexpected(value.error());
  }
  return std::bit_cast<std::int8_t>(*value);
}

Result<std::int16_t> ByteReader::read_i16_le() {
  const Result<std::uint16_t> value = read_u16_le();
  if (!value) {
    return make_unexpected(value.error());
  }
  return std::bit_cast<std::int16_t>(*value);
}

Result<std::int32_t> ByteReader::read_i32_le() {
  const Result<std::uint32_t> value = read_u32_le();
  if (!value) {
    return make_unexpected(value.error());
  }
  return std::bit_cast<std::int32_t>(*value);
}

Result<std::int64_t> ByteReader::read_i64_le() {
  const Result<std::uint64_t> value = read_u64_le();
  if (!value) {
    return make_unexpected(value.error());
  }
  return std::bit_cast<std::int64_t>(*value);
}

Result<float> ByteReader::read_float32_le() {
  const Result<std::uint32_t> bits = read_u32_le();
  if (!bits) {
    return make_unexpected(bits.error());
  }
  return std::bit_cast<float>(*bits);
}

Result<double> ByteReader::read_float64_le() {
  const Result<std::uint64_t> bits = read_u64_le();
  if (!bits) {
    return make_unexpected(bits.error());
  }
  return std::bit_cast<double>(*bits);
}

Result<ByteView> ByteReader::read_exact(const std::size_t size) {
  const Result<ByteView> bytes = peek_exact(size);
  if (!bytes) {
    return make_unexpected(bytes.error());
  }
  offset_ += size;
  return *bytes;
}

Result<ByteView> ByteReader::peek_exact(const std::size_t size) const {
  if (size > remaining()) {
    return make_unexpected(bounds_error(size, "peek_exact"));
  }
  return bytes_.subspan(offset_, size);
}

Status ByteReader::skip(const std::size_t size) {
  if (size > remaining()) {
    return bounds_error(size, "skip");
  }
  offset_ += size;
  return Status::ok();
}

Result<ByteReader> ByteReader::read_subreader(const std::size_t size) {
  const Result<ByteView> bytes = read_exact(size);
  if (!bytes) {
    return make_unexpected(bytes.error());
  }
  return ByteReader{*bytes};
}

} // namespace chronos::common
