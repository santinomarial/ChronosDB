#ifndef CHRONOS_SCHEMA_UTF8_HPP_
#define CHRONOS_SCHEMA_UTF8_HPP_

#include "chronos/common/bytes.hpp"

#include <string_view>

namespace chronos::schema {

// Validates Unicode scalar-value UTF-8: overlong forms, surrogates, values above U+10FFFF,
// isolated continuation bytes, invalid lead bytes, and truncated sequences are rejected.
[[nodiscard]] bool is_valid_utf8(std::string_view value) noexcept;
[[nodiscard]] bool is_valid_utf8(common::ByteView value) noexcept;

} // namespace chronos::schema

#endif // CHRONOS_SCHEMA_UTF8_HPP_
