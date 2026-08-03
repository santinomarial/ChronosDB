#ifndef CHRONOS_SCHEMA_UTF8_HPP_
#define CHRONOS_SCHEMA_UTF8_HPP_

#include <string_view>

namespace chronos::schema {

// Validates Unicode scalar-value UTF-8: overlong forms, surrogates, values above U+10FFFF,
// isolated continuation bytes, invalid lead bytes, and truncated sequences are rejected.
[[nodiscard]] bool is_valid_utf8(std::string_view value) noexcept;

} // namespace chronos::schema

#endif // CHRONOS_SCHEMA_UTF8_HPP_
