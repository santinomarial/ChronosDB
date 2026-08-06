#ifndef CHRONOS_QUERY_LITERAL_HPP_
#define CHRONOS_QUERY_LITERAL_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/uuid.hpp"

#include <cstdint>
#include <string_view>

namespace chronos::query {

// These functions parse the normalized literal payload retained by the SQL AST. Timestamp and
// interval values use exact nanoseconds; dates use signed days from the Unix epoch.
[[nodiscard]] common::Result<std::int64_t> parse_sql_integer_literal(std::string_view text);
[[nodiscard]] common::Result<double> parse_sql_float_literal(std::string_view text);
[[nodiscard]] common::Result<std::int64_t> parse_sql_timestamp_ns_literal(std::string_view text);
[[nodiscard]] common::Result<std::int32_t> parse_sql_date_literal(std::string_view text);
[[nodiscard]] common::Result<std::int64_t> parse_sql_interval_ns_literal(std::string_view text);
[[nodiscard]] common::Result<common::Uuid> parse_sql_uuid_literal(std::string_view text);

} // namespace chronos::query

#endif // CHRONOS_QUERY_LITERAL_HPP_
