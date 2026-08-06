#include "chronos/query/parser.hpp"

#include <cstddef>
#include <cstdint>
#include <string_view>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  const std::string_view sql{reinterpret_cast<const char*>(data), size};
  const chronos::query::SqlResult<chronos::query::ParsedSqlSelect> parsed =
      chronos::query::parse_sql_v1_select(sql, {.lexer = {.maximum_input_bytes = 4096U,
                                                          .maximum_tokens = 2048U,
                                                          .maximum_token_bytes = 4096U},
                                                .maximum_ast_nodes = 2048U,
                                                .maximum_expression_depth = 64U,
                                                .maximum_list_elements = 512U});
  if (parsed.has_value()) {
    static_cast<void>(parsed->items().size());
    static_cast<void>(parsed->asof_joins().size());
  } else {
    static_cast<void>(parsed.error().code());
  }
  return 0;
}
