#include "chronos/query/parser.hpp"

#include <cstddef>
#include <cstdint>
#include <string_view>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  // libFuzzer exposes bytes; string_view is the parser's non-owning byte interface.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  const std::string_view sql{reinterpret_cast<const char*>(data), size};
  constexpr chronos::query::SqlParserLimits kLimits{.lexer = {.maximum_input_bytes = 4096U,
                                                              .maximum_tokens = 2048U,
                                                              .maximum_token_bytes = 4096U},
                                                    .maximum_ast_nodes = 2048U,
                                                    .maximum_expression_depth = 64U,
                                                    .maximum_list_elements = 512U};
  const auto select = chronos::query::parse_sql_v1_select(sql, kLimits);
  const auto create = chronos::query::parse_sql_v1_create_table(sql, kLimits);
  const auto insert = chronos::query::parse_sql_v1_insert(sql, kLimits);
  static_cast<void>(select.has_value() ? select->items().size() : 0U);
  static_cast<void>(create.has_value() ? create->columns().size() : 0U);
  static_cast<void>(insert.has_value() ? insert->rows().size() : 0U);
  return 0;
}
