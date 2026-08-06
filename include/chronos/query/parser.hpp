#ifndef CHRONOS_QUERY_PARSER_HPP_
#define CHRONOS_QUERY_PARSER_HPP_

#include "chronos/query/ast.hpp"
#include "chronos/query/lexer.hpp"

#include <cstddef>
#include <string_view>

namespace chronos::query {

struct SqlParserLimits {
  SqlLexerLimits lexer;
  std::size_t maximum_ast_nodes{262'144U};
  std::size_t maximum_expression_depth{128U};
  std::size_t maximum_list_elements{65'536U};
};

[[nodiscard]] SqlResult<ParsedSqlSelect> parse_sql_v1_select(std::string_view sql,
                                                             SqlParserLimits limits = {});

} // namespace chronos::query

#endif // CHRONOS_QUERY_PARSER_HPP_
