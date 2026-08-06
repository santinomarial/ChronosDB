#include "chronos/query/lexer.hpp"

#include <cstddef>
#include <cstdint>
#include <string_view>

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, const std::size_t size) {
  const std::string_view sql{reinterpret_cast<const char*>(data), size};
  const chronos::query::SqlResult<chronos::query::SqlTokenStream> result =
      chronos::query::tokenize_sql_v1(sql);
  if (result.has_value()) {
    for (const chronos::query::SqlToken& token : result->tokens()) {
      static_cast<void>(token.kind());
      static_cast<void>(token.text().size());
    }
  } else {
    static_cast<void>(result.error().status().code());
  }
  return 0;
}
