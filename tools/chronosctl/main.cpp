#include "chronos/common/version.hpp"

#include <iostream>
#include <string_view>

namespace {

void print_usage(const std::string_view program) {
  std::cerr << "Usage: " << program << " version [--json]\n";
}

} // namespace

int main(const int argc, const char* const argv[]) {
  if (argc == 2 && std::string_view{argv[1]} == "version") {
    std::cout << chronos::common::version_text() << '\n';
    return 0;
  }

  if (argc == 3 && std::string_view{argv[1]} == "version" &&
      std::string_view{argv[2]} == "--json") {
    std::cout << chronos::common::version_json() << '\n';
    return 0;
  }

  print_usage(argc > 0 ? std::string_view{argv[0]} : std::string_view{"chronosctl"});
  return 2;
}
