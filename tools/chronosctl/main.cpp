#include "chronos/common/byte_reader.hpp"
#include "chronos/common/status.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/common/version.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/ingest/columnar_append_format.hpp"
#include "chronos/network/client_session.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/network/native_query_tcp_execution.hpp"
#include "chronos/network/native_quorum_ingest_tcp_execution.hpp"
#include "chronos/service/native_client_tls_route_owner.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <limits>
#include <netinet/in.h>
#include <new>
#include <optional>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <system_error>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

namespace {

constexpr std::uint64_t kMaximumTimeoutMilliseconds = 3'600'000U;
constexpr std::size_t kMaximumRedirects = 8U;
constexpr std::chrono::milliseconds kMaximumPollWait{100};
constexpr int kSqlSocketTimeoutSeconds = 5;

struct SqlOptions {
  std::string host;
  std::uint16_t port{};
  std::string execute;
};

struct SqlParseResult {
  std::optional<SqlOptions> options{std::nullopt};
  std::string error;
  bool help{};
};

struct RoutedSqlOptions {
  chronos::common::Uuid group_id;
  std::uint64_t initial_node_id{};
  std::uint64_t minimum_placement_epoch{};
  std::string routes_file;
  std::string tls_certificate_file;
  std::string tls_private_key_file;
  std::string tls_trust_store_file;
  std::string execute;
  std::chrono::milliseconds timeout{};
};

struct RoutedSqlParseResult {
  std::optional<RoutedSqlOptions> options{std::nullopt};
  std::string error;
  bool help{};
};

struct QuorumSyncOptions {
  chronos::common::Uuid group_id;
  std::uint64_t initial_node_id{};
  std::uint64_t minimum_placement_epoch{};
  std::string routes_file;
  std::string tls_certificate_file;
  std::string tls_private_key_file;
  std::string tls_trust_store_file;
  std::string append_file;
  std::chrono::milliseconds timeout{};
  bool json{};
};

struct ParseResult {
  std::optional<QuorumSyncOptions> options{std::nullopt};
  std::string error;
  bool help{};
};

class Descriptor {
public:
  explicit Descriptor(const int descriptor) noexcept : descriptor_(descriptor) {}
  ~Descriptor() {
    if (descriptor_ >= 0) {
      static_cast<void>(::close(descriptor_));
    }
  }
  Descriptor(const Descriptor&) = delete;
  Descriptor& operator=(const Descriptor&) = delete;

  [[nodiscard]] int get() const noexcept {
    return descriptor_;
  }

  [[nodiscard]] int release() noexcept {
    const int descriptor = descriptor_;
    descriptor_ = -1;
    return descriptor;
  }

private:
  int descriptor_;
};

[[nodiscard]] chronos::common::Status invalid(const std::string_view message) {
  return {chronos::common::StatusCode::kInvalidArgument, std::string{message}};
}

[[nodiscard]] chronos::common::Status io_error(const std::string_view operation,
                                               const int error = errno) {
  return {chronos::common::StatusCode::kIoError,
          std::string{operation} + ": " +
              std::error_code{error, std::generic_category()}.message()};
}

void print_usage(const std::string_view program, std::ostream& output) {
  output << "Usage:\n"
         << "  " << program << " version [--json]\n"
         << "  " << program << " sql --host 127.0.0.1 --port PORT --execute \"SQL\"\n"
         << "  " << program << " routed-sql --group UUID --initial-node NODE_ID\\\n"
         << "      --minimum-placement-epoch EPOCH --routes FILE --tls-cert FILE\\\n"
         << "      --tls-key FILE --tls-ca FILE --execute \"SQL\" --timeout-ms MILLISECONDS\n"
         << "  " << program << " quorum-sync [--json] --group UUID --initial-node NODE_ID\\\n"
         << "      --minimum-placement-epoch EPOCH --routes FILE --tls-cert FILE\\\n"
         << "      --tls-key FILE --tls-ca FILE --append-file FILE --timeout-ms MILLISECONDS\n";
}

void print_routed_sql_usage(const std::string_view program, std::ostream& output) {
  output
      << "Usage: " << program << " routed-sql --group UUID --initial-node NODE_ID\\\n"
      << "    --minimum-placement-epoch EPOCH --routes FILE --tls-cert FILE\\\n"
      << "    --tls-key FILE --tls-ca FILE --execute \"SQL\" --timeout-ms MILLISECONDS\n"
      << "\n"
      << "Executes one exact finite SQL query through an authenticated single-group route.\n"
      << "Results are printed only after QUERY_END as tab-separated column names and rows.\n"
      << "UUID must use lowercase 8-4-4-4-12 form; positive integers must be canonical decimal.\n"
      << "The timeout range is 1 through " << kMaximumTimeoutMilliseconds << " milliseconds.\n";
}

void print_sql_usage(const std::string_view program, std::ostream& output) {
  output << "Usage: " << program << " sql --host 127.0.0.1 --port PORT --execute \"SQL\"\n"
         << "\n"
         << "Executes one SQL statement through the plaintext loopback native-protocol server.\n"
         << "Results are printed as tab-separated column names and rows.\n";
}

void print_quorum_sync_usage(const std::string_view program, std::ostream& output) {
  output
      << "Usage: " << program << " quorum-sync [--json] --group UUID --initial-node NODE_ID\\\n"
      << "    --minimum-placement-epoch EPOCH --routes FILE --tls-cert FILE\\\n"
      << "    --tls-key FILE --tls-ca FILE --append-file FILE --timeout-ms MILLISECONDS\n"
      << "\n"
      << "Sends one exact canonical Columnar Append v1 payload with QUORUM_SYNC durability.\n"
      << "UUID must use lowercase 8-4-4-4-12 form; positive integers must be canonical decimal.\n"
      << "The timeout range is 1 through " << kMaximumTimeoutMilliseconds << " milliseconds.\n";
}

[[nodiscard]] std::optional<std::uint8_t> parse_hex_digit(const char value) noexcept {
  if (value >= '0' && value <= '9') {
    return static_cast<std::uint8_t>(value - '0');
  }
  if (value >= 'a' && value <= 'f') {
    return static_cast<std::uint8_t>(value - 'a' + 10);
  }
  return std::nullopt;
}

[[nodiscard]] std::optional<chronos::common::Uuid>
parse_uuid(const std::string_view text) noexcept {
  if (text.size() != 36U || text[8U] != '-' || text[13U] != '-' || text[18U] != '-' ||
      text[23U] != '-') {
    return std::nullopt;
  }
  chronos::common::Uuid::Bytes bytes{};
  std::size_t output = 0U;
  for (std::size_t input = 0U; input < text.size();) {
    if (input == 8U || input == 13U || input == 18U || input == 23U) {
      ++input;
      continue;
    }
    const auto high = parse_hex_digit(text[input]);
    const auto low = parse_hex_digit(text[input + 1U]);
    if (!high.has_value() || !low.has_value()) {
      return std::nullopt;
    }
    bytes[output++] = static_cast<std::byte>((*high << 4U) | *low);
    input += 2U;
  }
  chronos::common::Uuid uuid{bytes};
  return uuid.is_nil() ? std::nullopt : std::optional<chronos::common::Uuid>{uuid};
}

[[nodiscard]] bool parse_positive_decimal(const std::string_view text,
                                          std::uint64_t& output) noexcept {
  if (text.empty() || (text.size() > 1U && text.front() == '0')) {
    return false;
  }
  const auto parsed = std::from_chars(text.data(), text.data() + text.size(), output);
  return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size() && output != 0U;
}

[[nodiscard]] SqlParseResult parse_sql_options(const std::span<const char* const> arguments) {
  SqlOptions options;
  bool host_seen{};
  bool port_seen{};
  bool execute_seen{};

  auto value = [&arguments](std::size_t& index) -> std::optional<std::string_view> {
    if (++index >= arguments.size()) {
      return std::nullopt;
    }
    return std::string_view{arguments[index]};
  };

  for (std::size_t index = 2U; index < arguments.size(); ++index) {
    const std::string_view argument{arguments[index]};
    if (argument == "--help") {
      if (arguments.size() != 3U) {
        return {.error = "--help cannot be combined with sql options"};
      }
      return {.error = {}, .help = true};
    }
    if (argument == "--host") {
      if (host_seen) {
        return {.error = "--host was specified more than once"};
      }
      const auto parsed = value(index);
      if (!parsed.has_value() || *parsed != "127.0.0.1") {
        return {.error = "--host must be the plaintext loopback address 127.0.0.1"};
      }
      options.host = *parsed;
      host_seen = true;
      continue;
    }
    if (argument == "--port") {
      if (port_seen) {
        return {.error = "--port was specified more than once"};
      }
      const auto text = value(index);
      std::uint64_t parsed{};
      if (!text.has_value() || !parse_positive_decimal(*text, parsed) || parsed > 65'535U) {
        return {.error = "--port requires a canonical decimal in the range 1..65535"};
      }
      options.port = static_cast<std::uint16_t>(parsed);
      port_seen = true;
      continue;
    }
    if (argument == "--execute") {
      if (execute_seen) {
        return {.error = "--execute was specified more than once"};
      }
      const auto parsed = value(index);
      if (!parsed.has_value() || parsed->empty()) {
        return {.error = "--execute requires a nonempty SQL statement"};
      }
      options.execute = *parsed;
      execute_seen = true;
      continue;
    }
    return {.error = "unknown sql option: " + std::string{argument}};
  }

  if (!host_seen || !port_seen || !execute_seen) {
    return {.error = "sql requires --host, --port, and --execute"};
  }
  return {.options = std::move(options), .error = {}};
}

[[nodiscard]] RoutedSqlParseResult
parse_routed_sql_options(const std::span<const char* const> arguments) {
  RoutedSqlOptions options;
  bool group_seen{};
  bool initial_node_seen{};
  bool placement_epoch_seen{};
  bool routes_seen{};
  bool certificate_seen{};
  bool private_key_seen{};
  bool trust_store_seen{};
  bool execute_seen{};
  bool timeout_seen{};

  auto value = [&arguments](std::size_t& index) -> std::optional<std::string_view> {
    if (++index >= arguments.size()) {
      return std::nullopt;
    }
    return std::string_view{arguments[index]};
  };
  auto assign_path = [&value](const std::string_view name, bool* const seen, std::size_t& index,
                              std::string& destination) -> std::string {
    if (*seen) {
      return std::string{name} + " was specified more than once";
    }
    const auto parsed = value(index);
    if (!parsed.has_value() || parsed->empty()) {
      return std::string{name} + " requires a nonempty path";
    }
    destination = *parsed;
    *seen = true;
    return {};
  };

  for (std::size_t index = 2U; index < arguments.size(); ++index) {
    const std::string_view argument{arguments[index]};
    if (argument == "--help") {
      if (arguments.size() != 3U) {
        return {.error = "--help cannot be combined with routed-sql options"};
      }
      return {.error = {}, .help = true};
    }
    if (argument == "--group") {
      if (group_seen) {
        return {.error = "--group was specified more than once"};
      }
      const auto text = value(index);
      const auto parsed = text.has_value() ? parse_uuid(*text) : std::nullopt;
      if (!parsed.has_value()) {
        return {.error = "--group requires a non-nil lowercase canonical UUID"};
      }
      options.group_id = *parsed;
      group_seen = true;
      continue;
    }
    if (argument == "--initial-node" || argument == "--minimum-placement-epoch" ||
        argument == "--timeout-ms") {
      bool* seen =
          argument == "--initial-node"
              ? &initial_node_seen
              : (argument == "--minimum-placement-epoch" ? &placement_epoch_seen : &timeout_seen);
      if (*seen) {
        return {.error = std::string{argument} + " was specified more than once"};
      }
      const auto text = value(index);
      std::uint64_t parsed{};
      if (!text.has_value() || !parse_positive_decimal(*text, parsed)) {
        return {.error = std::string{argument} + " requires a positive canonical decimal"};
      }
      if (argument == "--initial-node") {
        options.initial_node_id = parsed;
      } else if (argument == "--minimum-placement-epoch") {
        options.minimum_placement_epoch = parsed;
      } else {
        if (parsed > kMaximumTimeoutMilliseconds) {
          return {.error = "--timeout-ms exceeds the one-hour command limit"};
        }
        options.timeout = std::chrono::milliseconds{parsed};
      }
      *seen = true;
      continue;
    }
    if (argument == "--execute") {
      if (execute_seen) {
        return {.error = "--execute was specified more than once"};
      }
      const auto parsed = value(index);
      if (!parsed.has_value() || parsed->empty()) {
        return {.error = "--execute requires a nonempty SQL statement"};
      }
      options.execute = *parsed;
      execute_seen = true;
      continue;
    }

    std::string path_error;
    if (argument == "--routes") {
      path_error = assign_path(argument, &routes_seen, index, options.routes_file);
    } else if (argument == "--tls-cert") {
      path_error = assign_path(argument, &certificate_seen, index, options.tls_certificate_file);
    } else if (argument == "--tls-key") {
      path_error = assign_path(argument, &private_key_seen, index, options.tls_private_key_file);
    } else if (argument == "--tls-ca") {
      path_error = assign_path(argument, &trust_store_seen, index, options.tls_trust_store_file);
    } else {
      return {.error = "unknown routed-sql option: " + std::string{argument}};
    }
    if (!path_error.empty()) {
      return {.error = std::move(path_error)};
    }
  }

  if (!group_seen || !initial_node_seen || !placement_epoch_seen || !routes_seen ||
      !certificate_seen || !private_key_seen || !trust_store_seen || !execute_seen ||
      !timeout_seen) {
    return {.error = "routed-sql requires every group, route, TLS, query, and timeout option"};
  }
  return {.options = std::move(options), .error = {}};
}

[[nodiscard]] ParseResult parse_quorum_sync_options(const std::span<const char* const> arguments) {
  QuorumSyncOptions options;
  bool group_seen{};
  bool initial_node_seen{};
  bool placement_epoch_seen{};
  bool routes_seen{};
  bool certificate_seen{};
  bool private_key_seen{};
  bool trust_store_seen{};
  bool append_seen{};
  bool timeout_seen{};
  bool json_seen{};

  auto value = [&arguments](std::size_t& index) -> std::optional<std::string_view> {
    if (++index >= arguments.size()) {
      return std::nullopt;
    }
    return std::string_view{arguments[index]};
  };
  auto assign_path = [&value](const std::string_view name, bool* const seen, std::size_t& index,
                              std::string& destination) -> std::string {
    if (*seen) {
      return std::string{name} + " was specified more than once";
    }
    const auto parsed = value(index);
    if (!parsed.has_value() || parsed->empty()) {
      return std::string{name} + " requires a nonempty path";
    }
    destination = *parsed;
    *seen = true;
    return {};
  };

  for (std::size_t index = 2U; index < arguments.size(); ++index) {
    const std::string_view argument{arguments[index]};
    if (argument == "--help") {
      if (arguments.size() != 3U) {
        return {.error = "--help cannot be combined with quorum-sync options"};
      }
      return {.error = {}, .help = true};
    }
    if (argument == "--json") {
      if (json_seen) {
        return {.error = "--json was specified more than once"};
      }
      options.json = true;
      json_seen = true;
      continue;
    }
    if (argument == "--group") {
      if (group_seen) {
        return {.error = "--group was specified more than once"};
      }
      const auto text = value(index);
      const auto parsed = text.has_value() ? parse_uuid(*text) : std::nullopt;
      if (!parsed.has_value()) {
        return {.error = "--group requires a non-nil lowercase canonical UUID"};
      }
      options.group_id = *parsed;
      group_seen = true;
      continue;
    }
    if (argument == "--initial-node" || argument == "--minimum-placement-epoch" ||
        argument == "--timeout-ms") {
      bool* seen =
          argument == "--initial-node"
              ? &initial_node_seen
              : (argument == "--minimum-placement-epoch" ? &placement_epoch_seen : &timeout_seen);
      if (*seen) {
        return {.error = std::string{argument} + " was specified more than once"};
      }
      const auto text = value(index);
      std::uint64_t parsed{};
      if (!text.has_value() || !parse_positive_decimal(*text, parsed)) {
        return {.error = std::string{argument} + " requires a positive canonical decimal"};
      }
      if (argument == "--initial-node") {
        options.initial_node_id = parsed;
      } else if (argument == "--minimum-placement-epoch") {
        options.minimum_placement_epoch = parsed;
      } else {
        if (parsed > kMaximumTimeoutMilliseconds) {
          return {.error = "--timeout-ms exceeds the one-hour command limit"};
        }
        options.timeout = std::chrono::milliseconds{parsed};
      }
      *seen = true;
      continue;
    }

    std::string path_error;
    if (argument == "--routes") {
      path_error = assign_path(argument, &routes_seen, index, options.routes_file);
    } else if (argument == "--tls-cert") {
      path_error = assign_path(argument, &certificate_seen, index, options.tls_certificate_file);
    } else if (argument == "--tls-key") {
      path_error = assign_path(argument, &private_key_seen, index, options.tls_private_key_file);
    } else if (argument == "--tls-ca") {
      path_error = assign_path(argument, &trust_store_seen, index, options.tls_trust_store_file);
    } else if (argument == "--append-file") {
      path_error = assign_path(argument, &append_seen, index, options.append_file);
    } else {
      return {.error = "unknown quorum-sync option: " + std::string{argument}};
    }
    if (!path_error.empty()) {
      return {.error = std::move(path_error)};
    }
  }

  if (!group_seen || !initial_node_seen || !placement_epoch_seen || !routes_seen ||
      !certificate_seen || !private_key_seen || !trust_store_seen || !append_seen ||
      !timeout_seen) {
    return {.error = "quorum-sync requires every group, route, TLS, append, and timeout option"};
  }
  return {.options = std::move(options), .error = {}};
}

[[nodiscard]] chronos::common::Result<std::vector<std::byte>>
read_append_file(const std::string& path) {
  const int raw_descriptor = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
  if (raw_descriptor < 0) {
    return chronos::common::make_unexpected(io_error("opening Columnar Append input"));
  }
  Descriptor descriptor{raw_descriptor};
  struct stat metadata {};
  if (::fstat(descriptor.get(), &metadata) != 0) {
    return chronos::common::make_unexpected(io_error("inspecting Columnar Append input"));
  }
  constexpr std::size_t kMaximumBytes =
      chronos::ingest::columnar_append_v1::kMaximumApplicationPayloadLength;
  if (!S_ISREG(metadata.st_mode)) {
    return chronos::common::make_unexpected(invalid("Columnar Append input is not a regular file"));
  }
  if (metadata.st_size <= 0 ||
      static_cast<std::uintmax_t>(metadata.st_size) > static_cast<std::uintmax_t>(kMaximumBytes)) {
    return chronos::common::make_unexpected(
        invalid("Columnar Append input size is outside the canonical v1 limit"));
  }

  try {
    std::vector<std::byte> bytes(static_cast<std::size_t>(metadata.st_size));
    std::size_t offset = 0U;
    while (offset < bytes.size()) {
      const ssize_t read = ::read(descriptor.get(), bytes.data() + offset, bytes.size() - offset);
      if (read < 0) {
        if (errno == EINTR) {
          continue;
        }
        return chronos::common::make_unexpected(io_error("reading Columnar Append input"));
      }
      if (read == 0) {
        return chronos::common::make_unexpected(
            io_error("Columnar Append input changed during the exact read", EIO));
      }
      offset += static_cast<std::size_t>(read);
    }
    std::byte trailing{};
    ssize_t trailing_read{};
    do {
      trailing_read = ::read(descriptor.get(), &trailing, 1U);
    } while (trailing_read < 0 && errno == EINTR);
    if (trailing_read < 0) {
      return chronos::common::make_unexpected(io_error("checking Columnar Append input length"));
    }
    if (trailing_read != 0) {
      return chronos::common::make_unexpected(
          io_error("Columnar Append input grew during the exact read", EIO));
    }
    return bytes;
  } catch (const std::bad_alloc&) {
    return chronos::common::make_unexpected(
        chronos::common::Status{chronos::common::StatusCode::kResourceExhausted,
                                "allocating the bounded Columnar Append input failed"});
  }
}

[[nodiscard]] std::array<char, 37> format_uuid(const chronos::common::Uuid& uuid) noexcept {
  constexpr std::array<char, 16> kHex{'0', '1', '2', '3', '4', '5', '6', '7',
                                      '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  std::array<char, 37> output{};
  std::size_t input = 0U;
  for (std::size_t index = 0U; index < 36U; ++index) {
    if (index == 8U || index == 13U || index == 18U || index == 23U) {
      output[index] = '-';
      continue;
    }
    const std::uint8_t byte = std::to_integer<std::uint8_t>(uuid.bytes()[input / 2U]);
    output[index] = (input % 2U == 0U) ? kHex[byte >> 4U] : kHex[byte & 0x0fU];
    ++input;
  }
  return output;
}

[[nodiscard]] std::string escaped_text(const std::string_view value) {
  constexpr std::array<char, 16> kHex{'0', '1', '2', '3', '4', '5', '6', '7',
                                      '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  std::string output;
  output.reserve(value.size());
  for (const char character : value) {
    const auto byte = static_cast<unsigned char>(character);
    switch (byte) {
    case '\\':
      output.append("\\\\");
      break;
    case '\t':
      output.append("\\t");
      break;
    case '\n':
      output.append("\\n");
      break;
    case '\r':
      output.append("\\r");
      break;
    default:
      if (byte < 0x20U || byte == 0x7fU) {
        output.append("\\x");
        output.push_back(kHex[byte >> 4U]);
        output.push_back(kHex[byte & 0x0fU]);
      } else {
        output.push_back(static_cast<char>(byte));
      }
      break;
    }
  }
  return output;
}

template <typename Value>
[[nodiscard]] chronos::common::Result<std::string>
format_number(const chronos::common::Result<Value>& value) {
  if (!value.has_value()) {
    return chronos::common::make_unexpected(value.error());
  }
  std::array<char, 128U> buffer{};
  std::to_chars_result formatted{};
  if constexpr (std::is_floating_point_v<Value>) {
    formatted = std::to_chars(buffer.data(), buffer.data() + buffer.size(), *value,
                              std::chars_format::general, std::numeric_limits<Value>::max_digits10);
  } else {
    formatted = std::to_chars(buffer.data(), buffer.data() + buffer.size(), *value);
  }
  if (formatted.ec != std::errc{}) {
    return chronos::common::make_unexpected(invalid("formatting a query result number failed"));
  }
  return std::string{buffer.data(), formatted.ptr};
}

[[nodiscard]] chronos::common::Result<std::string>
format_decimal(const chronos::common::ByteView bytes, const std::uint16_t scale) {
  if (bytes.size() != 16U) {
    return chronos::common::make_unexpected(invalid("query result decimal has an invalid width"));
  }
  std::array<std::uint8_t, 16U> magnitude{};
  const bool negative = (std::to_integer<std::uint8_t>(bytes.back()) & 0x80U) != 0U;
  std::uint16_t carry = negative ? 1U : 0U;
  for (std::size_t index = 0U; index < magnitude.size(); ++index) {
    std::uint16_t limb = std::to_integer<std::uint8_t>(bytes[index]);
    if (negative) {
      limb = static_cast<std::uint16_t>(~limb) & 0xffU;
      limb = static_cast<std::uint16_t>(limb + carry);
      carry = static_cast<std::uint16_t>(limb >> 8U);
    }
    magnitude[index] = static_cast<std::uint8_t>(limb & 0xffU);
  }

  std::string digits;
  do {
    carry = 0U;
    bool nonzero{};
    for (std::size_t index = magnitude.size(); index > 0U; --index) {
      const std::uint16_t dividend =
          static_cast<std::uint16_t>((carry << 8U) | magnitude[index - 1U]);
      magnitude[index - 1U] = static_cast<std::uint8_t>(dividend / 10U);
      carry = static_cast<std::uint16_t>(dividend % 10U);
      nonzero = nonzero || magnitude[index - 1U] != 0U;
    }
    digits.push_back(static_cast<char>('0' + carry));
    if (!nonzero) {
      break;
    }
  } while (true);
  std::ranges::reverse(digits);

  if (scale != 0U) {
    if (digits.size() <= scale) {
      digits.insert(0U, static_cast<std::size_t>(scale) - digits.size() + 1U, '0');
    }
    digits.insert(digits.size() - scale, 1U, '.');
  }
  const bool zero =
      std::ranges::all_of(digits, [](const char value) { return value == '0' || value == '.'; });
  if (negative && !zero) {
    digits.insert(digits.begin(), '-');
  }
  return digits;
}

[[nodiscard]] chronos::common::Result<std::string>
format_query_cell(const chronos::network::QueryResultCell& cell,
                  const chronos::schema::LogicalType type) {
  using chronos::schema::LogicalTypeKind;
  if (cell.is_null) {
    return std::string{"NULL"};
  }
  chronos::common::ByteReader reader{cell.value};
  switch (type.kind()) {
  case LogicalTypeKind::kBool: {
    auto value = reader.read_u8();
    if (!value.has_value()) {
      return chronos::common::make_unexpected(value.error());
    }
    return *value == 0U ? std::string{"false"} : std::string{"true"};
  }
  case LogicalTypeKind::kInt8:
    return format_number(reader.read_i8());
  case LogicalTypeKind::kInt16:
    return format_number(reader.read_i16_le());
  case LogicalTypeKind::kInt32:
  case LogicalTypeKind::kDate:
    return format_number(reader.read_i32_le());
  case LogicalTypeKind::kInt64:
  case LogicalTypeKind::kTimestampNs:
    return format_number(reader.read_i64_le());
  case LogicalTypeKind::kUInt8:
    return format_number(reader.read_u8());
  case LogicalTypeKind::kUInt16:
    return format_number(reader.read_u16_le());
  case LogicalTypeKind::kUInt32:
    return format_number(reader.read_u32_le());
  case LogicalTypeKind::kUInt64:
    return format_number(reader.read_u64_le());
  case LogicalTypeKind::kFloat32:
    return format_number(reader.read_float32_le());
  case LogicalTypeKind::kFloat64:
    return format_number(reader.read_float64_le());
  case LogicalTypeKind::kDecimal:
    return format_decimal(cell.value, type.parameter_1());
  case LogicalTypeKind::kSymbol:
  case LogicalTypeKind::kString: {
    std::string text(cell.value.size(), '\0');
    if (!cell.value.empty()) {
      std::memcpy(text.data(), cell.value.data(), cell.value.size());
    }
    return escaped_text(text);
  }
  case LogicalTypeKind::kBinary: {
    constexpr std::array<char, 16> kHex{'0', '1', '2', '3', '4', '5', '6', '7',
                                        '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    std::string output{"0x"};
    output.reserve(2U + (cell.value.size() * 2U));
    for (const std::byte byte : cell.value) {
      const std::uint8_t value = std::to_integer<std::uint8_t>(byte);
      output.push_back(kHex[value >> 4U]);
      output.push_back(kHex[value & 0x0fU]);
    }
    return output;
  }
  case LogicalTypeKind::kUuid: {
    if (cell.value.size() != chronos::common::Uuid::kSize) {
      return chronos::common::make_unexpected(invalid("query result UUID has an invalid width"));
    }
    chronos::common::Uuid::Bytes bytes{};
    std::ranges::copy(cell.value, bytes.begin());
    const auto formatted = format_uuid(chronos::common::Uuid{bytes});
    return std::string{formatted.data()};
  }
  }
  return chronos::common::make_unexpected(invalid("query result logical type is unsupported"));
}

struct PrintedColumn {
  std::string name;
  chronos::schema::LogicalType type;
  bool nullable{};
};

[[nodiscard]] chronos::common::Status
print_query_batch(const chronos::network::QueryResultBatchView& batch,
                  std::vector<PrintedColumn>& schema, std::ostream& output = std::cout) {
  const auto columns = batch.columns();
  if (schema.empty()) {
    schema.reserve(columns.size());
    for (std::size_t index = 0U; index < columns.size(); ++index) {
      if (index != 0U) {
        output << '\t';
      }
      output << escaped_text(columns[index].name);
      schema.push_back({.name = std::string{columns[index].name},
                        .type = columns[index].type,
                        .nullable = columns[index].nullable});
    }
    output << '\n';
  } else {
    if (columns.size() != schema.size()) {
      return invalid("query result schema changed between batches");
    }
    for (std::size_t index = 0U; index < columns.size(); ++index) {
      if (columns[index].name != schema[index].name || columns[index].type != schema[index].type ||
          columns[index].nullable != schema[index].nullable) {
        return invalid("query result schema changed between batches");
      }
    }
  }

  for (std::uint32_t row = 0U; row < batch.row_count(); ++row) {
    for (std::size_t column = 0U; column < columns.size(); ++column) {
      if (column != 0U) {
        output << '\t';
      }
      const auto* const cell = batch.cell(row, column);
      if (cell == nullptr) {
        return invalid("query result cell is missing");
      }
      auto formatted = format_query_cell(*cell, columns[column].type);
      if (!formatted.has_value()) {
        return formatted.error();
      }
      output << *formatted;
    }
    output << '\n';
  }
  if (!output) {
    return io_error("writing SQL result", EIO);
  }
  return chronos::common::Status::ok();
}

[[nodiscard]] chronos::common::Result<int> connect_sql_socket(const SqlOptions& options) {
  const int socket = ::socket(AF_INET, SOCK_STREAM, 0);
  if (socket < 0) {
    return chronos::common::make_unexpected(io_error("creating SQL client socket"));
  }
  Descriptor descriptor{socket};
  if (::fcntl(socket, F_SETFD, FD_CLOEXEC) != 0) {
    return chronos::common::make_unexpected(io_error("setting SQL client close-on-exec"));
  }
  const timeval timeout{.tv_sec = kSqlSocketTimeoutSeconds, .tv_usec = 0};
  if (::setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) != 0 ||
      ::setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) != 0) {
    return chronos::common::make_unexpected(io_error("setting SQL client socket timeout"));
  }
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(options.port);
  if (::inet_pton(AF_INET, options.host.c_str(), &address.sin_addr) != 1) {
    return chronos::common::make_unexpected(invalid("SQL host is not a valid IPv4 address"));
  }
  // POSIX requires the generic sockaddr view of the initialized IPv4 address.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  if (::connect(socket, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
    return chronos::common::make_unexpected(io_error("connecting to chronosd"));
  }
  return descriptor.release();
}

[[nodiscard]] chronos::common::Status send_pending(const int socket,
                                                   chronos::network::NativeClientSession& session) {
  while (!session.pending_write().empty()) {
    const chronos::common::ByteView bytes = session.pending_write();
    const ssize_t sent = ::send(socket, bytes.data(), bytes.size(), MSG_NOSIGNAL);
    if (sent < 0 && errno == EINTR) {
      continue;
    }
    if (sent <= 0) {
      return io_error("sending native protocol bytes", sent == 0 ? EPIPE : errno);
    }
    if (const chronos::common::Status consumed =
            session.consume_written(static_cast<std::size_t>(sent));
        !consumed.is_ok()) {
      return consumed;
    }
  }
  return chronos::common::Status::ok();
}

[[nodiscard]] chronos::common::Result<std::vector<chronos::network::Frame>>
receive_frames(const int socket, chronos::network::NativeClientSession& session) {
  std::array<std::byte, std::size_t{64U} * 1024U> buffer{};
  ssize_t received{};
  do {
    received = ::recv(socket, buffer.data(), buffer.size(), 0);
  } while (received < 0 && errno == EINTR);
  if (received <= 0) {
    return chronos::common::make_unexpected(
        io_error("receiving native protocol bytes", received == 0 ? ECONNRESET : errno));
  }
  return session.receive(
      chronos::common::ByteView{buffer.data(), static_cast<std::size_t>(received)});
}

[[nodiscard]] std::string_view
protocol_error_name(const chronos::network::ProtocolErrorCode code) noexcept {
  using chronos::network::ProtocolErrorCode;
  switch (code) {
  case ProtocolErrorCode::kMalformedFrame:
    return "malformed_frame";
  case ProtocolErrorCode::kUnsupportedVersion:
    return "unsupported_version";
  case ProtocolErrorCode::kInvalidState:
    return "invalid_state";
  case ProtocolErrorCode::kDuplicateRequest:
    return "duplicate_request";
  case ProtocolErrorCode::kUnknownRequest:
    return "unknown_request";
  case ProtocolErrorCode::kOverloaded:
    return "overloaded";
  case ProtocolErrorCode::kCancelled:
    return "cancelled";
  case ProtocolErrorCode::kInvalidRequest:
    return "invalid_request";
  case ProtocolErrorCode::kExecutionFailure:
    return "execution_failure";
  case ProtocolErrorCode::kUnauthorized:
    return "unauthorized";
  case ProtocolErrorCode::kInternal:
    return "internal";
  }
  return "unknown";
}

[[nodiscard]] int run_sql(const SqlOptions& options) {
  auto socket_result = connect_sql_socket(options);
  if (!socket_result.has_value()) {
    std::cerr << "chronosctl: " << socket_result.error().to_string() << '\n';
    return 1;
  }
  Descriptor socket{*socket_result};
  auto session_result =
      chronos::network::NativeClientSession::create({.maximum_in_flight_requests = 1U});
  if (!session_result.has_value()) {
    std::cerr << "chronosctl: " << session_result.error().to_string() << '\n';
    return 1;
  }
  auto session = std::move(*session_result);
  if (const chronos::common::Status status = session.queue_handshake(); !status.is_ok()) {
    std::cerr << "chronosctl: " << status.to_string() << '\n';
    return 1;
  }
  if (const chronos::common::Status status = send_pending(socket.get(), session); !status.is_ok()) {
    std::cerr << "chronosctl: " << status.to_string() << '\n';
    return 1;
  }
  while (session.phase() == chronos::network::ClientSessionPhase::kAwaitingServerHello) {
    auto frames = receive_frames(socket.get(), session);
    if (!frames.has_value()) {
      std::cerr << "chronosctl: " << frames.error().to_string() << '\n';
      return 1;
    }
  }
  if (session.phase() != chronos::network::ClientSessionPhase::kActive) {
    std::cerr << "chronosctl: invalid_state: native protocol handshake did not become active\n";
    return 1;
  }

  auto request_id = session.queue_query(options.execute);
  if (!request_id.has_value()) {
    std::cerr << "chronosctl: " << request_id.error().to_string() << '\n';
    return 1;
  }
  if (const chronos::common::Status status = send_pending(socket.get(), session); !status.is_ok()) {
    std::cerr << "chronosctl: " << status.to_string() << '\n';
    return 1;
  }

  std::vector<PrintedColumn> result_schema;
  bool ended{};
  while (session.in_flight_requests() != 0U) {
    auto frames = receive_frames(socket.get(), session);
    if (!frames.has_value()) {
      std::cerr << "chronosctl: " << frames.error().to_string() << '\n';
      return 1;
    }
    for (const chronos::network::Frame& frame : *frames) {
      if (frame.header.message_type == chronos::network::MessageType::kQueryResult) {
        auto batch = chronos::network::decode_query_result_batch(frame.payload);
        if (!batch.has_value()) {
          std::cerr << "chronosctl: " << batch.error().to_string() << '\n';
          return 1;
        }
        if (const chronos::common::Status status = print_query_batch(*batch, result_schema);
            !status.is_ok()) {
          std::cerr << "chronosctl: " << status.to_string() << '\n';
          return 1;
        }
      } else if (frame.header.message_type == chronos::network::MessageType::kError) {
        auto error = chronos::network::decode_error_message(frame.payload);
        if (!error.has_value()) {
          std::cerr << "chronosctl: " << error.error().to_string() << '\n';
          return 1;
        }
        std::string message(error->message.size(), '\0');
        if (!error->message.empty()) {
          std::memcpy(message.data(), error->message.data(), error->message.size());
        }
        std::cerr << "chronosctl: server " << protocol_error_name(error->code) << ": " << message
                  << '\n';
        return 1;
      } else if (frame.header.message_type == chronos::network::MessageType::kQueryEnd) {
        ended = true;
      }
    }
  }
  if (!ended) {
    std::cerr << "chronosctl: invalid_state: SQL request completed without QUERY_END\n";
    return 1;
  }
  return 0;
}

[[nodiscard]] int run_routed_sql(RoutedSqlOptions options) {
  auto routes = chronos::service::NativeClientTlsRouteOwner::load(
      {.route_config_file = std::move(options.routes_file),
       .tls = {.certificate_chain_file = std::move(options.tls_certificate_file),
               .private_key_file = std::move(options.tls_private_key_file),
               .trust_store_file = std::move(options.tls_trust_store_file)}});
  if (!routes.has_value()) {
    std::cerr << "chronosctl: " << routes.error().to_string() << '\n';
    return 1;
  }

  try {
    const auto published = routes->leader_routes();
    std::vector<chronos::network::NativeLeaderRoute> operation_routes(published.begin(),
                                                                      published.end());
    chronos::network::NativeQueryTcpExecutionConfig config{
        .client = {.retry = {.routing = {.group_id = options.group_id,
                                         .initial_node_id = options.initial_node_id,
                                         .minimum_placement_epoch = options.minimum_placement_epoch,
                                         .routes = std::move(operation_routes),
                                         .limits = {.maximum_routes = published.size(),
                                                    .maximum_redirects = kMaximumRedirects}}},
                   .authenticator = &routes->authority(),
                   .node_authorizer = &routes->authority()},
        .operation_deadline = std::chrono::steady_clock::now() + options.timeout};
    auto execution = chronos::network::NativeQueryTcpExecution::begin(std::move(config),
                                                                      std::move(options.execute));
    if (!execution.has_value()) {
      std::cerr << "chronosctl: " << execution.error().to_string() << '\n';
      return 1;
    }
    while (execution->state() == chronos::network::NativeQueryTcpExecutionState::kRunning) {
      const chronos::common::Status status = execution->poll_once(kMaximumPollWait);
      if (!status.is_ok()) {
        std::cerr << "chronosctl: " << status.to_string() << '\n';
        return 1;
      }
    }
    if (!execution->result().has_value()) {
      std::cerr << "chronosctl: " << execution->failure().to_string() << '\n';
      return 1;
    }

    std::ostringstream formatted_result;
    std::vector<PrintedColumn> result_schema;
    for (const auto& encoded_batch : execution->result()->encoded_batches) {
      auto batch = chronos::network::decode_query_result_batch(encoded_batch);
      if (!batch.has_value()) {
        std::cerr << "chronosctl: " << batch.error().to_string() << '\n';
        return 1;
      }
      const chronos::common::Status printed =
          print_query_batch(*batch, result_schema, formatted_result);
      if (!printed.is_ok()) {
        std::cerr << "chronosctl: " << printed.to_string() << '\n';
        return 1;
      }
    }
    std::cout << formatted_result.view();
    if (!std::cout) {
      std::cerr << "chronosctl: io_error: writing routed SQL result failed\n";
      return 1;
    }
    return 0;
  } catch (const std::bad_alloc&) {
    std::cerr << "chronosctl: resource_exhausted: allocating routed SQL operation state failed\n";
    return 1;
  } catch (const std::length_error&) {
    std::cerr << "chronosctl: resource_exhausted: routed SQL operation exceeds container limits\n";
    return 1;
  }
}

[[nodiscard]] std::string_view
outcome_name(const chronos::network::IngestOutcome outcome) noexcept {
  return outcome == chronos::network::IngestOutcome::kApplied ? "APPLIED" : "MATCHING_RETRY";
}

void print_receipt(const chronos::network::QuorumSyncIngestAcknowledgement& receipt,
                   const chronos::network::NativeQuorumIngestTcpExecutionMetrics& metrics,
                   const bool json) {
  const auto group = format_uuid(receipt.group_id);
  if (json) {
    std::cout << R"({"command":"quorum-sync","status":"ok","outcome":")"
              << outcome_name(receipt.outcome) << R"(","group_id":")" << group.data()
              << R"(","leader_node_id":)" << receipt.leader_node_id
              << ",\"leader_term\":" << receipt.leader_term
              << ",\"log_index\":" << receipt.log_index << ",\"entry_term\":" << receipt.entry_term
              << ",\"local_durable_physical_sequence\":" << receipt.local_durable_physical_sequence
              << ",\"attempts\":" << metrics.attempts_started
              << ",\"redirects\":" << metrics.redirects_followed << "}\n";
    return;
  }
  std::cout << "QUORUM_SYNC " << outcome_name(receipt.outcome) << " group=" << group.data()
            << " leader_node_id=" << receipt.leader_node_id
            << " leader_term=" << receipt.leader_term << " log_index=" << receipt.log_index
            << " entry_term=" << receipt.entry_term
            << " local_durable_physical_sequence=" << receipt.local_durable_physical_sequence
            << " attempts=" << metrics.attempts_started
            << " redirects=" << metrics.redirects_followed << '\n';
}

[[nodiscard]] int run_quorum_sync(QuorumSyncOptions options) {
  auto append = read_append_file(options.append_file);
  if (!append.has_value()) {
    std::cerr << "chronosctl: " << append.error().to_string() << '\n';
    return 1;
  }
  const auto decoded = chronos::ingest::decode_columnar_append_v1_exact(*append);
  if (!decoded.has_value()) {
    std::cerr << "chronosctl: " << decoded.error().status().to_string() << '\n';
    return 1;
  }

  auto routes = chronos::service::NativeClientTlsRouteOwner::load(
      {.route_config_file = std::move(options.routes_file),
       .tls = {.certificate_chain_file = std::move(options.tls_certificate_file),
               .private_key_file = std::move(options.tls_private_key_file),
               .trust_store_file = std::move(options.tls_trust_store_file)}});
  if (!routes.has_value()) {
    std::cerr << "chronosctl: " << routes.error().to_string() << '\n';
    return 1;
  }

  try {
    const auto published = routes->leader_routes();
    std::vector<chronos::network::NativeLeaderRoute> operation_routes(published.begin(),
                                                                      published.end());
    chronos::network::NativeQuorumIngestTcpExecutionConfig config{
        .client = {.retry = {.routing = {.group_id = options.group_id,
                                         .initial_node_id = options.initial_node_id,
                                         .minimum_placement_epoch = options.minimum_placement_epoch,
                                         .routes = std::move(operation_routes),
                                         .limits = {.maximum_routes = published.size(),
                                                    .maximum_redirects = kMaximumRedirects}}},
                   .authenticator = &routes->authority(),
                   .node_authorizer = &routes->authority()},
        .operation_deadline = std::chrono::steady_clock::now() + options.timeout};
    auto execution = chronos::network::NativeQuorumIngestTcpExecution::begin(std::move(config),
                                                                             std::move(*append));
    if (!execution.has_value()) {
      std::cerr << "chronosctl: " << execution.error().to_string() << '\n';
      return 1;
    }
    while (execution->state() == chronos::network::NativeQuorumIngestTcpExecutionState::kRunning) {
      const chronos::common::Status status = execution->poll_once(kMaximumPollWait);
      if (!status.is_ok()) {
        std::cerr << "chronosctl: " << status.to_string() << '\n';
        return 1;
      }
    }
    auto receipt = execution->result();
    if (!receipt.has_value()) {
      std::cerr << "chronosctl: " << receipt.error().to_string() << '\n';
      return 1;
    }
    print_receipt(*receipt, execution->metrics(), options.json);
    return 0;
  } catch (const std::bad_alloc&) {
    std::cerr
        << "chronosctl: resource_exhausted: allocating native client operation state failed\n";
    return 1;
  } catch (const std::length_error&) {
    std::cerr
        << "chronosctl: resource_exhausted: native client operation exceeds container limits\n";
    return 1;
  }
}

} // namespace

namespace {

[[nodiscard]] int run_main(const int argc, const char* const* const argv) {
  const std::string_view program =
      argc > 0 ? std::string_view{argv[0]} : std::string_view{"chronosctl"};
  if (argc == 2 && std::string_view{argv[1]} == "version") {
    std::cout << chronos::common::version_text() << '\n';
    return 0;
  }
  if (argc == 3 && std::string_view{argv[1]} == "version" &&
      std::string_view{argv[2]} == "--json") {
    std::cout << chronos::common::version_json() << '\n';
    return 0;
  }
  if (argc == 2 && std::string_view{argv[1]} == "--help") {
    print_usage(program, std::cout);
    return 0;
  }
  if (argc >= 2 && std::string_view{argv[1]} == "sql") {
    try {
      SqlParseResult parsed =
          parse_sql_options(std::span<const char* const>{argv, static_cast<std::size_t>(argc)});
      if (parsed.help) {
        print_sql_usage(program, std::cout);
        return 0;
      }
      if (!parsed.options.has_value()) {
        std::cerr << "chronosctl: " << parsed.error << '\n';
        print_sql_usage(program, std::cerr);
        return 2;
      }
      return run_sql(*parsed.options);
    } catch (const std::bad_alloc&) {
      std::cerr << "chronosctl: resource_exhausted: SQL command allocation failed\n";
      return 1;
    } catch (const std::length_error&) {
      std::cerr << "chronosctl: resource_exhausted: SQL command exceeds container limits\n";
      return 1;
    }
  }
  if (argc >= 2 && std::string_view{argv[1]} == "routed-sql") {
    try {
      RoutedSqlParseResult parsed = parse_routed_sql_options(
          std::span<const char* const>{argv, static_cast<std::size_t>(argc)});
      if (parsed.help) {
        print_routed_sql_usage(program, std::cout);
        return 0;
      }
      if (!parsed.options.has_value()) {
        std::cerr << "chronosctl: " << parsed.error << '\n';
        print_routed_sql_usage(program, std::cerr);
        return 2;
      }
      return run_routed_sql(std::move(*parsed.options));
    } catch (const std::bad_alloc&) {
      std::cerr << "chronosctl: resource_exhausted: parsing routed SQL options failed\n";
      return 1;
    } catch (const std::length_error&) {
      std::cerr << "chronosctl: resource_exhausted: routed SQL options exceed container limits\n";
      return 1;
    }
  }
  if (argc >= 2 && std::string_view{argv[1]} == "quorum-sync") {
    try {
      ParseResult parsed = parse_quorum_sync_options(
          std::span<const char* const>{argv, static_cast<std::size_t>(argc)});
      if (parsed.help) {
        print_quorum_sync_usage(program, std::cout);
        return 0;
      }
      if (!parsed.options.has_value()) {
        std::cerr << "chronosctl: " << parsed.error << '\n';
        print_quorum_sync_usage(program, std::cerr);
        return 2;
      }
      return run_quorum_sync(std::move(*parsed.options));
    } catch (const std::bad_alloc&) {
      std::cerr << "chronosctl: resource_exhausted: parsing command options failed\n";
      return 1;
    } catch (const std::length_error&) {
      std::cerr << "chronosctl: resource_exhausted: command options exceed container limits\n";
      return 1;
    }
  }

  print_usage(program, std::cerr);
  return 2;
}

} // namespace

int main(const int argc, const char* const argv[]) noexcept {
  try {
    return run_main(argc, argv);
  } catch (const std::bad_alloc&) {
    std::cerr << "chronosctl: resource_exhausted: command allocation failed\n";
  } catch (const std::length_error&) {
    std::cerr << "chronosctl: resource_exhausted: command exceeds container limits\n";
  } catch (const std::exception& error) {
    std::cerr << "chronosctl: internal: unhandled command failure: " << error.what() << '\n';
  } catch (...) {
    std::cerr << "chronosctl: internal: unhandled non-standard command failure\n";
  }
  return 1;
}
