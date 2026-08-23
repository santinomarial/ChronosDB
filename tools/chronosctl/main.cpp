#include "chronos/common/status.hpp"
#include "chronos/common/uuid.hpp"
#include "chronos/common/version.hpp"
#include "chronos/ingest/columnar_append.hpp"
#include "chronos/ingest/columnar_append_format.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/network/native_quorum_ingest_tcp_execution.hpp"
#include "chronos/service/native_client_tls_route_owner.hpp"

#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <iostream>
#include <new>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <vector>

namespace {

constexpr std::uint64_t kMaximumTimeoutMilliseconds = 3'600'000U;
constexpr std::size_t kMaximumRedirects = 8U;
constexpr std::chrono::milliseconds kMaximumPollWait{100};

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
  std::optional<QuorumSyncOptions> options;
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
         << "  " << program << " quorum-sync [--json] --group UUID --initial-node NODE_ID\\\n"
         << "      --minimum-placement-epoch EPOCH --routes FILE --tls-cert FILE\\\n"
         << "      --tls-key FILE --tls-ca FILE --append-file FILE --timeout-ms MILLISECONDS\n";
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
      return {.help = true};
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
  return {.options = std::move(options)};
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

int main(const int argc, const char* const argv[]) {
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
