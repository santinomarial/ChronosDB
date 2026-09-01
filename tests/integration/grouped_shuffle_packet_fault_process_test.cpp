#include "chronos/cluster/distributed_vector_grouped_aggregate_shuffle_job_control_tls.hpp"
#include "chronos/network/tcp_socket.hpp"

#include <array>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fcntl.h>
#include <filesystem>
#include <gtest/gtest.h>
#include <optional>
#include <poll.h>
#include <string>
#include <string_view>
#include <sys/types.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::integration {
namespace {

using JobControlClient = cluster::DistributedVectorGroupedAggregateShuffleJobControlTlsClient;
using JobControlClientState =
    cluster::DistributedVectorGroupedAggregateShuffleJobControlTlsClientState;
using JobControlRequest = cluster::DistributedVectorGroupedAggregateShuffleJobControlRequest;
using JobControlResponse = cluster::DistributedVectorGroupedAggregateShuffleJobControlResponse;
using JobControlServer = cluster::DistributedVectorGroupedAggregateShuffleJobControlTlsServer;
using JobControlServerState =
    cluster::DistributedVectorGroupedAggregateShuffleJobControlTlsServerState;

[[nodiscard]] std::filesystem::path fixture(const char* name) {
  return std::filesystem::path{CHRONOS_TEST_TLS_FIXTURE_DIR} / name;
}

[[nodiscard]] network::TlsServerConfig server_tls() {
  return {.certificate_chain_file = fixture("server.pem").string(),
          .private_key_file = fixture("server-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string()};
}

[[nodiscard]] network::TlsClientConfig client_tls() {
  return {.certificate_chain_file = fixture("client.pem").string(),
          .private_key_file = fixture("client-key.pem").string(),
          .trust_store_file = fixture("ca.pem").string(),
          .expected_server_identity = "127.0.0.1"};
}

[[nodiscard]] common::Uuid uuid(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.back() = static_cast<std::byte>(seed);
  return common::Uuid{bytes};
}

[[nodiscard]] cluster::DistributedVectorGroupedAggregateShuffleJobPrepare prepare() {
  const auto string = schema::LogicalType::create(schema::LogicalTypeKind::kString).value();
  const auto count = schema::LogicalType::create(schema::LogicalTypeKind::kInt64).value();
  return {.coordinator_node_id = 9U,
          .target_node_id = 3U,
          .coordinator_result_endpoint = {{127U, 0U, 0U, 1U}, 8137U},
          .execution_timeout = std::chrono::milliseconds{30'000},
          .authority = cluster::DistributedVectorGroupedAggregateShuffleAuthority::create(
                           uuid(1U), {{schema::TabletId::from_uuid(uuid(2U)).value(), 3U}},
                           {{0U, 3U}}, {{0U, string, false}},
                           {{query::VectorAggregateOperation::kCountStar, std::nullopt}})
                           .value(),
          .result_schema = {.columns = {{"region", string, false}, {"count", count, false}}}};
}

[[nodiscard]] cluster::DistributedVectorGroupedAggregateShuffleJobCancel cancel() {
  return {.query_id = uuid(1U), .coordinator_node_id = 9U, .target_node_id = 3U};
}

class Authenticator final : public network::ConnectionAuthenticator {
public:
  explicit Authenticator(const std::uint64_t principal) : principal_(principal) {}

  common::Result<network::PeerAuthenticationResult>
  authenticate(const network::PeerAuthenticationRequest& request) override {
    saw_fingerprint = request.peer_certificate_sha256.has_value();
    return network::PeerAuthenticationResult{.authorized = true, .principal_id = principal_};
  }

  bool saw_fingerprint{};

private:
  std::uint64_t principal_{};
};

class Authorizer final : public cluster::ClusterNodePrincipalAuthorizer {
public:
  common::Result<bool> authorize_node(const std::uint64_t principal,
                                      const raft::NodeId node) const override {
    return (principal == 93U && node == 9U) || (principal == 94U && node == 3U);
  }
};

[[nodiscard]] cluster::DistributedVectorGroupedAggregateShuffleJobControlTlsLimits
job_control_limits() {
  return {.handshake_timeout = std::chrono::milliseconds{100},
          .exchange_timeout = std::chrono::milliseconds{100}};
}

struct TcpPair {
  network::TcpSocket server;
  network::TcpSocket client;
};

[[nodiscard]] common::Result<TcpPair> connect_pair(network::TcpListener& listener) {
  auto client = network::TcpSocket::begin_connect(listener.bound_endpoint());
  if (!client.has_value())
    return common::make_unexpected(client.error());
  std::optional<network::TcpSocket> server;
  for (std::size_t attempt = 0U; attempt < 64U; ++attempt) {
    std::array<pollfd, 2U> descriptors{{
        {.fd = listener.descriptor(), .events = POLLIN, .revents = 0},
        {.fd = client->descriptor(), .events = POLLOUT, .revents = 0},
    }};
    const int ready = ::poll(descriptors.data(), descriptors.size(), 100);
    if (ready < 0 && errno != EINTR) {
      return common::make_unexpected(common::Status{common::StatusCode::kUnavailable,
                                                    "loopback Job Control connection poll failed"});
    }
    if (client->connect_state() == network::TcpConnectState::kInProgress &&
        (descriptors[1].revents & (POLLOUT | POLLERR | POLLHUP)) != 0) {
      auto connected = client->finish_connect();
      if (!connected.has_value())
        return common::make_unexpected(connected.error());
    }
    if (!server.has_value() && (descriptors[0].revents & POLLIN) != 0) {
      auto accepted = listener.accept_one();
      if (!accepted.has_value())
        return common::make_unexpected(accepted.error());
      if (accepted->has_value())
        server = std::move(*accepted);
    }
    if (server.has_value() && client->connect_state() == network::TcpConnectState::kConnected)
      return TcpPair{.server = std::move(*server), .client = std::move(*client)};
  }
  return common::make_unexpected(common::Status{
      common::StatusCode::kUnavailable, "loopback Job Control connection did not complete"});
}

struct ControlSessions {
  // The TLS sessions borrow these descriptors and therefore precede them in destruction order.
  network::TcpSocket server_tcp;
  network::TcpSocket client_tcp;
  JobControlServer server;
  JobControlClient client;
};

struct SessionDependencies {
  network::TlsServerContext* server_context{};
  network::TlsClientContext* client_context{};
  Authenticator* client_authenticator{};
  Authenticator* server_authenticator{};
  Authorizer* authorizer{};
  cluster::DistributedVectorGroupedAggregateShuffleJobService* service{};
};

[[nodiscard]] common::Result<ControlSessions>
create_sessions(network::TcpListener& listener, const SessionDependencies dependencies,
                JobControlRequest request, const JobControlClient::TimePoint start) {
  auto pair = connect_pair(listener);
  if (!pair.has_value())
    return common::make_unexpected(pair.error());
  auto server_socket =
      network::TlsSocket::accept(*dependencies.server_context, pair->server.descriptor());
  if (!server_socket.has_value())
    return common::make_unexpected(server_socket.error());
  auto client_socket =
      network::TlsSocket::connect(*dependencies.client_context, pair->client.descriptor());
  if (!client_socket.has_value())
    return common::make_unexpected(client_socket.error());
  auto server = JobControlServer::create(std::move(*server_socket),
                                         {.authenticator = dependencies.client_authenticator,
                                          .service = dependencies.service,
                                          .peer_ipv4_address = {127U, 0U, 0U, 1U},
                                          .limits = job_control_limits()},
                                         start);
  if (!server.has_value())
    return common::make_unexpected(server.error());
  auto client = JobControlClient::create(std::move(*client_socket),
                                         {.authenticator = dependencies.server_authenticator,
                                          .node_authorizer = dependencies.authorizer,
                                          .peer_ipv4_address = {127U, 0U, 0U, 1U},
                                          .request = std::move(request),
                                          .limits = job_control_limits()},
                                         start);
  if (!client.has_value())
    return common::make_unexpected(client.error());
  return ControlSessions{.server_tcp = std::move(pair->server),
                         .client_tcp = std::move(pair->client),
                         .server = std::move(*server),
                         .client = std::move(*client)};
}

[[nodiscard]] common::Status drive_authenticated(ControlSessions& sessions,
                                                 const JobControlClient::TimePoint now) {
  for (std::size_t attempt = 0U; attempt < 1024U; ++attempt) {
    if (sessions.client.state() == JobControlClientState::kHandshaking) {
      const common::Status progress = sessions.client.on_ready(true, true, now);
      if (!progress.is_ok())
        return progress;
    }
    if (sessions.server.state() == JobControlServerState::kHandshaking) {
      const common::Status progress = sessions.server.on_ready(true, true, now);
      if (!progress.is_ok())
        return progress;
    }
    if (sessions.client.state() == JobControlClientState::kWritingRequest &&
        sessions.server.state() == JobControlServerState::kReadingRequest) {
      return common::Status::ok();
    }
  }
  return {common::StatusCode::kUnavailable,
          "loopback Job Control mutual-TLS handshake did not complete"};
}

[[nodiscard]] common::Status drive_to_response(ControlSessions& sessions,
                                               const JobControlClient::TimePoint now) {
  for (std::size_t attempt = 0U; attempt < 1024U; ++attempt) {
    if (sessions.client.state() != JobControlClientState::kReadingResponse) {
      const common::Status progress = sessions.client.on_ready(true, true, now);
      if (!progress.is_ok())
        return progress;
    }
    if (sessions.server.state() == JobControlServerState::kReadingRequest) {
      const common::Status progress = sessions.server.on_ready(true, true, now);
      if (!progress.is_ok())
        return progress;
    }
    if (sessions.client.state() == JobControlClientState::kReadingResponse &&
        sessions.server.state() == JobControlServerState::kWritingResponse) {
      return common::Status::ok();
    }
  }
  return {common::StatusCode::kUnavailable,
          "loopback Job Control request did not reach response admission"};
}

[[nodiscard]] common::Result<JobControlResponse>
complete_exchange(ControlSessions& sessions, const JobControlClient::TimePoint now) {
  for (std::size_t attempt = 0U; attempt < 1024U; ++attempt) {
    const common::Status client_progress = sessions.client.on_ready(true, true, now);
    if (!client_progress.is_ok())
      return common::make_unexpected(client_progress);
    const common::Status server_progress = sessions.server.on_ready(true, true, now);
    if (!server_progress.is_ok())
      return common::make_unexpected(server_progress);
    if (sessions.client.state() == JobControlClientState::kComplete &&
        sessions.server.state() == JobControlServerState::kComplete) {
      return sessions.client.result();
    }
  }
  return common::make_unexpected(common::Status{common::StatusCode::kUnavailable,
                                                "loopback Job Control exchange did not complete"});
}

class ChildProcess {
public:
  ChildProcess() = default;
  ~ChildProcess() {
    static_cast<void>(kill_abruptly());
  }
  ChildProcess(const ChildProcess&) = delete;
  ChildProcess& operator=(const ChildProcess&) = delete;

  [[nodiscard]] bool start(std::vector<std::string> arguments) {
    arguments.insert(arguments.begin(), GROUPED_SHUFFLE_RESULT_PROCESS_CHILD_PATH);
    std::vector<char*> raw_arguments;
    raw_arguments.reserve(arguments.size() + 1U);
    for (std::string& argument : arguments)
      raw_arguments.push_back(argument.data());
    raw_arguments.push_back(nullptr);
    std::array<int, 2U> output{};
    if (::pipe(output.data()) != 0)
      return false;
    if (::fcntl(output[0], F_SETFD, FD_CLOEXEC) != 0 ||
        ::fcntl(output[1], F_SETFD, FD_CLOEXEC) != 0) {
      ::close(output[0]);
      ::close(output[1]);
      return false;
    }
    pid_ = ::fork();
    if (pid_ == 0) {
      static_cast<void>(::dup2(output[1], STDOUT_FILENO));
      static_cast<void>(::dup2(output[1], STDERR_FILENO));
      ::close(output[0]);
      ::close(output[1]);
      ::execv(raw_arguments.front(), raw_arguments.data());
      std::_Exit(127);
    }
    ::close(output[1]);
    if (pid_ < 0) {
      ::close(output[0]);
      return false;
    }
    output_ = output[0];
    return true;
  }

  [[nodiscard]] std::optional<std::string> read_line(const std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    std::string line;
    for (;;) {
      const auto now = std::chrono::steady_clock::now();
      if (now >= deadline)
        return std::nullopt;
      const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
      pollfd descriptor{.fd = output_, .events = POLLIN, .revents = 0};
      if (::poll(&descriptor, 1U, static_cast<int>(remaining.count())) <= 0)
        return std::nullopt;
      char value{};
      if (::read(output_, &value, 1U) != 1)
        return std::nullopt;
      if (value == '\n')
        return line;
      line.push_back(value);
    }
  }

  [[nodiscard]] std::optional<std::string> read_until(const std::string_view expected,
                                                      const std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
      const auto now = std::chrono::steady_clock::now();
      if (now >= deadline)
        return std::nullopt;
      const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
      auto line = read_line(remaining);
      if (!line.has_value())
        return std::nullopt;
      if (*line == expected)
        return line;
    }
  }

  [[nodiscard]] int wait_for_exit(const std::chrono::milliseconds timeout) noexcept {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    int status{};
    while (std::chrono::steady_clock::now() < deadline) {
      const pid_t result = ::waitpid(pid_, &status, WNOHANG);
      if (result == pid_) {
        pid_ = -1;
        close_output();
        return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
      }
      if (result < 0 && errno != EINTR)
        break;
      std::this_thread::sleep_for(std::chrono::milliseconds{5});
    }
    return -1;
  }

  [[nodiscard]] bool kill_abruptly() noexcept {
    close_output();
    if (pid_ <= 0)
      return false;
    if (::kill(pid_, SIGKILL) != 0)
      return false;
    int status{};
    const bool killed =
        ::waitpid(pid_, &status, 0) == pid_ && WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL;
    pid_ = -1;
    return killed;
  }

private:
  void close_output() noexcept {
    if (output_ >= 0) {
      ::close(output_);
      output_ = -1;
    }
  }

  pid_t pid_{-1};
  int output_{-1};
};

[[nodiscard]] int run_command(std::vector<std::string> arguments, const bool quiet = false) {
  std::vector<char*> raw_arguments;
  raw_arguments.reserve(arguments.size() + 1U);
  for (std::string& argument : arguments)
    raw_arguments.push_back(argument.data());
  raw_arguments.push_back(nullptr);
  const pid_t pid = ::fork();
  if (pid == 0) {
    if (quiet) {
      const int null_output = ::open("/dev/null", O_WRONLY);
      if (null_output < 0 || ::dup2(null_output, STDOUT_FILENO) < 0 ||
          ::dup2(null_output, STDERR_FILENO) < 0) {
        std::_Exit(126);
      }
      ::close(null_output);
    }
    ::execv(raw_arguments.front(), raw_arguments.data());
    std::_Exit(127);
  }
  if (pid < 0)
    return -1;
  int status{};
  pid_t result{};
  do {
    result = ::waitpid(pid, &status, 0);
  } while (result < 0 && errno == EINTR);
  if (result != pid)
    return -1;
  return WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
}

[[nodiscard]] std::string iptables_path() {
  const char* configured = std::getenv("CHRONOS_IPTABLES_PATH");
  return configured == nullptr || *configured == '\0' ? "/usr/sbin/iptables" : configured;
}

[[nodiscard]] std::optional<std::string> packet_fault_unavailability(const std::string& iptables) {
#if !defined(__linux__)
  static_cast<void>(iptables);
  return "packet-fault qualification requires Linux netfilter";
#else
  const char* enabled = std::getenv("CHRONOS_RUN_PACKET_FAULT_TESTS");
  if (enabled == nullptr || std::string_view{enabled} != "1")
    return "set CHRONOS_RUN_PACKET_FAULT_TESTS=1 in an isolated Linux network namespace";
  if (::geteuid() != 0)
    return "packet-fault qualification requires root in an isolated namespace";
  if (::access(iptables.c_str(), X_OK) != 0)
    return "iptables executable is unavailable: " + iptables;
  if (run_command({iptables, "-w", "2", "-L", "OUTPUT", "-n"}, true) != 0)
    return "the current namespace does not grant netfilter administration";
  return std::nullopt;
#endif
}

enum class PacketDirection : std::uint8_t {
  kCoordinatorToReducer = 1,
  kReducerToCoordinator = 2,
};

class PacketDropRule {
public:
  PacketDropRule(std::string executable, const PacketDirection direction,
                 const std::uint16_t reducer_port)
      : executable_(std::move(executable)), direction_(direction),
        port_(std::to_string(reducer_port)) {}
  ~PacketDropRule() noexcept {
    try {
      if (!remove()) {
        constexpr std::string_view message{"packet-fault rule cleanup failed\n"};
        static_cast<void>(::write(STDERR_FILENO, message.data(), message.size()));
      }
    } catch (...) {
      constexpr std::string_view message{"packet-fault rule cleanup threw\n"};
      static_cast<void>(::write(STDERR_FILENO, message.data(), message.size()));
    }
  }
  PacketDropRule(const PacketDropRule&) = delete;
  PacketDropRule& operator=(const PacketDropRule&) = delete;

  [[nodiscard]] bool install() {
    if (installed_)
      return true;
    installed_ = run_command(command("-I", true)) == 0;
    return installed_;
  }

  [[nodiscard]] bool remove() {
    if (!installed_)
      return true;
    const bool removed = run_command(command("-D", false)) == 0;
    if (removed)
      installed_ = false;
    return removed;
  }

private:
  [[nodiscard]] std::vector<std::string> command(const std::string_view action,
                                                 const bool insert) const {
    std::vector<std::string> arguments{executable_, "-w", "2", std::string{action}, "OUTPUT"};
    if (insert)
      arguments.emplace_back("1");
    arguments.insert(arguments.end(),
                     {"-p", "tcp", "-d", "127.0.0.1/32",
                      direction_ == PacketDirection::kCoordinatorToReducer ? "--dport" : "--sport",
                      port_, "-j", "DROP"});
    return arguments;
  }

  std::string executable_;
  PacketDirection direction_;
  std::string port_;
  bool installed_{};
};

[[nodiscard]] std::uint16_t job_reducer_port(const std::string_view line) {
  constexpr std::string_view prefix{"JOB_REDUCER_READY "};
  if (!line.starts_with(prefix))
    return 0U;
  const std::string value{line.substr(prefix.size())};
  const unsigned long port = std::strtoul(value.c_str(), nullptr, 10);
  return port <= 65'535UL ? static_cast<std::uint16_t>(port) : 0U;
}

[[nodiscard]] std::string line_or_empty(const std::optional<std::string>& line) {
  return line.value_or(std::string{});
}

void expect_directional_packet_loss_and_healing(const PacketDirection direction) {
  const std::string iptables = iptables_path();
  if (const auto unavailable = packet_fault_unavailability(iptables); unavailable.has_value())
    GTEST_SKIP() << *unavailable;

  ChildProcess isolated_reducer;
  ChildProcess healthy_reducer;
  ASSERT_TRUE(isolated_reducer.start({"job-reducer", "2", "none"}));
  ASSERT_TRUE(healthy_reducer.start({"job-reducer", "3", "none"}));
  const auto isolated_ready = isolated_reducer.read_line(std::chrono::seconds{5});
  const auto healthy_ready = healthy_reducer.read_line(std::chrono::seconds{5});
  ASSERT_TRUE(isolated_ready.has_value());
  ASSERT_TRUE(healthy_ready.has_value());
  const std::uint16_t isolated_port = job_reducer_port(line_or_empty(isolated_ready));
  const std::uint16_t healthy_port = job_reducer_port(line_or_empty(healthy_ready));
  ASSERT_NE(isolated_port, 0U) << line_or_empty(isolated_ready);
  ASSERT_NE(healthy_port, 0U) << line_or_empty(healthy_ready);

  ChildProcess partitioned_coordinator;
  ASSERT_TRUE(
      partitioned_coordinator.start({"job-coordinator-two", std::to_string(isolated_port),
                                     std::to_string(healthy_port), "1", "5000", "300", "hold"}));
  const auto leased = partitioned_coordinator.read_line(std::chrono::seconds{5});
  ASSERT_TRUE(leased.has_value());
  ASSERT_EQ(line_or_empty(leased), "LEASED 1");
  ASSERT_TRUE(isolated_reducer.read_until("RENEWED 1", std::chrono::seconds{5}).has_value());
  ASSERT_TRUE(healthy_reducer.read_until("RENEWED 1", std::chrono::seconds{5}).has_value());

  PacketDropRule partition{iptables, direction, isolated_port};
  ASSERT_TRUE(partition.install());
  ASSERT_TRUE(isolated_reducer.read_until("EXPIRED 1", std::chrono::seconds{3}).has_value());
  ASSERT_TRUE(healthy_reducer.read_until("CANCEL_REQUESTS 1", std::chrono::seconds{3}).has_value());
  ASSERT_TRUE(partitioned_coordinator.read_until("FAILED 1", std::chrono::seconds{12}).has_value());
  const int failed_exit = partitioned_coordinator.wait_for_exit(std::chrono::seconds{12});
  ASSERT_EQ(failed_exit, 6);
  ASSERT_TRUE(partition.remove());

  ChildProcess replacement;
  ASSERT_TRUE(replacement.start({"job-coordinator-two", std::to_string(isolated_port),
                                 std::to_string(healthy_port), "2", "5000", "300", "cancel"}));
  const auto replacement_leased = replacement.read_line(std::chrono::seconds{5});
  ASSERT_TRUE(replacement_leased.has_value());
  EXPECT_EQ(line_or_empty(replacement_leased), "LEASED 2");
  ASSERT_TRUE(isolated_reducer.read_until("ACTIVE 2", std::chrono::seconds{5}).has_value());
  ASSERT_TRUE(healthy_reducer.read_until("ACTIVE 2", std::chrono::seconds{5}).has_value());
  const auto replacement_cancelled = replacement.read_line(std::chrono::seconds{5});
  ASSERT_TRUE(replacement_cancelled.has_value());
  EXPECT_EQ(line_or_empty(replacement_cancelled), "CANCELLED 2");
  EXPECT_EQ(replacement.wait_for_exit(std::chrono::seconds{5}), 0);
  EXPECT_TRUE(
      isolated_reducer.read_until("CANCEL_REQUESTS 1", std::chrono::seconds{5}).has_value());
  EXPECT_TRUE(healthy_reducer.read_until("CANCEL_REQUESTS 2", std::chrono::seconds{5}).has_value());
  EXPECT_TRUE(isolated_reducer.kill_abruptly());
  EXPECT_TRUE(healthy_reducer.kill_abruptly());
}

void expect_established_request_black_hole_and_healing() {
  const std::string iptables = iptables_path();
  if (const auto unavailable = packet_fault_unavailability(iptables); unavailable.has_value())
    GTEST_SKIP() << *unavailable;

  Authenticator client_authenticator{93U};
  Authenticator server_authenticator{94U};
  Authorizer authorizer;
  auto server_context = network::TlsServerContext::create(server_tls());
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(server_context.has_value()) << server_context.error().to_string();
  ASSERT_TRUE(client_context.has_value()) << client_context.error().to_string();
  const std::array result_contexts{cluster::DistributedQueryNodeTlsContext{9U, &*client_context}};
  auto service = cluster::DistributedVectorGroupedAggregateShuffleJobService::create(
      {.local_node_id = 3U,
       .shuffle_tls = server_tls(),
       .shuffle_authenticator = &client_authenticator,
       .result_authenticator = &server_authenticator,
       .node_authorizer = &authorizer,
       .result_tls_contexts = result_contexts});
  ASSERT_TRUE(service.has_value()) << service.error().to_string();
  auto listener = network::TcpListener::bind();
  ASSERT_TRUE(listener.has_value()) << listener.error().to_string();
  const JobControlClient::TimePoint start{};
  const SessionDependencies dependencies{.server_context = &*server_context,
                                         .client_context = &*client_context,
                                         .client_authenticator = &client_authenticator,
                                         .server_authenticator = &server_authenticator,
                                         .authorizer = &authorizer,
                                         .service = &*service};

  {
    auto sessions = create_sessions(*listener, dependencies, JobControlRequest{prepare()}, start);
    ASSERT_TRUE(sessions.has_value()) << sessions.error().to_string();
    const common::Status authenticated =
        drive_authenticated(*sessions, start + std::chrono::milliseconds{1});
    ASSERT_TRUE(authenticated.is_ok()) << authenticated.to_string();
    ASSERT_TRUE(client_authenticator.saw_fingerprint);
    ASSERT_TRUE(server_authenticator.saw_fingerprint);
    ASSERT_EQ(service->metrics().prepare_requests, 0U);

    PacketDropRule partition{iptables, PacketDirection::kCoordinatorToReducer,
                             listener->bound_endpoint().port};
    ASSERT_TRUE(partition.install());
    EXPECT_TRUE(
        sessions->client.on_ready(true, true, start + std::chrono::milliseconds{2}).is_ok());
    EXPECT_EQ(sessions->client.state(), JobControlClientState::kReadingResponse);
    EXPECT_TRUE(
        sessions->server.on_ready(true, true, start + std::chrono::milliseconds{2}).is_ok());
    EXPECT_EQ(sessions->server.state(), JobControlServerState::kReadingRequest);
    EXPECT_EQ(service->metrics().prepare_requests, 0U);

    const common::Status client_timeout =
        sessions->client.on_ready(false, false, sessions->client.deadline());
    EXPECT_EQ(client_timeout.code(), common::StatusCode::kUnavailable);
    EXPECT_EQ(sessions->client.state(), JobControlClientState::kFailed);
    const common::Status server_timeout =
        sessions->server.on_ready(false, false, sessions->server.deadline());
    EXPECT_EQ(server_timeout.code(), common::StatusCode::kUnavailable);
    EXPECT_EQ(sessions->server.state(), JobControlServerState::kFailed);
    ASSERT_TRUE(partition.remove());
  }

  {
    auto sessions = create_sessions(*listener, dependencies, JobControlRequest{prepare()}, start);
    ASSERT_TRUE(sessions.has_value()) << sessions.error().to_string();
    auto response = complete_exchange(*sessions, start + std::chrono::milliseconds{1});
    ASSERT_TRUE(response.has_value()) << response.error().to_string();
    EXPECT_EQ(response->action,
              cluster::DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare);
    EXPECT_EQ(response->status_code, common::StatusCode::kOk);
    EXPECT_FALSE(response->reducer_shuffle_endpoint.has_value());
  }
  EXPECT_EQ(service->metrics().prepare_requests, 1U);
  EXPECT_EQ(service->metrics().active_jobs, 1U);

  {
    auto sessions = create_sessions(*listener, dependencies, JobControlRequest{cancel()}, start);
    ASSERT_TRUE(sessions.has_value()) << sessions.error().to_string();
    auto response = complete_exchange(*sessions, start + std::chrono::milliseconds{1});
    ASSERT_TRUE(response.has_value()) << response.error().to_string();
    EXPECT_EQ(response->action,
              cluster::DistributedVectorGroupedAggregateShuffleJobControlAction::kCancel);
    EXPECT_EQ(response->status_code, common::StatusCode::kOk);
  }
  EXPECT_EQ(service->metrics().cancel_requests, 1U);
  EXPECT_EQ(service->metrics().cancelled_jobs, 1U);
  EXPECT_EQ(service->metrics().active_jobs, 1U);
  const common::Status reaped =
      service->poll_once(std::chrono::milliseconds{0}, start + std::chrono::milliseconds{30'002});
  ASSERT_TRUE(reaped.is_ok()) << reaped.to_string();
  EXPECT_EQ(service->metrics().active_jobs, 0U);
}

void expect_established_response_black_hole_and_idempotent_retry() {
  const std::string iptables = iptables_path();
  if (const auto unavailable = packet_fault_unavailability(iptables); unavailable.has_value())
    GTEST_SKIP() << *unavailable;

  Authenticator client_authenticator{93U};
  Authenticator server_authenticator{94U};
  Authorizer authorizer;
  auto server_context = network::TlsServerContext::create(server_tls());
  auto client_context = network::TlsClientContext::create(client_tls());
  ASSERT_TRUE(server_context.has_value()) << server_context.error().to_string();
  ASSERT_TRUE(client_context.has_value()) << client_context.error().to_string();
  const std::array result_contexts{cluster::DistributedQueryNodeTlsContext{9U, &*client_context}};
  auto service = cluster::DistributedVectorGroupedAggregateShuffleJobService::create(
      {.local_node_id = 3U,
       .shuffle_tls = server_tls(),
       .shuffle_authenticator = &client_authenticator,
       .result_authenticator = &server_authenticator,
       .node_authorizer = &authorizer,
       .result_tls_contexts = result_contexts});
  ASSERT_TRUE(service.has_value()) << service.error().to_string();
  auto listener = network::TcpListener::bind();
  ASSERT_TRUE(listener.has_value()) << listener.error().to_string();
  const JobControlClient::TimePoint start{};
  const SessionDependencies dependencies{.server_context = &*server_context,
                                         .client_context = &*client_context,
                                         .client_authenticator = &client_authenticator,
                                         .server_authenticator = &server_authenticator,
                                         .authorizer = &authorizer,
                                         .service = &*service};

  {
    auto sessions = create_sessions(*listener, dependencies, JobControlRequest{prepare()}, start);
    ASSERT_TRUE(sessions.has_value()) << sessions.error().to_string();
    const common::Status authenticated =
        drive_authenticated(*sessions, start + std::chrono::milliseconds{1});
    ASSERT_TRUE(authenticated.is_ok()) << authenticated.to_string();
    const common::Status admitted =
        drive_to_response(*sessions, start + std::chrono::milliseconds{2});
    ASSERT_TRUE(admitted.is_ok()) << admitted.to_string();
    ASSERT_EQ(service->metrics().prepare_requests, 1U);
    ASSERT_EQ(service->metrics().active_jobs, 1U);

    PacketDropRule partition{iptables, PacketDirection::kReducerToCoordinator,
                             listener->bound_endpoint().port};
    ASSERT_TRUE(partition.install());
    const common::Status server_write =
        sessions->server.on_ready(true, true, start + std::chrono::milliseconds{3});
    ASSERT_TRUE(server_write.is_ok()) << server_write.to_string();
    EXPECT_EQ(sessions->server.state(), JobControlServerState::kComplete);
    EXPECT_TRUE(
        sessions->client.on_ready(true, true, start + std::chrono::milliseconds{3}).is_ok());
    EXPECT_EQ(sessions->client.state(), JobControlClientState::kReadingResponse);
    EXPECT_FALSE(sessions->client.result().has_value());

    const common::Status client_timeout =
        sessions->client.on_ready(false, false, sessions->client.deadline());
    EXPECT_EQ(client_timeout.code(), common::StatusCode::kUnavailable);
    EXPECT_EQ(sessions->client.state(), JobControlClientState::kFailed);
    ASSERT_TRUE(partition.remove());
  }

  {
    auto sessions = create_sessions(*listener, dependencies, JobControlRequest{prepare()}, start);
    ASSERT_TRUE(sessions.has_value()) << sessions.error().to_string();
    auto response = complete_exchange(*sessions, start + std::chrono::milliseconds{1});
    ASSERT_TRUE(response.has_value()) << response.error().to_string();
    EXPECT_EQ(response->action,
              cluster::DistributedVectorGroupedAggregateShuffleJobControlAction::kPrepare);
    EXPECT_EQ(response->status_code, common::StatusCode::kOk);
  }
  EXPECT_EQ(service->metrics().prepare_requests, 2U);
  EXPECT_EQ(service->metrics().duplicate_prepares, 1U);
  EXPECT_EQ(service->metrics().active_jobs, 1U);

  {
    auto sessions = create_sessions(*listener, dependencies, JobControlRequest{cancel()}, start);
    ASSERT_TRUE(sessions.has_value()) << sessions.error().to_string();
    auto response = complete_exchange(*sessions, start + std::chrono::milliseconds{1});
    ASSERT_TRUE(response.has_value()) << response.error().to_string();
    EXPECT_EQ(response->action,
              cluster::DistributedVectorGroupedAggregateShuffleJobControlAction::kCancel);
    EXPECT_EQ(response->status_code, common::StatusCode::kOk);
  }
  EXPECT_EQ(service->metrics().cancel_requests, 1U);
  EXPECT_EQ(service->metrics().cancelled_jobs, 1U);
  EXPECT_EQ(service->metrics().active_jobs, 1U);
  const common::Status reaped =
      service->poll_once(std::chrono::milliseconds{0}, start + std::chrono::milliseconds{30'002});
  ASSERT_TRUE(reaped.is_ok()) << reaped.to_string();
  EXPECT_EQ(service->metrics().active_jobs, 0U);
}

TEST(GroupedShufflePacketFaultProcessTest,
     DropsCoordinatorPacketsToOneReducerAndHealsForAFreshLifecycle) {
  expect_directional_packet_loss_and_healing(PacketDirection::kCoordinatorToReducer);
}

TEST(GroupedShufflePacketFaultProcessTest, DropsOneReducerResponsesAndHealsForAFreshLifecycle) {
  expect_directional_packet_loss_and_healing(PacketDirection::kReducerToCoordinator);
}

TEST(GroupedShufflePacketFaultProcessTest,
     DropsAnEstablishedAuthenticatedRequestAndHealsTheRetainedListener) {
  expect_established_request_black_hole_and_healing();
}

TEST(GroupedShufflePacketFaultProcessTest,
     DropsAnAdmittedPrepareResponseAndRetriesIdempotentlyAfterHealing) {
  expect_established_response_black_hole_and_idempotent_retry();
}

} // namespace
} // namespace chronos::integration
