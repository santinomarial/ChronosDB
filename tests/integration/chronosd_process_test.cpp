#include "chronos/columnar/columnar_batch_codec.hpp"
#include "chronos/common/byte_reader.hpp"
#include "chronos/network/messages.hpp"
#include "chronos/network/protocol.hpp"
#include "chronos/network/subscription_messages.hpp"
#include "chronos/raft/durable_runtime.hpp"
#include "chronos/raft/metadata_codec.hpp"
#include "chronos/raft/schema_definition_codec.hpp"
#include "chronos/raft/tablet_group_binding_codec.hpp"
#include "chronos/runtime/database_bootstrap.hpp"
#include "chronos/service/replicated_ingest_database.hpp"
#include "chronos/service/replicated_ingest_runtime.hpp"
#include "columnar/columnar_test_support.hpp"
#include "ingest/ingest_test_support.hpp"

#include <arpa/inet.h>
#include <array>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <netinet/in.h>
#include <optional>
#include <poll.h>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::integration {
namespace {

class ChildProcess {
public:
  ChildProcess() = default;
  ~ChildProcess() {
    static_cast<void>(stop());
  }
  ChildProcess(const ChildProcess&) = delete;
  ChildProcess& operator=(const ChildProcess&) = delete;

  [[nodiscard]] bool start(const std::string& data_directory = {},
                           const std::string& subscription_sql = {},
                           const std::string& subscription_key_file = {},
                           const std::string& replicated_groups_file = {}) {
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
      ::close(output[0]);
      ::close(output[1]);
      if (data_directory.empty())
        ::execl(CHRONOSD_PATH, CHRONOSD_PATH, "--port", "0", nullptr);
      else if (!replicated_groups_file.empty())
        ::execl(CHRONOSD_PATH, CHRONOSD_PATH, "--port", "0", "--data-dir", data_directory.c_str(),
                "--replicated-groups", replicated_groups_file.c_str(), nullptr);
      else if (subscription_sql.empty())
        ::execl(CHRONOSD_PATH, CHRONOSD_PATH, "--port", "0", "--data-dir", data_directory.c_str(),
                nullptr);
      else
        ::execl(CHRONOSD_PATH, CHRONOSD_PATH, "--port", "0", "--data-dir", data_directory.c_str(),
                "--subscription-sql", subscription_sql.c_str(), "--subscription-key-file",
                subscription_key_file.c_str(), nullptr);
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

  [[nodiscard]] bool start_replicated_transport(const std::string& data_directory,
                                                const std::string& replicated_groups_file,
                                                const std::string& replicated_peers_file,
                                                const std::string& certificate_file,
                                                const std::string& private_key_file,
                                                const std::string& trust_store_file) {
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
      ::close(output[0]);
      ::close(output[1]);
      ::execl(CHRONOSD_PATH, CHRONOSD_PATH, "--port", "0", "--data-dir", data_directory.c_str(),
              "--replicated-groups", replicated_groups_file.c_str(), "--replicated-peers",
              replicated_peers_file.c_str(), "--raft-tls-cert", certificate_file.c_str(),
              "--raft-tls-key", private_key_file.c_str(), "--raft-tls-ca", trust_store_file.c_str(),
              nullptr);
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

  [[nodiscard]] std::string read_startup_line() const {
    pollfd descriptor{.fd = output_, .events = POLLIN, .revents = 0};
    if (::poll(&descriptor, 1, 5000) <= 0)
      return {};
    std::string line;
    for (;;) {
      char value{};
      const ssize_t count = ::read(output_, &value, 1U);
      if (count != 1 || value == '\n')
        return line;
      line.push_back(value);
    }
  }

  [[nodiscard]] int stop() noexcept {
    if (output_ >= 0) {
      ::close(output_);
      output_ = -1;
    }
    if (pid_ <= 0)
      return -1;
    static_cast<void>(::kill(pid_, SIGTERM));
    int status{};
    static_cast<void>(::waitpid(pid_, &status, 0));
    pid_ = -1;
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
  }

  [[nodiscard]] bool kill_abruptly() noexcept {
    if (output_ >= 0) {
      ::close(output_);
      output_ = -1;
    }
    if (pid_ <= 0)
      return false;
    if (::kill(pid_, SIGKILL) != 0)
      return false;
    int status{};
    if (::waitpid(pid_, &status, 0) != pid_)
      return false;
    pid_ = -1;
    return WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL;
  }

private:
  pid_t pid_{-1};
  int output_{-1};
};

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronosd-process-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  [[nodiscard]] const std::string& path() const noexcept {
    return path_;
  }

private:
  std::string path_;
};

[[nodiscard]] std::uint16_t parse_port(const std::string_view line) {
  constexpr std::string_view prefix{"chronosd listening on 127.0.0.1:"};
  const std::size_t end = line.find(' ', prefix.size());
  if (!line.starts_with(prefix) || end == std::string_view::npos)
    return 0U;
  const std::string port{line.substr(prefix.size(), end - prefix.size())};
  const unsigned long value = std::strtoul(port.c_str(), nullptr, 10);
  return value <= 65'535UL ? static_cast<std::uint16_t>(value) : 0U;
}

[[nodiscard]] int connect_client(const std::uint16_t port) {
  const int socket = ::socket(AF_INET, SOCK_STREAM, 0);
  if (socket < 0)
    return -1;
  if (::fcntl(socket, F_SETFD, FD_CLOEXEC) != 0) {
    ::close(socket);
    return -1;
  }
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  // POSIX requires the generic sockaddr view of the initialized IPv4 address.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  if (::connect(socket, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
    ::close(socket);
    return -1;
  }
  const timeval timeout{.tv_sec = 5, .tv_usec = 0};
  static_cast<void>(::setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)));
  return socket;
}

[[nodiscard]] bool send_all(const int socket, const std::vector<std::byte>& bytes) {
  std::size_t offset{};
  while (offset < bytes.size()) {
    const ssize_t count =
        ::send(socket, bytes.data() + offset, bytes.size() - offset, MSG_NOSIGNAL);
    if (count <= 0)
      return false;
    offset += static_cast<std::size_t>(count);
  }
  return true;
}

[[nodiscard]] std::vector<std::byte> receive_frame(const int socket) {
  std::vector<std::byte> bytes(network::kFrameHeaderSize);
  std::size_t offset{};
  while (offset < bytes.size()) {
    const ssize_t count = ::recv(socket, bytes.data() + offset, bytes.size() - offset, 0);
    if (count <= 0)
      return {};
    offset += static_cast<std::size_t>(count);
  }
  const auto header = network::decode_frame_header(bytes);
  if (!header.has_value())
    return {};
  bytes.resize(network::kFrameHeaderSize + header->payload_size);
  while (offset < bytes.size()) {
    const ssize_t count = ::recv(socket, bytes.data() + offset, bytes.size() - offset, 0);
    if (count <= 0)
      return {};
    offset += static_cast<std::size_t>(count);
  }
  return bytes;
}

[[nodiscard]] std::string byte_string(const common::ByteView bytes) {
  std::string value(bytes.size(), '\0');
  std::memcpy(value.data(), bytes.data(), bytes.size());
  return value;
}

void handshake(const int client) {
  const auto hello_payload = network::encode_client_hello({}).value();
  ASSERT_TRUE(
      send_all(client, network::encode_frame({.message_type = network::MessageType::kClientHello},
                                             hello_payload)
                           .value()));
  const auto response = network::decode_frame(receive_frame(client));
  ASSERT_TRUE(response.has_value());
  EXPECT_EQ(response->header.message_type, network::MessageType::kServerHello);
}

void handshake_subscriptions(const int client) {
  const auto hello_payload =
      network::encode_client_hello(
          {.maximum_minor = 1U, .feature_bits = network::kProtocolV1SubscriptionFeature})
          .value();
  ASSERT_TRUE(
      send_all(client, network::encode_frame({.message_type = network::MessageType::kClientHello},
                                             hello_payload)
                           .value()));
  const auto response = network::decode_frame(receive_frame(client));
  ASSERT_TRUE(response.has_value());
  ASSERT_EQ(response->header.message_type, network::MessageType::kServerHello);
  const auto hello = network::decode_server_hello(response->payload);
  ASSERT_TRUE(hello.has_value());
  EXPECT_EQ(hello->selected_minor, 1U);
  EXPECT_EQ(hello->feature_bits, network::kProtocolV1SubscriptionFeature);
}

void handshake_quorum_sync(const int client) {
  constexpr std::uint64_t features =
      network::kProtocolV2QuorumSyncFeature | network::kProtocolV2LeaderRedirectFeature;
  const auto hello_payload =
      network::encode_client_hello({.maximum_major = network::kProtocolV2Major,
                                    .maximum_minor = network::kProtocolV2LatestMinor,
                                    .feature_bits = features})
          .value();
  ASSERT_TRUE(
      send_all(client, network::encode_frame({.message_type = network::MessageType::kClientHello},
                                             hello_payload)
                           .value()));
  const auto response = network::decode_frame(receive_frame(client));
  ASSERT_TRUE(response.has_value());
  ASSERT_EQ(response->header.message_type, network::MessageType::kServerHello);
  const auto hello = network::decode_server_hello(response->payload);
  ASSERT_TRUE(hello.has_value());
  EXPECT_EQ(hello->selected_major, network::kProtocolV2Major);
  EXPECT_EQ(hello->selected_minor, network::kProtocolV2LatestMinor);
  EXPECT_EQ(hello->feature_bits, features);
}

[[nodiscard]] common::Uuid repeated_id(const std::uint8_t seed) {
  common::Uuid::Bytes bytes{};
  bytes.fill(static_cast<std::byte>(seed));
  return common::Uuid{bytes};
}

[[nodiscard]] raft::GroupId replicated_metadata_group() {
  return repeated_id(0x90U);
}

[[nodiscard]] raft::GroupId replicated_tablet_group() {
  return repeated_id(0x91U);
}

[[nodiscard]] schema::TabletId replicated_tablet_id() {
  return columnar::test::id<schema::TabletId>(90U);
}

[[nodiscard]] std::vector<raft::RaftGroupConfiguration> replicated_cluster_groups() {
  return {{replicated_metadata_group(), {1U, 2U, 3U}}, {replicated_tablet_group(), {1U, 2U, 3U}}};
}

[[nodiscard]] std::vector<std::byte> replicated_command() {
  auto batch = columnar::OwnedColumnarBatch::create(columnar::test::batch_schema(),
                                                    columnar::test::batch_columns())
                   .value();
  const auto batch_bytes = columnar::encode_columnar_batch_v1(batch).value();
  const auto append = ingest::encode_columnar_append_v1(
                          {.client_id = ingest::test::request_id<ingest::ClientId>(9U),
                           .client_batch_id = ingest::test::request_id<ingest::ClientBatchId>(90U),
                           .tablet_id = replicated_tablet_id()},
                          batch_bytes)
                          .value();
  return {append.bytes().begin(), append.bytes().end()};
}

void provision_replicated_database(const std::string& path) {
  const runtime::DatabaseBootstrapDescriptor descriptor{.database_id = repeated_id(0x92U),
                                                        .metadata_group_id =
                                                            replicated_metadata_group(),
                                                        .local_node_id = 1U,
                                                        .mutable_head_rows = 8U,
                                                        .maximum_sealed_generations = 2U,
                                                        .variable_column_bytes = 8U,
                                                        .maximum_retry_entries = 8U,
                                                        .wal_segment_target_bytes = 64ULL * 1024U,
                                                        .raft_segment_target_bytes = 64ULL * 1024U};
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(
      {.database_root = path, .new_database = descriptor});
  ASSERT_TRUE(bootstrap.has_value()) << bootstrap.error().to_string();
  auto tablet = ingest::TabletState::create(
      columnar::test::batch_schema(), replicated_tablet_id(),
      {.head_capacity = {.row_capacity = 8U, .variable_value_bytes = {0U, 8U, 0U}},
       .maximum_schema_versions = 1U,
       .maximum_sealed_generations = 2U,
       .maximum_retry_entries = 8U});
  auto retries = ingest::RetryDirectory::create({.maximum_entries = 8U});
  ASSERT_TRUE(tablet.has_value());
  ASSERT_TRUE(retries.has_value());
  std::vector<ingest::AsyncRaftTabletApplicationConfig> tablets;
  tablets.push_back({.group_id = replicated_tablet_group(),
                     .snapshot_storage = std::nullopt,
                     .retry_directory = std::move(*retries),
                     .tablet = std::move(*tablet),
                     .retained_schemas = {columnar::test::batch_schema()},
                     .decode_limits = {}});
  auto owner = service::ReplicatedIngestRuntime::create_new(
      {.local_node_id = 1U,
       .log = {.directory_path = bootstrap->raft_directory_path(),
               .target_segment_size = descriptor.raft_segment_target_bytes},
       .groups = {{replicated_metadata_group(), {1U}}, {replicated_tablet_group(), {1U}}},
       .tablets = std::move(tablets),
       .metadata = {.group_id = replicated_metadata_group()}});
  ASSERT_TRUE(owner.has_value()) << owner.error().to_string();
  auto election =
      owner->runtime()->try_submit({{replicated_metadata_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(election.has_value());
  ASSERT_TRUE(election->wait().has_value());
  const raft::ProposeOperation schema{
      raft::kRaftSchemaDefinitionEntryType,
      raft::encode_schema_definition_v1(
          {.name = "events", .quoted = false, .schema = columnar::test::batch_schema()})
          .value()};
  const raft::ProposeOperation policy{
      raft::kRaftMetadataCommandEntryType,
      raft::encode_metadata_command_v1(
          raft::TablePolicyMetadata{columnar::test::batch_schema()->table_id(), 1'000'000'000LL,
                                    86'400'000'000'000LL, 86'400'000'000'000LL, 0LL, 8U})
          .value()};
  const raft::ProposeOperation placement{
      raft::kRaftMetadataCommandEntryType,
      raft::encode_metadata_command_v1(
          raft::TabletPlacementMetadata{
              columnar::test::batch_schema()->table_id(), replicated_tablet_id(), 1U, {1U}, 1U})
          .value()};
  const raft::ProposeOperation binding{
      raft::kRaftTabletGroupBindingEntryType,
      raft::encode_tablet_group_binding_v1({replicated_tablet_id(), replicated_tablet_group()})
          .value()};
  auto metadata = owner->runtime()->try_submit({{replicated_metadata_group(), schema},
                                                {replicated_metadata_group(), policy},
                                                {replicated_metadata_group(), placement},
                                                {replicated_metadata_group(), binding}});
  ASSERT_TRUE(metadata.has_value());
  ASSERT_TRUE(metadata->wait().has_value());
  EXPECT_TRUE(owner->shutdown().is_ok());
  EXPECT_TRUE(bootstrap->close().is_ok());
}

[[nodiscard]] bool
enqueue_outbound(const common::Result<std::vector<raft::DurableRaftResult>>& results,
                 std::deque<raft::GroupOutboundMessage>& outbound) {
  if (!results.has_value()) {
    ADD_FAILURE() << results.error().to_string();
    return false;
  }
  for (const raft::DurableRaftResult& result : *results) {
    if (!result.status.is_ok()) {
      ADD_FAILURE() << result.status.to_string();
      return false;
    }
    if (!result.transition.has_value())
      continue;
    for (const raft::GroupOutboundMessage& message : result.transition->outbound)
      outbound.push_back(message);
  }
  return true;
}

[[nodiscard]] bool
route_outbound(std::array<std::unique_ptr<raft::DurableMultiRaftRuntime>, 3U>& runtimes,
               std::deque<raft::GroupOutboundMessage>& outbound) {
  constexpr std::size_t maximum_messages = 10'000U;
  std::size_t routed{};
  while (!outbound.empty()) {
    if (++routed > maximum_messages) {
      ADD_FAILURE() << "pre-provisioning Raft routing did not quiesce";
      return false;
    }
    raft::GroupOutboundMessage message = std::move(outbound.front());
    outbound.pop_front();
    if (message.outbound.destination == 0U || message.outbound.destination > runtimes.size()) {
      ADD_FAILURE() << "pre-provisioning Raft message has an invalid destination";
      return false;
    }
    auto& destination = runtimes[static_cast<std::size_t>(message.outbound.destination - 1U)];
    if (destination == nullptr) {
      ADD_FAILURE() << "pre-provisioning Raft destination is unavailable";
      return false;
    }
    auto received = destination->execute_batch(
        {{message.group_id,
          raft::ReceiveOperation{message.source, std::move(message.outbound.message)}}});
    if (!enqueue_outbound(received, outbound))
      return false;
  }
  return true;
}

void provision_replicated_cluster(const std::array<std::string, 3U>& roots) {
  std::array<std::unique_ptr<runtime::DatabaseBootstrap>, 3U> bootstraps;
  std::array<std::unique_ptr<raft::DurableMultiRaftRuntime>, 3U> runtimes;
  const auto groups = replicated_cluster_groups();
  for (std::size_t index = 0U; index < roots.size(); ++index) {
    ASSERT_TRUE(std::filesystem::create_directory(roots[index]));
    const runtime::DatabaseBootstrapDescriptor descriptor{
        .database_id = repeated_id(0x92U),
        .metadata_group_id = replicated_metadata_group(),
        .local_node_id = static_cast<raft::NodeId>(index + 1U),
        .mutable_head_rows = 8U,
        .maximum_sealed_generations = 2U,
        .variable_column_bytes = 8U,
        .maximum_retry_entries = 8U,
        .wal_segment_target_bytes = 64ULL * 1024U,
        .raft_segment_target_bytes = 64ULL * 1024U};
    auto bootstrap = runtime::DatabaseBootstrap::open_or_create(
        {.database_root = roots[index], .new_database = descriptor});
    ASSERT_TRUE(bootstrap.has_value()) << bootstrap.error().to_string();
    bootstraps[index] = std::make_unique<runtime::DatabaseBootstrap>(std::move(*bootstrap));
    auto durable = raft::DurableMultiRaftRuntime::create_new(
        descriptor.local_node_id,
        {.directory_path = bootstraps[index]->raft_directory_path(),
         .target_segment_size = descriptor.raft_segment_target_bytes},
        groups);
    ASSERT_TRUE(durable.has_value()) << durable.error().to_string();
    runtimes[index] = std::make_unique<raft::DurableMultiRaftRuntime>(std::move(*durable));
  }

  std::deque<raft::GroupOutboundMessage> outbound;
  auto elections = runtimes.front()->execute_batch(
      {{replicated_metadata_group(), raft::StartElectionOperation{}},
       {replicated_tablet_group(), raft::StartElectionOperation{}}});
  ASSERT_TRUE(enqueue_outbound(elections, outbound));
  ASSERT_TRUE(route_outbound(runtimes, outbound));
  ASSERT_EQ(runtimes.front()->find_group(replicated_metadata_group())->role(), raft::Role::kLeader);
  ASSERT_EQ(runtimes.front()->find_group(replicated_tablet_group())->role(), raft::Role::kLeader);

  const raft::ProposeOperation schema{
      raft::kRaftSchemaDefinitionEntryType,
      raft::encode_schema_definition_v1(
          {.name = "events", .quoted = false, .schema = columnar::test::batch_schema()})
          .value()};
  const raft::ProposeOperation policy{
      raft::kRaftMetadataCommandEntryType,
      raft::encode_metadata_command_v1(
          raft::TablePolicyMetadata{columnar::test::batch_schema()->table_id(), 1'000'000'000LL,
                                    86'400'000'000'000LL, 86'400'000'000'000LL, 0LL, 8U})
          .value()};
  const raft::ProposeOperation placement{
      raft::kRaftMetadataCommandEntryType,
      raft::encode_metadata_command_v1(
          raft::TabletPlacementMetadata{columnar::test::batch_schema()->table_id(),
                                        replicated_tablet_id(),
                                        1U,
                                        {1U, 2U, 3U},
                                        1U})
          .value()};
  const raft::ProposeOperation binding{
      raft::kRaftTabletGroupBindingEntryType,
      raft::encode_tablet_group_binding_v1({replicated_tablet_id(), replicated_tablet_group()})
          .value()};
  auto metadata = runtimes.front()->execute_batch({{replicated_metadata_group(), schema},
                                                   {replicated_metadata_group(), policy},
                                                   {replicated_metadata_group(), placement},
                                                   {replicated_metadata_group(), binding}});
  ASSERT_TRUE(enqueue_outbound(metadata, outbound));
  ASSERT_TRUE(route_outbound(runtimes, outbound));
  auto heartbeat =
      runtimes.front()->execute_batch({{replicated_metadata_group(), raft::HeartbeatOperation{}}});
  ASSERT_TRUE(enqueue_outbound(heartbeat, outbound));
  ASSERT_TRUE(route_outbound(runtimes, outbound));
  for (const auto& runtime : runtimes) {
    ASSERT_NE(runtime, nullptr);
    ASSERT_EQ(runtime->find_group(replicated_metadata_group())->commit_index(), 4U);
  }

  for (auto& runtime : runtimes) {
    ASSERT_TRUE(runtime->close().is_ok());
    runtime.reset();
  }
  for (auto& bootstrap : bootstraps) {
    ASSERT_TRUE(bootstrap->close().is_ok());
    bootstrap.reset();
  }
}

[[nodiscard]] std::uint16_t reserve_loopback_port() {
  const int descriptor = ::socket(AF_INET, SOCK_STREAM, 0);
  if (descriptor < 0)
    return 0U;
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_port = 0U;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  // POSIX requires the generic sockaddr view of the initialized IPv4 address.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  if (::bind(descriptor, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0) {
    ::close(descriptor);
    return 0U;
  }
  socklen_t size = sizeof(address);
  // POSIX requires the generic sockaddr view for the initialized IPv4 address.
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  if (::getsockname(descriptor, reinterpret_cast<sockaddr*>(&address), &size) != 0) {
    ::close(descriptor);
    return 0U;
  }
  const std::uint16_t port = ntohs(address.sin_port);
  ::close(descriptor);
  return port;
}

[[nodiscard]] std::string write_exclusive_file(std::string path, const std::string_view bytes) {
  const int file = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  EXPECT_GE(file, 0);
  if (file < 0)
    return {};
  EXPECT_EQ(::write(file, bytes.data(), bytes.size()), static_cast<ssize_t>(bytes.size()));
  EXPECT_EQ(::fsync(file), 0);
  EXPECT_EQ(::close(file), 0);
  return path;
}

struct ReplicatedClusterFiles {
  std::string groups;
  std::string peers;
  std::string trust_store;
  std::array<std::string, 3U> certificates;
  std::array<std::string, 3U> private_keys;
};

[[nodiscard]] ReplicatedClusterFiles
write_replicated_cluster_files(const std::string& directory,
                               const std::array<std::uint16_t, 3U>& ports) {
  ReplicatedClusterFiles files;
  files.groups = write_exclusive_file(directory + "/replicated-cluster-groups.conf",
                                      "CHRONOSDB_REPLICATED_GROUPS_V1\n"
                                      "90909090-9090-9090-9090-909090909090=1,2,3\n"
                                      "91919191-9191-9191-9191-919191919191=1,2,3\n");
  constexpr std::array<std::string_view, 3U> fingerprints{
      "7145018d7511b2e2af9e5531e01e9061af0a43e0b193621be717906b20e253a9",
      "baf82073b1ad1f131414b65c6b302bd1d09b7f3bbb224916e19f305f201b091f",
      "63f54c1e48bd0323467c32bc3cef96126f1ed8700a7d95c4463397a229db83ec"};
  std::string peers = "CHRONOSDB_REPLICATED_PEERS_V1\n";
  for (std::size_t index = 0U; index < ports.size(); ++index) {
    peers += std::to_string(index + 1U) + "=127.0.0.1:" + std::to_string(ports[index]) +
             ",127.0.0.1," + std::string{fingerprints[index]} + "\n";
  }
  files.peers = write_exclusive_file(directory + "/replicated-cluster-peers.conf", peers);
  const std::string fixture_directory = CHRONOS_TEST_TLS_FIXTURE_DIR;
  files.trust_store = fixture_directory + "/cluster-ca.pem";
  for (std::size_t index = 0U; index < ports.size(); ++index) {
    files.certificates[index] = fixture_directory + "/node" + std::to_string(index + 1U) + ".pem";
    files.private_keys[index] =
        directory + "/node-" + std::to_string(index + 1U) + "-private-key.pem";
    std::error_code error;
    const bool copied = std::filesystem::copy_file(
        fixture_directory + "/node" + std::to_string(index + 1U) + "-key.pem",
        files.private_keys[index], std::filesystem::copy_options::none, error);
    EXPECT_TRUE(copied) << error.message();
    EXPECT_EQ(::chmod(files.private_keys[index].c_str(), 0600), 0);
  }
  return files;
}

[[nodiscard]] std::string write_replicated_groups(const std::string& directory) {
  std::string path = directory + "/replicated-groups.conf";
  constexpr std::string_view bytes = "CHRONOSDB_REPLICATED_GROUPS_V1\n"
                                     "90909090-9090-9090-9090-909090909090=1\n"
                                     "91919191-9191-9191-9191-919191919191=1\n";
  const int file = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  EXPECT_GE(file, 0);
  if (file < 0)
    return {};
  EXPECT_EQ(::write(file, bytes.data(), bytes.size()), static_cast<ssize_t>(bytes.size()));
  EXPECT_EQ(::fsync(file), 0);
  EXPECT_EQ(::close(file), 0);
  return path;
}

[[nodiscard]] network::Frame send_replicated_ingest(const int client,
                                                    const std::uint64_t request_id = 1U) {
  const network::IngestProtocolContext context{.protocol_major = network::kProtocolV2Major,
                                               .protocol_minor = network::kProtocolV2LatestMinor,
                                               .feature_bits =
                                                   network::kProtocolV2QuorumSyncFeature};
  const auto payload = network::encode_ingest_request(network::DurabilityMode::kQuorumSync,
                                                      replicated_command(), context);
  EXPECT_TRUE(payload.has_value());
  EXPECT_TRUE(
      send_all(client, network::encode_frame({.protocol_major = network::kProtocolV2Major,
                                              .protocol_minor = network::kProtocolV2LatestMinor,
                                              .message_type = network::MessageType::kIngestRequest,
                                              .request_id = request_id},
                                             *payload)
                           .value()));
  auto response = network::decode_frame(receive_frame(client));
  EXPECT_TRUE(response.has_value());
  return response.value_or(network::Frame{});
}

struct ClusterIngestAcknowledgement {
  std::size_t node_index{};
  network::QuorumSyncIngestAcknowledgement acknowledgement;
};

[[nodiscard]] std::optional<ClusterIngestAcknowledgement>
await_cluster_ingest(std::array<int, 3U>& clients, const std::array<bool, 3U>& active,
                     std::array<std::uint64_t, 3U>& request_ids) {
  constexpr std::size_t maximum_attempts = 200U;
  for (std::size_t attempt = 0U; attempt < maximum_attempts; ++attempt) {
    for (std::size_t index = 0U; index < clients.size(); ++index) {
      if (!active[index] || clients[index] < 0)
        continue;
      const network::Frame response = send_replicated_ingest(clients[index], request_ids[index]++);
      if (response.header.message_type == network::MessageType::kQuorumSyncIngestAcknowledgement) {
        auto acknowledgement = network::decode_quorum_sync_ingest_acknowledgement(response.payload);
        if (!acknowledgement.has_value()) {
          ADD_FAILURE() << acknowledgement.error().to_string();
          return std::nullopt;
        }
        return ClusterIngestAcknowledgement{index, *acknowledgement};
      }
      if (response.header.message_type == network::MessageType::kLeaderRedirect) {
        auto redirect = network::decode_leader_redirect(response.payload);
        if (!redirect.has_value()) {
          ADD_FAILURE() << redirect.error().to_string();
          return std::nullopt;
        }
        EXPECT_EQ(redirect->group_id, replicated_tablet_group());
        EXPECT_GE(redirect->leader_node_id, 1U);
        EXPECT_LE(redirect->leader_node_id, clients.size());
        continue;
      }
      if (response.header.message_type == network::MessageType::kError) {
        EXPECT_TRUE(network::decode_error_message(response.payload).has_value());
        continue;
      }
      ADD_FAILURE() << "unexpected replicated ingest response type "
                    << static_cast<unsigned int>(response.header.message_type);
      return std::nullopt;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{25});
  }
  ADD_FAILURE() << "replicated ingest did not reach a leader before the attempt bound";
  return std::nullopt;
}

void expect_recovered_replicated_cluster(const std::array<std::string, 3U>& roots) {
  for (const std::string& root : roots) {
    auto database = service::ReplicatedIngestDatabase::open_existing(
        {.bootstrap = {.database_root = root}, .groups = replicated_cluster_groups()});
    ASSERT_TRUE(database.has_value()) << database.error().to_string();
    auto tablet =
        database->ingest_runtime()->tablet_application()->snapshot(replicated_tablet_group());
    ASSERT_TRUE(tablet.has_value()) << tablet.error().to_string();
    EXPECT_EQ(tablet->visible_row_count(), 2U);
    EXPECT_EQ(tablet->retry_entry_count(), 1U);
    EXPECT_TRUE(database->shutdown().is_ok());
  }
}

[[nodiscard]] network::Frame send_replicated_query(const int client, const std::uint64_t request_id,
                                                   const std::string_view sql) {
  const auto payload = network::encode_query_request(sql).value();
  EXPECT_TRUE(
      send_all(client, network::encode_frame({.protocol_major = network::kProtocolV2Major,
                                              .protocol_minor = network::kProtocolV2LatestMinor,
                                              .message_type = network::MessageType::kQueryRequest,
                                              .request_id = request_id},
                                             payload)
                           .value()));
  auto response = network::decode_frame(receive_frame(client));
  EXPECT_TRUE(response.has_value());
  return response.value_or(network::Frame{});
}

[[nodiscard]] network::Frame send_query(const int client, const std::uint64_t request_id,
                                        const std::string_view sql) {
  const auto payload = network::encode_query_request(sql).value();
  EXPECT_TRUE(send_all(
      client,
      network::encode_frame(
          {.message_type = network::MessageType::kQueryRequest, .request_id = request_id}, payload)
          .value()));
  auto response = network::decode_frame(receive_frame(client));
  EXPECT_TRUE(response.has_value());
  return response.value_or(network::Frame{});
}

TEST(ChronosdProcessTest, NegotiatesPongsAndRejectsUnconfiguredDataPlane) {
  ChildProcess child;
  ASSERT_TRUE(child.start());
  const std::string startup = child.read_startup_line();
  EXPECT_NE(startup.find("data_plane=unconfigured"), std::string::npos);
  const std::uint16_t port = parse_port(startup);
  ASSERT_NE(port, 0U);

  const int client = connect_client(port);
  ASSERT_GE(client, 0);
  handshake(client);

  ASSERT_TRUE(send_all(
      client, network::encode_frame({.message_type = network::MessageType::kPing}, {}).value()));
  auto response = network::decode_frame(receive_frame(client));
  ASSERT_TRUE(response.has_value());
  EXPECT_EQ(response->header.message_type, network::MessageType::kPong);

  const auto query_payload = network::encode_query_request("SELECT 1").value();
  ASSERT_TRUE(send_all(
      client,
      network::encode_frame({.message_type = network::MessageType::kQueryRequest, .request_id = 1U},
                            query_payload)
          .value()));
  response = network::decode_frame(receive_frame(client));
  ASSERT_TRUE(response.has_value());
  ASSERT_EQ(response->header.message_type, network::MessageType::kError);
  const auto error = network::decode_error_message(response->payload);
  ASSERT_TRUE(error.has_value());
  EXPECT_EQ(error->code, network::ProtocolErrorCode::kExecutionFailure);
  EXPECT_EQ(byte_string(error->message), "chronosd data plane is not configured");

  ::close(client);
  EXPECT_EQ(child.stop(), 0);
}

TEST(ChronosdProcessTest, CreatesQueriesAndRecoversAConfiguredDatabase) {
  constexpr std::string_view create_sql =
      "CREATE TABLE trades (ts TIMESTAMP_NS NOT NULL, symbol SYMBOL NOT NULL, price "
      "DECIMAL(20, 8) NOT NULL, note STRING) EVENT TIME ts ORDER KEY (symbol, ts) "
      "PARTITION BY time_bucket(INTERVAL '1 day', ts) SHARD KEY (symbol) DEDUP KEY "
      "(symbol, ts) RETENTION INTERVAL '30 days' SYSTEM HISTORY RETENTION INTERVAL "
      "'7 days' ALLOWED LATENESS INTERVAL '0 seconds'";
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  ChildProcess child;
  ASSERT_TRUE(child.start(directory.path()));
  std::string startup = child.read_startup_line();
  EXPECT_NE(startup.find("data_plane=configured"), std::string::npos);
  std::uint16_t port = parse_port(startup);
  ASSERT_NE(port, 0U);

  int client = connect_client(port);
  ASSERT_GE(client, 0);
  handshake(client);
  auto response = send_query(client, 1U, create_sql);
  ASSERT_EQ(response.header.message_type, network::MessageType::kQueryResult);
  auto ddl = network::decode_query_result_batch(response.payload);
  ASSERT_TRUE(ddl.has_value()) << ddl.error().to_string();
  EXPECT_EQ(ddl->row_count(), 1U);
  EXPECT_EQ(ddl->columns().size(), 5U);
  response = network::decode_frame(receive_frame(client)).value();
  EXPECT_EQ(response.header.message_type, network::MessageType::kQueryEnd);

  response = send_query(client, 2U, "SELECT count(*) AS rows FROM trades");
  ASSERT_EQ(response.header.message_type, network::MessageType::kQueryResult);
  auto count = network::decode_query_result_batch(response.payload);
  ASSERT_TRUE(count.has_value()) << count.error().to_string();
  ASSERT_NE(count->cell(0U, 0U), nullptr);
  common::ByteReader zero{count->cell(0U, 0U)->value};
  EXPECT_EQ(zero.read_i64_le().value(), 0);
  response = network::decode_frame(receive_frame(client)).value();
  EXPECT_EQ(response.header.message_type, network::MessageType::kQueryEnd);
  ::close(client);
  EXPECT_EQ(child.stop(), 0);

  ASSERT_TRUE(child.start(directory.path()));
  startup = child.read_startup_line();
  EXPECT_NE(startup.find("data_plane=configured"), std::string::npos);
  port = parse_port(startup);
  ASSERT_NE(port, 0U);
  client = connect_client(port);
  ASSERT_GE(client, 0);
  handshake(client);
  response = send_query(client, 3U, "SELECT count(*) AS rows FROM trades");
  ASSERT_EQ(response.header.message_type, network::MessageType::kQueryResult);
  count = network::decode_query_result_batch(response.payload);
  ASSERT_TRUE(count.has_value()) << count.error().to_string();
  common::ByteReader recovered{count->cell(0U, 0U)->value};
  EXPECT_EQ(recovered.read_i64_le().value(), 0);
  response = network::decode_frame(receive_frame(client)).value();
  EXPECT_EQ(response.header.message_type, network::MessageType::kQueryEnd);
  ::close(client);
  EXPECT_EQ(child.stop(), 0);
}

TEST(ChronosdProcessTest, AppliesAndRecoversQuorumSyncThroughReplicatedDaemonMode) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  provision_replicated_database(directory.path());
  const std::string groups = write_replicated_groups(directory.path());
  ASSERT_FALSE(groups.empty());

  ChildProcess child;
  ASSERT_TRUE(child.start(directory.path(), {}, {}, groups));
  std::string startup = child.read_startup_line();
  EXPECT_NE(startup.find("data_plane=replicated"), std::string::npos);
  EXPECT_NE(startup.find("raft_transport=local"), std::string::npos);
  std::uint16_t port = parse_port(startup);
  ASSERT_NE(port, 0U);
  int client = connect_client(port);
  ASSERT_GE(client, 0);
  handshake_quorum_sync(client);
  auto response = send_replicated_ingest(client);
  ASSERT_EQ(response.header.message_type, network::MessageType::kQuorumSyncIngestAcknowledgement);
  auto acknowledgement = network::decode_quorum_sync_ingest_acknowledgement(response.payload);
  ASSERT_TRUE(acknowledgement.has_value()) << acknowledgement.error().to_string();
  EXPECT_EQ(acknowledgement->outcome, network::IngestOutcome::kApplied);
  EXPECT_EQ(acknowledgement->group_id, replicated_tablet_group());
  response = send_replicated_query(client, 2U, "SELECT count(*) AS rows FROM events");
  ASSERT_EQ(response.header.message_type, network::MessageType::kQueryResult);
  auto count = network::decode_query_result_batch(response.payload);
  ASSERT_TRUE(count.has_value()) << count.error().to_string();
  common::ByteReader applied_count{count->cell(0U, 0U)->value};
  EXPECT_EQ(applied_count.read_i64_le().value(), 2);
  response = network::decode_frame(receive_frame(client)).value();
  EXPECT_EQ(response.header.message_type, network::MessageType::kQueryEnd);
  ::close(client);
  ASSERT_EQ(child.stop(), 0);

  ASSERT_TRUE(child.start(directory.path(), {}, {}, groups));
  startup = child.read_startup_line();
  EXPECT_NE(startup.find("data_plane=replicated"), std::string::npos);
  EXPECT_NE(startup.find("raft_transport=local"), std::string::npos);
  port = parse_port(startup);
  ASSERT_NE(port, 0U);
  client = connect_client(port);
  ASSERT_GE(client, 0);
  handshake_quorum_sync(client);
  response = send_replicated_ingest(client);
  ASSERT_EQ(response.header.message_type, network::MessageType::kQuorumSyncIngestAcknowledgement);
  acknowledgement = network::decode_quorum_sync_ingest_acknowledgement(response.payload);
  ASSERT_TRUE(acknowledgement.has_value()) << acknowledgement.error().to_string();
  EXPECT_EQ(acknowledgement->outcome, network::IngestOutcome::kMatchingRetry);
  EXPECT_EQ(acknowledgement->group_id, replicated_tablet_group());
  response = send_replicated_query(client, 3U, "SELECT count(*) AS rows FROM events");
  ASSERT_EQ(response.header.message_type, network::MessageType::kQueryResult);
  count = network::decode_query_result_batch(response.payload);
  ASSERT_TRUE(count.has_value()) << count.error().to_string();
  common::ByteReader recovered_count{count->cell(0U, 0U)->value};
  EXPECT_EQ(recovered_count.read_i64_le().value(), 2);
  response = network::decode_frame(receive_frame(client)).value();
  EXPECT_EQ(response.header.message_type, network::MessageType::kQueryEnd);
  ::close(client);
  EXPECT_EQ(child.stop(), 0);
}

TEST(ChronosdProcessTest, ReplicatesRetriesAndFailsOverAcrossThreeAuthenticatedDaemons) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const std::array<std::string, 3U> roots{
      directory.path() + "/node-1", directory.path() + "/node-2", directory.path() + "/node-3"};
  provision_replicated_cluster(roots);

  std::array<std::uint16_t, 3U> raft_ports{};
  for (std::size_t index = 0U; index < raft_ports.size(); ++index) {
    do {
      raft_ports[index] = reserve_loopback_port();
    } while (raft_ports[index] != 0U &&
             std::ranges::find(
                 raft_ports.begin(), raft_ports.begin() + static_cast<std::ptrdiff_t>(index),
                 raft_ports[index]) != raft_ports.begin() + static_cast<std::ptrdiff_t>(index));
    ASSERT_NE(raft_ports[index], 0U);
  }
  const ReplicatedClusterFiles files = write_replicated_cluster_files(directory.path(), raft_ports);
  ASSERT_FALSE(files.groups.empty());
  ASSERT_FALSE(files.peers.empty());

  std::array<ChildProcess, 3U> children;
  std::array<int, 3U> clients{-1, -1, -1};
  std::array<bool, 3U> active{true, true, true};
  std::array<std::uint64_t, 3U> request_ids{1U, 1U, 1U};
  for (std::size_t index = 0U; index < children.size(); ++index) {
    ASSERT_TRUE(children[index].start_replicated_transport(
        roots[index], files.groups, files.peers, files.certificates[index],
        files.private_keys[index], files.trust_store));
    const std::string startup = children[index].read_startup_line();
    EXPECT_NE(startup.find("data_plane=replicated"), std::string::npos) << startup;
    EXPECT_NE(startup.find("raft_transport=configured"), std::string::npos) << startup;
    const std::uint16_t port = parse_port(startup);
    ASSERT_NE(port, 0U) << startup;
    clients[index] = connect_client(port);
    ASSERT_GE(clients[index], 0);
    handshake_quorum_sync(clients[index]);
  }

  auto first = await_cluster_ingest(clients, active, request_ids);
  if (!first.has_value()) {
    ADD_FAILURE() << "first replicated ingest did not complete";
    return;
  }
  const ClusterIngestAcknowledgement first_result = *first;
  EXPECT_EQ(first_result.acknowledgement.outcome, network::IngestOutcome::kApplied);
  EXPECT_EQ(first_result.acknowledgement.group_id, replicated_tablet_group());
  EXPECT_EQ(first_result.acknowledgement.leader_node_id, first_result.node_index + 1U);
  EXPECT_GT(first_result.acknowledgement.leader_term, 0U);
  EXPECT_GT(first_result.acknowledgement.log_index, 0U);

  const std::size_t failed_leader = first_result.node_index;
  ::close(clients[failed_leader]);
  clients[failed_leader] = -1;
  active[failed_leader] = false;
  ASSERT_TRUE(children[failed_leader].kill_abruptly());

  auto retried = await_cluster_ingest(clients, active, request_ids);
  if (!retried.has_value()) {
    ADD_FAILURE() << "retry after leader loss did not complete";
    return;
  }
  const ClusterIngestAcknowledgement retry_result = *retried;
  EXPECT_NE(retry_result.node_index, failed_leader);
  EXPECT_EQ(retry_result.acknowledgement.outcome, network::IngestOutcome::kMatchingRetry);
  EXPECT_EQ(retry_result.acknowledgement.group_id, replicated_tablet_group());
  EXPECT_EQ(retry_result.acknowledgement.leader_node_id, retry_result.node_index + 1U);
  EXPECT_GT(retry_result.acknowledgement.leader_term, first_result.acknowledgement.leader_term);
  EXPECT_GT(retry_result.acknowledgement.log_index, first_result.acknowledgement.log_index);
  EXPECT_EQ(retry_result.acknowledgement.entry_term, retry_result.acknowledgement.leader_term);
  EXPECT_GT(retry_result.acknowledgement.entry_term, first_result.acknowledgement.entry_term);

  for (std::size_t index = 0U; index < children.size(); ++index) {
    if (!active[index])
      continue;
    ::close(clients[index]);
    clients[index] = -1;
    EXPECT_EQ(children[index].stop(), 0);
  }
  expect_recovered_replicated_cluster(roots);
}

TEST(ChronosdProcessTest, StreamsAcknowledgesAndResumesAConfiguredSubscription) {
  constexpr std::string_view create_sql =
      "CREATE TABLE trades (ts TIMESTAMP_NS NOT NULL, symbol SYMBOL NOT NULL, price "
      "DECIMAL(20, 8) NOT NULL, note STRING) EVENT TIME ts ORDER KEY (symbol, ts) "
      "PARTITION BY time_bucket(INTERVAL '1 day', ts) SHARD KEY (symbol) DEDUP KEY "
      "(symbol, ts) RETENTION INTERVAL '30 days' SYSTEM HISTORY RETENTION INTERVAL "
      "'7 days' ALLOWED LATENESS INTERVAL '0 seconds'";
  constexpr std::string_view subscription_sql = "SUBSCRIBE SELECT ts FROM trades";
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  ChildProcess child;
  ASSERT_TRUE(child.start(directory.path()));
  std::uint16_t port = parse_port(child.read_startup_line());
  ASSERT_NE(port, 0U);
  int client = connect_client(port);
  ASSERT_GE(client, 0);
  handshake(client);
  auto response = send_query(client, 1U, create_sql);
  ASSERT_EQ(response.header.message_type, network::MessageType::kQueryResult);
  response = network::decode_frame(receive_frame(client)).value();
  ASSERT_EQ(response.header.message_type, network::MessageType::kQueryEnd);
  ::close(client);
  ASSERT_EQ(child.stop(), 0);

  const std::string key_path = directory.path() + "/subscription.key";
  const int key_file = ::open(key_path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  ASSERT_GE(key_file, 0);
  std::array<std::byte, 32U> key{};
  key.fill(std::byte{0x5a});
  ASSERT_EQ(::write(key_file, key.data(), key.size()), static_cast<ssize_t>(key.size()));
  ASSERT_EQ(::fsync(key_file), 0);
  ASSERT_EQ(::close(key_file), 0);

  ASSERT_TRUE(child.start(directory.path(), std::string{subscription_sql}, key_path));
  const std::string startup = child.read_startup_line();
  EXPECT_NE(startup.find("subscriptions=configured"), std::string::npos);
  port = parse_port(startup);
  ASSERT_NE(port, 0U);
  client = connect_client(port);
  ASSERT_GE(client, 0);
  handshake_subscriptions(client);

  common::Uuid::Bytes subscription_bytes{};
  subscription_bytes.front() = std::byte{1};
  const common::Uuid subscription_id{subscription_bytes};
  auto subscribe = network::encode_subscription_request(
      {.mode = network::SubscriptionStartMode::kNewQuery,
       .subscription_id = subscription_id,
       .body = std::as_bytes(std::span{subscription_sql.data(), subscription_sql.size()})});
  ASSERT_TRUE(subscribe.has_value());
  ASSERT_TRUE(send_all(
      client,
      network::encode_frame(
          {.message_type = network::MessageType::kSubscribeRequest, .request_id = 10U}, *subscribe)
          .value()));
  response = network::decode_frame(receive_frame(client)).value();
  ASSERT_EQ(response.header.message_type, network::MessageType::kQueryResult);
  EXPECT_NE(response.header.flags & network::kFrameFlagEndStream, 0U);
  response = network::decode_frame(receive_frame(client)).value();
  ASSERT_EQ(response.header.message_type, network::MessageType::kSubscriptionReady);

  response = send_query(client, 11U,
                        "INSERT INTO trades VALUES "
                        "(TIMESTAMP '1970-01-01 00:00:00.000000001Z', "
                        "CAST('A' AS SYMBOL), 1, NULL)");
  ASSERT_EQ(response.header.message_type, network::MessageType::kQueryResult);
  response = network::decode_frame(receive_frame(client)).value();
  ASSERT_EQ(response.header.message_type, network::MessageType::kQueryEnd);
  response = network::decode_frame(receive_frame(client)).value();
  ASSERT_EQ(response.header.message_type, network::MessageType::kSubscriptionChange);
  auto change = network::decode_subscription_change(response.payload);
  ASSERT_TRUE(change.has_value()) << change.error().to_string();
  EXPECT_EQ(change->delivery_sequence, 1U);
  auto nested = network::decode_query_result_batch(change->payload);
  ASSERT_TRUE(nested.has_value()) << nested.error().to_string();
  EXPECT_EQ(nested->row_count(), 1U);

  auto acknowledgement = network::encode_subscription_acknowledgement({1U});
  ASSERT_TRUE(acknowledgement.has_value());
  ASSERT_TRUE(send_all(
      client, network::encode_frame({.message_type = network::MessageType::kSubscriptionAcknowledge,
                                     .request_id = 10U},
                                    *acknowledgement)
                  .value()));
  response = network::decode_frame(receive_frame(client)).value();
  ASSERT_EQ(response.header.message_type, network::MessageType::kSubscriptionCheckpoint);
  auto checkpoint = network::decode_subscription_checkpoint(response.payload);
  ASSERT_TRUE(checkpoint.has_value());
  std::vector<std::byte> resume_token{checkpoint->resume_token.begin(),
                                      checkpoint->resume_token.end()};
  ::close(client);
  ASSERT_EQ(child.stop(), 0);

  ASSERT_TRUE(child.start(directory.path(), std::string{subscription_sql}, key_path));
  port = parse_port(child.read_startup_line());
  ASSERT_NE(port, 0U);
  client = connect_client(port);
  ASSERT_GE(client, 0);
  handshake_subscriptions(client);
  auto resume =
      network::encode_subscription_request({.mode = network::SubscriptionStartMode::kResume,
                                            .subscription_id = subscription_id,
                                            .body = resume_token});
  ASSERT_TRUE(resume.has_value());
  ASSERT_TRUE(send_all(
      client,
      network::encode_frame(
          {.message_type = network::MessageType::kSubscribeRequest, .request_id = 20U}, *resume)
          .value()));
  response = network::decode_frame(receive_frame(client)).value();
  ASSERT_EQ(response.header.message_type, network::MessageType::kQueryResult);
  EXPECT_NE(response.header.flags & network::kFrameFlagEndStream, 0U);
  response = network::decode_frame(receive_frame(client)).value();
  EXPECT_EQ(response.header.message_type, network::MessageType::kSubscriptionReady);
  ::close(client);
  EXPECT_EQ(child.stop(), 0);
}

} // namespace
} // namespace chronos::integration
