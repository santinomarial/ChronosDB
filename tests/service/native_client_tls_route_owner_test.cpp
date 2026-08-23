#include "chronos/network/tls_socket.hpp"
#include "chronos/service/native_client_tls_route_owner.hpp"

#include <array>
#include <cstddef>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <sys/socket.h>
#include <system_error>
#include <unistd.h>

namespace chronos::service {
namespace {

constexpr const char* kServerFingerprint =
    "e79120b0ee5e55f91ea4cb4a29d3ca20aaa36a4abdbeb74d06f97751e61368d1";

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-native-client-tls-XXXXXX").string();
    if (char* created = ::mkdtemp(pattern.data()); created != nullptr)
      path_ = created;
  }
  ~TemporaryDirectory() {
    std::error_code ignored;
    std::filesystem::remove_all(path_, ignored);
  }
  [[nodiscard]] const std::filesystem::path& path() const noexcept {
    return path_;
  }

private:
  std::filesystem::path path_;
};

struct SocketPair {
  std::array<int, 2> sockets{-1, -1};
  ~SocketPair() {
    for (const int socket : sockets) {
      if (socket >= 0)
        static_cast<void>(::close(socket));
    }
  }
};

[[nodiscard]] std::filesystem::path fixture(const char* name) {
  return std::filesystem::path{CHRONOS_NETWORK_FIXTURE_DIR} / "tls" / name;
}

[[nodiscard]] std::filesystem::path write_route_file(const TemporaryDirectory& directory,
                                                     const char* name = "native-routes.conf") {
  const std::filesystem::path path = directory.path() / name;
  std::ofstream stream{path, std::ios::binary | std::ios::trunc};
  stream << "CHRONOSDB_NATIVE_CLIENT_ROUTES_V1\n"
            "7=127.0.0.1:7421,127.0.0.1,"
         << kServerFingerprint << '\n';
  stream.close();
  EXPECT_TRUE(stream);
  return path;
}

[[nodiscard]] std::filesystem::path copy_private_key(const TemporaryDirectory& directory,
                                                     const char* name,
                                                     const std::filesystem::perms permissions) {
  const std::filesystem::path path = directory.path() / name;
  std::error_code error;
  std::filesystem::copy_file(fixture("client-key.pem"), path,
                             std::filesystem::copy_options::overwrite_existing, error);
  EXPECT_FALSE(error) << error.message();
  std::filesystem::permissions(path, permissions, std::filesystem::perm_options::replace, error);
  EXPECT_FALSE(error) << error.message();
  return path;
}

[[nodiscard]] NativeClientTlsRouteOwnerConfig
owner_config(const std::filesystem::path& route_file, const std::filesystem::path& private_key) {
  return {.route_config_file = route_file.string(),
          .tls = {.certificate_chain_file = fixture("client.pem").string(),
                  .private_key_file = private_key.string(),
                  .trust_store_file = fixture("ca.pem").string()}};
}

[[nodiscard]] SocketPair nonblocking_socket_pair() {
  SocketPair pair;
  EXPECT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, pair.sockets.data()), 0);
  for (const int socket : pair.sockets) {
    const int flags = ::fcntl(socket, F_GETFL, 0);
    EXPECT_GE(flags, 0);
    EXPECT_EQ(::fcntl(socket, F_SETFL, flags | O_NONBLOCK), 0);
  }
  return pair;
}

void complete_handshake(network::TlsSocket& server, network::TlsSocket& client) {
  for (std::size_t attempt = 0U; attempt < 1024U; ++attempt) {
    if (!server.handshake_complete()) {
      auto progress = server.handshake();
      ASSERT_TRUE(progress.has_value()) << progress.error().to_string();
    }
    if (!client.handshake_complete()) {
      auto progress = client.handshake();
      ASSERT_TRUE(progress.has_value()) << progress.error().to_string();
    }
    if (server.handshake_complete() && client.handshake_complete())
      return;
  }
  FAIL() << "native client TLS route handshake did not converge";
}

TEST(NativeClientTlsRouteOwnerTest, LoadsStableRoutesAndAuthenticatesARealMutualTlsServer) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const auto route_file = write_route_file(directory);
  const auto private_key =
      copy_private_key(directory, "client-key.pem",
                       std::filesystem::perms::owner_read | std::filesystem::perms::owner_write);
  auto owner = NativeClientTlsRouteOwner::load(owner_config(route_file, private_key));
  ASSERT_TRUE(owner.has_value()) << owner.error().to_string();
  ASSERT_EQ(owner->configured_routes().size(), 1U);
  ASSERT_EQ(owner->leader_routes().size(), 1U);
  EXPECT_EQ(owner->configured_routes()[0].node_id, 7U);
  EXPECT_EQ(owner->leader_routes()[0].node_id, 7U);
  EXPECT_EQ(owner->leader_routes()[0].endpoint, owner->configured_routes()[0].endpoint);
  ASSERT_NE(owner->leader_routes()[0].tls_context, nullptr);
  const network::TlsClientContext* stable_context = owner->leader_routes()[0].tls_context;

  NativeClientTlsRouteOwner moved = std::move(*owner);
  EXPECT_TRUE(owner->configured_routes().empty());
  EXPECT_TRUE(owner->leader_routes().empty());
  ASSERT_EQ(moved.leader_routes().size(), 1U);
  EXPECT_EQ(moved.leader_routes()[0].tls_context, stable_context);

  auto server_context =
      network::TlsServerContext::create({.certificate_chain_file = fixture("server.pem").string(),
                                         .private_key_file = fixture("server-key.pem").string(),
                                         .trust_store_file = fixture("ca.pem").string()});
  ASSERT_TRUE(server_context.has_value()) << server_context.error().to_string();
  SocketPair sockets = nonblocking_socket_pair();
  auto server = network::TlsSocket::accept(*server_context, sockets.sockets[0]);
  auto client = network::TlsSocket::connect(*stable_context, sockets.sockets[1]);
  ASSERT_TRUE(server.has_value()) << server.error().to_string();
  ASSERT_TRUE(client.has_value()) << client.error().to_string();
  complete_handshake(*server, *client);
  auto fingerprint = client->peer_certificate_sha256();
  ASSERT_TRUE(fingerprint.has_value()) << fingerprint.error().to_string();
  EXPECT_EQ(*fingerprint, moved.configured_routes()[0].certificate_sha256);
  auto authenticated =
      moved.authority().authenticate({.ipv4_address = moved.configured_routes()[0].endpoint.address,
                                      .transport_authenticated = true,
                                      .peer_certificate_sha256 = *fingerprint});
  ASSERT_TRUE(authenticated.has_value()) << authenticated.error().to_string();
  EXPECT_TRUE(authenticated->authorized);
  EXPECT_EQ(authenticated->principal_id, 7U);
  EXPECT_TRUE(*moved.authority().authorize_node(authenticated->principal_id, 7U));
}

TEST(NativeClientTlsRouteOwnerTest, RejectsUnsafeFilesBoundsAndInvalidCredentials) {
  TemporaryDirectory directory;
  ASSERT_FALSE(directory.path().empty());
  const auto route_file = write_route_file(directory);
  const auto private_key =
      copy_private_key(directory, "private-key.pem",
                       std::filesystem::perms::owner_read | std::filesystem::perms::owner_write);

  const auto insecure_key =
      copy_private_key(directory, "insecure-key.pem",
                       std::filesystem::perms::owner_read | std::filesystem::perms::owner_write |
                           std::filesystem::perms::group_read);
  auto insecure = NativeClientTlsRouteOwner::load(owner_config(route_file, insecure_key));
  ASSERT_FALSE(insecure.has_value());
  EXPECT_EQ(insecure.error().code(), common::StatusCode::kInvalidArgument);

  std::error_code error;
  std::filesystem::permissions(route_file,
                               std::filesystem::perms::owner_read |
                                   std::filesystem::perms::owner_write |
                                   std::filesystem::perms::group_write,
                               std::filesystem::perm_options::replace, error);
  ASSERT_FALSE(error) << error.message();
  auto writable_route = NativeClientTlsRouteOwner::load(owner_config(route_file, private_key));
  ASSERT_FALSE(writable_route.has_value());
  EXPECT_EQ(writable_route.error().code(), common::StatusCode::kInvalidArgument);
  std::filesystem::permissions(
      route_file, std::filesystem::perms::owner_read | std::filesystem::perms::owner_write,
      std::filesystem::perm_options::replace, error);
  ASSERT_FALSE(error) << error.message();

  const auto route_link = directory.path() / "route-link";
  std::filesystem::create_symlink(route_file, route_link, error);
  ASSERT_FALSE(error) << error.message();
  auto linked_route = NativeClientTlsRouteOwner::load(owner_config(route_link, private_key));
  ASSERT_FALSE(linked_route.has_value());
  EXPECT_EQ(linked_route.error().code(), common::StatusCode::kIoError);

  const auto key_link = directory.path() / "key-link";
  std::filesystem::create_symlink(private_key, key_link, error);
  ASSERT_FALSE(error) << error.message();
  auto linked_key = NativeClientTlsRouteOwner::load(owner_config(route_file, key_link));
  ASSERT_FALSE(linked_key.has_value());
  EXPECT_EQ(linked_key.error().code(), common::StatusCode::kIoError);

  const auto writable_trust = directory.path() / "writable-ca.pem";
  std::filesystem::copy_file(fixture("ca.pem"), writable_trust,
                             std::filesystem::copy_options::overwrite_existing, error);
  ASSERT_FALSE(error) << error.message();
  std::filesystem::permissions(writable_trust,
                               std::filesystem::perms::owner_read |
                                   std::filesystem::perms::owner_write |
                                   std::filesystem::perms::group_write,
                               std::filesystem::perm_options::replace, error);
  ASSERT_FALSE(error) << error.message();
  auto writable_trust_config = owner_config(route_file, private_key);
  writable_trust_config.tls.trust_store_file = writable_trust.string();
  auto mutable_trust = NativeClientTlsRouteOwner::load(writable_trust_config);
  ASSERT_FALSE(mutable_trust.has_value());
  EXPECT_EQ(mutable_trust.error().code(), common::StatusCode::kInvalidArgument);

  auto tiny = owner_config(route_file, private_key);
  tiny.limits.route_config.maximum_bytes = 8U;
  auto bounded = NativeClientTlsRouteOwner::load(tiny);
  ASSERT_FALSE(bounded.has_value());
  EXPECT_EQ(bounded.error().code(), common::StatusCode::kResourceExhausted);

  auto excessive_tls_limit = owner_config(route_file, private_key);
  excessive_tls_limit.limits.maximum_tls_file_bytes = std::size_t{65U} * 1024U * 1024U;
  auto excessive = NativeClientTlsRouteOwner::load(excessive_tls_limit);
  ASSERT_FALSE(excessive.has_value());
  EXPECT_EQ(excessive.error().code(), common::StatusCode::kInvalidArgument);

  auto invalid_certificate = owner_config(route_file, private_key);
  invalid_certificate.tls.certificate_chain_file = route_file.string();
  auto invalid_tls = NativeClientTlsRouteOwner::load(invalid_certificate);
  ASSERT_FALSE(invalid_tls.has_value());
  EXPECT_EQ(invalid_tls.error().code(), common::StatusCode::kUnauthenticated);

  auto empty = owner_config(route_file, private_key);
  empty.tls.trust_store_file.clear();
  auto incomplete = NativeClientTlsRouteOwner::load(empty);
  ASSERT_FALSE(incomplete.has_value());
  EXPECT_EQ(incomplete.error().code(), common::StatusCode::kInvalidArgument);
}

} // namespace
} // namespace chronos::service
