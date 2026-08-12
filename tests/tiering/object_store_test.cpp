#include "chronos/ingest/sha256.hpp"
#include "chronos/tiering/object_store.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <map>
#include <mutex>
#include <netinet/in.h>
#include <optional>
#include <poll.h>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::tiering {
namespace {

[[nodiscard]] std::string lower(std::string value) {
  std::ranges::transform(value, value.begin(), [](const unsigned char character) {
    return static_cast<char>(character >= 'A' && character <= 'Z' ? character + ('a' - 'A')
                                                                  : character);
  });
  return value;
}

struct RecordedRequest {
  std::string method;
  std::string target;
  std::map<std::string, std::string> headers;
  std::vector<std::byte> body;
};

struct LocalS3Behavior {
  std::string access_key_id{"test-access"};
  std::optional<std::string> session_token{"test-token"};
  std::size_t transient_put_failures{};
};

class LocalS3Server final {
public:
  explicit LocalS3Server(LocalS3Behavior behavior = {}) : behavior_(std::move(behavior)) {
    listener_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listener_ < 0) {
      failure_ = "socket creation failed";
      return;
    }
    const int reuse = 1;
    static_cast<void>(::setsockopt(listener_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0U;
    if (::bind(listener_, reinterpret_cast<const sockaddr*>(&address), sizeof(address)) != 0 ||
        ::listen(listener_, 8) != 0) {
      failure_ = "listener bind failed";
      ::close(listener_);
      listener_ = -1;
      return;
    }
    socklen_t address_length = sizeof(address);
    if (::getsockname(listener_, reinterpret_cast<sockaddr*>(&address), &address_length) != 0) {
      failure_ = "listener endpoint lookup failed";
      ::close(listener_);
      listener_ = -1;
      return;
    }
    port_ = ntohs(address.sin_port);
    worker_ = std::jthread{[this](const std::stop_token stop) { serve(stop); }};
  }

  ~LocalS3Server() {
    worker_.request_stop();
    if (listener_ >= 0) {
      static_cast<void>(::shutdown(listener_, SHUT_RDWR));
      ::close(listener_);
      if (worker_.joinable())
        worker_.join();
      listener_ = -1;
    }
  }

  LocalS3Server(const LocalS3Server&) = delete;
  LocalS3Server& operator=(const LocalS3Server&) = delete;

  [[nodiscard]] bool valid() const noexcept {
    return listener_ >= 0;
  }

  [[nodiscard]] std::uint16_t port() const noexcept {
    return port_;
  }

  [[nodiscard]] std::vector<RecordedRequest> requests() const {
    std::scoped_lock lock{mutex_};
    return requests_;
  }

  [[nodiscard]] std::string failure() const {
    std::scoped_lock lock{mutex_};
    return failure_;
  }

private:
  [[nodiscard]] static bool send_all(const int descriptor, const std::string_view bytes) {
    std::size_t sent{};
    while (sent < bytes.size()) {
      const ssize_t result = ::send(descriptor, bytes.data() + sent, bytes.size() - sent, 0);
      if (result <= 0)
        return false;
      sent += static_cast<std::size_t>(result);
    }
    return true;
  }

  [[nodiscard]] static std::optional<RecordedRequest> read_request(const int descriptor) {
    std::vector<char> bytes;
    bytes.reserve(4096U);
    std::array<char, 2048U> fragment{};
    std::size_t header_end = std::string::npos;
    std::size_t content_length{};
    while (bytes.size() <= 64U * 1024U) {
      const ssize_t received = ::recv(descriptor, fragment.data(), fragment.size(), 0);
      if (received <= 0)
        return std::nullopt;
      bytes.insert(bytes.end(), fragment.begin(), fragment.begin() + received);
      const std::string_view view{bytes.data(), bytes.size()};
      header_end = view.find("\r\n\r\n");
      if (header_end == std::string::npos)
        continue;

      RecordedRequest request;
      const std::size_t request_line_end = view.find("\r\n");
      if (request_line_end == std::string::npos)
        return std::nullopt;
      const std::string_view request_line = view.substr(0U, request_line_end);
      const std::size_t first_space = request_line.find(' ');
      const std::size_t second_space = request_line.find(' ', first_space + 1U);
      if (first_space == std::string::npos || second_space == std::string::npos)
        return std::nullopt;
      request.method = request_line.substr(0U, first_space);
      request.target = request_line.substr(first_space + 1U, second_space - first_space - 1U);

      std::size_t line_begin = request_line_end + 2U;
      while (line_begin < header_end) {
        const std::size_t line_end = view.find("\r\n", line_begin);
        if (line_end == std::string::npos || line_end > header_end)
          return std::nullopt;
        const std::string_view line = view.substr(line_begin, line_end - line_begin);
        const std::size_t colon = line.find(':');
        if (colon == std::string::npos)
          return std::nullopt;
        std::string name = lower(std::string{line.substr(0U, colon)});
        std::string_view value = line.substr(colon + 1U);
        while (!value.empty() && (value.front() == ' ' || value.front() == '\t'))
          value.remove_prefix(1U);
        request.headers.emplace(std::move(name), std::string{value});
        line_begin = line_end + 2U;
      }
      const auto length = request.headers.find("content-length");
      if (length != request.headers.end()) {
        try {
          content_length = std::stoull(length->second);
        } catch (...) {
          return std::nullopt;
        }
      }
      const std::size_t body_begin = header_end + 4U;
      while (bytes.size() - body_begin < content_length) {
        const ssize_t body_received = ::recv(descriptor, fragment.data(), fragment.size(), 0);
        if (body_received <= 0)
          return std::nullopt;
        bytes.insert(bytes.end(), fragment.begin(), fragment.begin() + body_received);
      }
      const auto* body = reinterpret_cast<const std::byte*>(bytes.data() + body_begin);
      request.body.assign(body, body + content_length);
      return request;
    }
    return std::nullopt;
  }

  void record_failure(std::string message) {
    std::scoped_lock lock{mutex_};
    if (failure_.empty())
      failure_ = std::move(message);
  }

  void handle(const int descriptor) {
    auto request = read_request(descriptor);
    if (!request.has_value()) {
      record_failure("request parsing failed");
      return;
    }
    {
      std::scoped_lock lock{mutex_};
      requests_.push_back(*request);
    }
    const auto authorization = request->headers.find("authorization");
    const auto signed_date = request->headers.find("x-amz-date");
    const auto token = request->headers.find("x-amz-security-token");
    const bool token_matches =
        behavior_.session_token.has_value()
            ? token != request->headers.end() && token->second == *behavior_.session_token
            : token == request->headers.end();
    if (authorization == request->headers.end() ||
        !authorization->second.starts_with("AWS4-HMAC-SHA256 ") ||
        !authorization->second.contains("Credential=" + behavior_.access_key_id + "/") ||
        !authorization->second.contains("SignedHeaders=") ||
        signed_date == request->headers.end() || !token_matches) {
      static_cast<void>(send_all(
          descriptor, "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"));
      return;
    }

    if (request->method == "PUT") {
      if (behavior_.transient_put_failures > 0U) {
        --behavior_.transient_put_failures;
        static_cast<void>(send_all(descriptor,
                                   "HTTP/1.1 503 Service Unavailable\r\nContent-Length: "
                                   "0\r\nConnection: close\r\n\r\n"));
        return;
      }
      const auto condition = request->headers.find("if-none-match");
      const auto digest = request->headers.find("x-amz-meta-chronos-sha256");
      const auto content_digest = request->headers.find("x-amz-content-sha256");
      const auto transport_digest = request->headers.find("x-amz-checksum-sha256");
      if (condition == request->headers.end() || condition->second != "*" ||
          digest == request->headers.end() || content_digest == request->headers.end() ||
          transport_digest == request->headers.end() || content_digest->second != digest->second ||
          transport_digest->second.empty()) {
        static_cast<void>(send_all(descriptor,
                                   "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: "
                                   "close\r\n\r\n"));
        return;
      }
      if (object_.has_value()) {
        static_cast<void>(send_all(descriptor,
                                   "HTTP/1.1 412 Precondition Failed\r\nContent-Length: "
                                   "0\r\nConnection: close\r\n\r\n"));
        return;
      }
      object_ = request->body;
      checksum_hex_ = digest->second;
      static_cast<void>(send_all(
          descriptor, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"));
      return;
    }
    if (!object_.has_value()) {
      static_cast<void>(send_all(
          descriptor, "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"));
      return;
    }
    if (request->method == "HEAD") {
      const std::string response =
          "HTTP/1.1 200 OK\r\nContent-Length: " + std::to_string(object_->size()) +
          "\r\nx-amz-meta-chronos-sha256: " + checksum_hex_ +
          "\r\nETag: \"fixture-etag\"\r\nConnection: close\r\n\r\n";
      static_cast<void>(send_all(descriptor, response));
      return;
    }
    if (request->method == "GET") {
      const auto range = request->headers.find("range");
      if (range == request->headers.end() || range->second != "bytes=1-2") {
        static_cast<void>(send_all(descriptor,
                                   "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Length: "
                                   "0\r\nConnection: close\r\n\r\n"));
        return;
      }
      const std::string header = "HTTP/1.1 206 Partial Content\r\nContent-Length: "
                                 "2\r\nContent-Range: bytes 1-2/3\r\nConnection: close\r\n\r\n";
      static_cast<void>(send_all(descriptor, header));
      const std::array<char, 2U> body{
          static_cast<char>(std::to_integer<unsigned char>((*object_)[1])),
          static_cast<char>(std::to_integer<unsigned char>((*object_)[2]))};
      static_cast<void>(send_all(descriptor, std::string_view{body.data(), body.size()}));
      return;
    }
    if (request->method == "DELETE") {
      const auto condition = request->headers.find("if-match");
      if (condition == request->headers.end() || condition->second != "\"fixture-etag\"") {
        static_cast<void>(send_all(descriptor,
                                   "HTTP/1.1 412 Precondition Failed\r\nContent-Length: "
                                   "0\r\nConnection: close\r\n\r\n"));
        return;
      }
      object_.reset();
      checksum_hex_.clear();
      static_cast<void>(send_all(
          descriptor, "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"));
      return;
    }
    static_cast<void>(send_all(
        descriptor,
        "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"));
  }

  void serve(const std::stop_token stop) {
    while (!stop.stop_requested()) {
      pollfd readiness{.fd = listener_, .events = POLLIN};
      const int ready = ::poll(&readiness, 1U, 50);
      if (ready < 0) {
        if (errno == EINTR)
          continue;
        if (!stop.stop_requested())
          record_failure("listener poll failed");
        return;
      }
      if (ready == 0)
        continue;
      const int connection = ::accept(listener_, nullptr, nullptr);
      if (connection < 0) {
        if (!stop.stop_requested())
          record_failure("listener accept failed");
        return;
      }
      handle(connection);
      ::close(connection);
    }
  }

  mutable std::mutex mutex_;
  int listener_{-1};
  std::uint16_t port_{};
  std::jthread worker_;
  std::vector<RecordedRequest> requests_;
  std::string failure_;
  std::optional<std::vector<std::byte>> object_;
  std::string checksum_hex_;
  LocalS3Behavior behavior_;
};

class RefreshingCredentialProvider final : public S3CredentialProvider {
public:
  [[nodiscard]] common::Result<S3Credentials> acquire(const S3CredentialRequest request) override {
    std::scoped_lock lock{mutex_};
    requests_.push_back(request);
    if (request == S3CredentialRequest::kRefresh)
      refreshed_ = true;
    if (!refreshed_) {
      return S3Credentials{.access_key_id = "expired-access",
                           .secret_access_key = "expired-secret",
                           .session_token = "expired-token"};
    }
    return S3Credentials{.access_key_id = "fresh-access",
                         .secret_access_key = "fresh-secret",
                         .session_token = "fresh-token"};
  }

  [[nodiscard]] std::vector<S3CredentialRequest> requests() const {
    std::scoped_lock lock{mutex_};
    return requests_;
  }

private:
  mutable std::mutex mutex_;
  bool refreshed_{};
  std::vector<S3CredentialRequest> requests_;
};

TEST(MemoryObjectStoreTest, ImmutablePutIsIdempotentAndRangesAreBounded) {
  MemoryObjectStore store;
  const std::vector<std::byte> bytes{std::byte{1U}, std::byte{2U}, std::byte{3U}};
  const auto checksum = ingest::sha256(bytes).value();
  EXPECT_TRUE(store.put_if_absent("part/a", bytes, checksum).has_value());
  EXPECT_TRUE(store.put_if_absent("part/a", bytes, checksum).has_value());
  EXPECT_EQ(store.object_count(), 1U);
  const auto range = store.get_range("part/a", 1U, 2U);
  ASSERT_TRUE(range.has_value());
  EXPECT_EQ(*range, (std::vector<std::byte>{std::byte{2U}, std::byte{3U}}));
  const std::vector<std::byte> other{std::byte{9U}};
  EXPECT_FALSE(store.put_if_absent("part/a", other, ingest::sha256(other).value()).has_value());
  auto wrong_delete = store.remove_if_exact("part/a", bytes.size(), ingest::sha256(other).value());
  ASSERT_FALSE(wrong_delete.has_value());
  EXPECT_EQ(wrong_delete.error().code(), common::StatusCode::kAlreadyExists);
  EXPECT_EQ(store.object_count(), 1U);
  auto removed = store.remove_if_exact("part/a", bytes.size(), checksum);
  ASSERT_TRUE(removed.has_value());
  EXPECT_TRUE(removed->removed);
  EXPECT_FALSE(removed->already_absent);
  EXPECT_EQ(store.object_count(), 0U);
  auto retry = store.remove_if_exact("part/a", bytes.size(), checksum);
  ASSERT_TRUE(retry.has_value());
  EXPECT_FALSE(retry->removed);
  EXPECT_TRUE(retry->already_absent);
}

TEST(S3ObjectStoreTest, SignsConditionalPutVerifiesRetryAndReadsExactRange) {
  LocalS3Server server;
  ASSERT_TRUE(server.valid()) << server.failure();
  S3ObjectStoreConfig config{.endpoint = "http://127.0.0.1:" + std::to_string(server.port()),
                             .region = "us-east-1",
                             .bucket = "chronos-test",
                             .access_key_id = "test-access",
                             .secret_access_key = "test-secret",
                             .session_token = "test-token",
                             .connect_timeout = std::chrono::milliseconds{1'000},
                             .request_timeout = std::chrono::milliseconds{2'000},
                             .maximum_response_bytes = 1024U};
  EXPECT_FALSE(S3ObjectStore::create(config).has_value());
  config.require_tls = false;
  auto store = S3ObjectStore::create(std::move(config));
  ASSERT_TRUE(store.has_value()) << store.error().to_string();

  const std::vector<std::byte> bytes{std::byte{1U}, std::byte{2U}, std::byte{3U}};
  const auto checksum = ingest::sha256(bytes).value();
  auto uploaded = (*store)->put_if_absent("parts/a b", bytes, checksum);
  ASSERT_TRUE(uploaded.has_value()) << uploaded.error().to_string();
  auto retried = (*store)->put_if_absent("parts/a b", bytes, checksum);
  ASSERT_TRUE(retried.has_value()) << retried.error().to_string();
  EXPECT_EQ(*retried, *uploaded);
  const std::vector<std::byte> conflicting_bytes{std::byte{9U}};
  auto conflict = (*store)->put_if_absent("parts/a b", conflicting_bytes,
                                          ingest::sha256(conflicting_bytes).value());
  ASSERT_FALSE(conflict.has_value());
  EXPECT_EQ(conflict.error().code(), common::StatusCode::kAlreadyExists);

  auto metadata = (*store)->stat("parts/a b");
  ASSERT_TRUE(metadata.has_value()) << metadata.error().to_string();
  EXPECT_EQ(metadata->size, 3U);
  EXPECT_EQ(metadata->checksum, checksum);
  auto range = (*store)->get_range("parts/a b", 1U, 2U);
  ASSERT_TRUE(range.has_value()) << range.error().to_string();
  EXPECT_EQ(*range, (std::vector<std::byte>{std::byte{2U}, std::byte{3U}}));

  auto wrong_delete = (*store)->remove_if_exact("parts/a b", bytes.size(),
                                                ingest::sha256(conflicting_bytes).value());
  ASSERT_FALSE(wrong_delete.has_value());
  EXPECT_EQ(wrong_delete.error().code(), common::StatusCode::kAlreadyExists);
  auto removed = (*store)->remove_if_exact("parts/a b", bytes.size(), checksum);
  ASSERT_TRUE(removed.has_value()) << removed.error().to_string();
  EXPECT_TRUE(removed->removed);
  EXPECT_FALSE(removed->already_absent);
  auto deletion_retry = (*store)->remove_if_exact("parts/a b", bytes.size(), checksum);
  ASSERT_TRUE(deletion_retry.has_value());
  EXPECT_FALSE(deletion_retry->removed);
  EXPECT_TRUE(deletion_retry->already_absent);

  const auto requests = server.requests();
  ASSERT_EQ(requests.size(), 11U);
  EXPECT_EQ(requests[0].method, "PUT");
  EXPECT_EQ(requests[0].target, "/chronos-test/parts/a%20b");
  EXPECT_EQ(requests[1].method, "PUT");
  EXPECT_EQ(requests[2].method, "HEAD");
  EXPECT_EQ(requests[3].method, "PUT");
  EXPECT_EQ(requests[4].method, "HEAD");
  EXPECT_EQ(requests[5].method, "HEAD");
  EXPECT_EQ(requests[6].method, "GET");
  EXPECT_EQ(requests[7].method, "HEAD");
  EXPECT_EQ(requests[8].method, "HEAD");
  EXPECT_EQ(requests[9].method, "DELETE");
  EXPECT_EQ(requests[9].headers.at("if-match"), "\"fixture-etag\"");
  EXPECT_EQ(requests[10].method, "HEAD");
  EXPECT_TRUE(server.failure().empty()) << server.failure();
}

TEST(S3ObjectStoreTest, RefreshesRejectedCredentialsAndRetriesTransientConditionalPut) {
  LocalS3Server server{LocalS3Behavior{.access_key_id = "fresh-access",
                                       .session_token = "fresh-token",
                                       .transient_put_failures = 1U}};
  ASSERT_TRUE(server.valid()) << server.failure();
  auto provider = std::make_shared<RefreshingCredentialProvider>();
  S3ObjectStoreConfig config{.endpoint = "http://127.0.0.1:" + std::to_string(server.port()),
                             .region = "us-east-1",
                             .bucket = "chronos-test",
                             .credential_provider = provider,
                             .connect_timeout = std::chrono::milliseconds{1'000},
                             .request_timeout = std::chrono::milliseconds{2'000},
                             .maximum_attempts = 3U,
                             .initial_retry_backoff = std::chrono::milliseconds{0},
                             .maximum_retry_backoff = std::chrono::milliseconds{0},
                             .maximum_response_bytes = 1024U,
                             .require_tls = false};
  auto store = S3ObjectStore::create(std::move(config));
  ASSERT_TRUE(store.has_value()) << store.error().to_string();

  const std::vector<std::byte> bytes{std::byte{4U}, std::byte{5U}, std::byte{6U}};
  const auto checksum = ingest::sha256(bytes).value();
  auto uploaded = (*store)->put_if_absent("parts/retried", bytes, checksum);
  ASSERT_TRUE(uploaded.has_value()) << uploaded.error().to_string();
  EXPECT_EQ(uploaded->size, bytes.size());
  EXPECT_EQ(uploaded->checksum, checksum);

  EXPECT_EQ(provider->requests(),
            (std::vector{S3CredentialRequest::kCurrent, S3CredentialRequest::kRefresh,
                         S3CredentialRequest::kCurrent}));
  const auto requests = server.requests();
  ASSERT_EQ(requests.size(), 3U);
  EXPECT_TRUE(requests[0].headers.at("authorization").contains("Credential=expired-access/"));
  EXPECT_TRUE(requests[1].headers.at("authorization").contains("Credential=fresh-access/"));
  EXPECT_TRUE(requests[2].headers.at("authorization").contains("Credential=fresh-access/"));
  EXPECT_TRUE(server.failure().empty()) << server.failure();
}

TEST(S3ObjectStoreTest, StopsAtTheConfiguredRetryAttemptLimit) {
  LocalS3Server server{LocalS3Behavior{.transient_put_failures = 3U}};
  ASSERT_TRUE(server.valid()) << server.failure();
  S3ObjectStoreConfig config{.endpoint = "http://127.0.0.1:" + std::to_string(server.port()),
                             .region = "us-east-1",
                             .bucket = "chronos-test",
                             .access_key_id = "test-access",
                             .secret_access_key = "test-secret",
                             .session_token = "test-token",
                             .connect_timeout = std::chrono::milliseconds{1'000},
                             .request_timeout = std::chrono::milliseconds{2'000},
                             .maximum_attempts = 2U,
                             .initial_retry_backoff = std::chrono::milliseconds{0},
                             .maximum_retry_backoff = std::chrono::milliseconds{0},
                             .maximum_response_bytes = 1024U,
                             .require_tls = false};
  auto store = S3ObjectStore::create(std::move(config));
  ASSERT_TRUE(store.has_value()) << store.error().to_string();

  const std::vector<std::byte> bytes{std::byte{7U}};
  auto uploaded = (*store)->put_if_absent("parts/exhausted", bytes, ingest::sha256(bytes).value());
  ASSERT_FALSE(uploaded.has_value());
  EXPECT_EQ(uploaded.error().code(), common::StatusCode::kUnavailable);
  EXPECT_EQ(server.requests().size(), 2U);
  EXPECT_TRUE(server.failure().empty()) << server.failure();
}

} // namespace
} // namespace chronos::tiering
