#include "chronos/cluster/raft_transport_receiver.hpp"

#include <cstddef>
#include <filesystem>
#include <gtest/gtest.h>
#include <string>
#include <system_error>
#include <unistd.h>
#include <utility>
#include <variant>
#include <vector>

namespace chronos::cluster {
namespace {

class TemporaryDirectory {
public:
  TemporaryDirectory() {
    std::string pattern =
        (std::filesystem::temp_directory_path() / "chronos-raft-receiver-XXXXXX").string();
    if (char* const created = ::mkdtemp(pattern.data()); created != nullptr)
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

[[nodiscard]] raft::GroupId group(const std::byte seed = std::byte{9U}) {
  common::Uuid::Bytes bytes{};
  bytes.fill(seed);
  return raft::GroupId{bytes};
}

class FixedAuthorizer final : public ClusterNodePrincipalAuthorizer {
public:
  [[nodiscard]] common::Result<bool>
  authorize_node(const std::uint64_t principal_id,
                 const raft::NodeId claimed_node_id) const override {
    ++calls;
    return principal_id == 700U && claimed_node_id == 1U;
  }

  mutable std::size_t calls{};
};

[[nodiscard]] std::vector<std::byte> vote_request(const raft::GroupId& group_id = group(),
                                                  const raft::NodeId destination = 2U) {
  return raft::encode_raft_transport_envelope_v1(
             {.group_id = group_id,
              .source = 1U,
              .destination = destination,
              .message = raft::RequestVoteRequest{1U, 1U, 0U, 0U}})
      .value();
}

TEST(RaftTransportReceiverTest, AuthenticatesRoutesPersistsAndEncodesResponse) {
  TemporaryDirectory directory;
  FixedAuthorizer authorizer;
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      2U, {.directory_path = directory.path().string()}, {{group(), {1U, 2U}}});
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto receiver = RaftTransportReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .runtime = &*runtime});
  ASSERT_TRUE(receiver.has_value()) << receiver.error().to_string();

  auto admission = receiver->try_receive(
      vote_request(), network::PeerAuthenticationResult{.authorized = true, .principal_id = 700U});

  ASSERT_TRUE(admission.has_value()) << admission.error().to_string();
  EXPECT_EQ(admission->completion.submission_sequence(), 1U);
  EXPECT_EQ(admission->group_id, group());
  EXPECT_EQ(admission->source_node_id, 1U);
  auto completed = admission->completion.wait();
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  ASSERT_EQ(completed->size(), 2U);
  ASSERT_TRUE(completed->front().status.is_ok()) << completed->front().status.to_string();
  ASSERT_TRUE(completed->front().transition.has_value());
  EXPECT_TRUE(completed->front().transition->persistence.has_value());
  ASSERT_TRUE((*completed)[1].status.is_ok());
  ASSERT_TRUE((*completed)[1].observation.has_value());
  EXPECT_EQ((*completed)[1].observation->group_id, group());
  EXPECT_EQ((*completed)[1].observation->current_term, 1U);
  auto frames = encode_durable_raft_outbound_v1(group(), 2U, completed->front());
  ASSERT_TRUE(frames.has_value()) << frames.error().to_string();
  ASSERT_EQ(frames->size(), 1U);
  auto decoded = raft::decode_raft_transport_envelope_v1(frames->front());
  ASSERT_TRUE(decoded.has_value()) << decoded.error().to_string();
  EXPECT_EQ(decoded->group_id, group());
  EXPECT_EQ(decoded->source, 2U);
  EXPECT_EQ(decoded->destination, 1U);
  ASSERT_TRUE(std::holds_alternative<raft::RequestVoteResponse>(decoded->message));
  EXPECT_TRUE(std::get<raft::RequestVoteResponse>(decoded->message).granted);
  EXPECT_EQ(authorizer.calls, 1U);
  ASSERT_TRUE(runtime->shutdown().is_ok());
}

TEST(RaftTransportReceiverTest, RejectsTrustAndRouteFailuresBeforeRuntimeAdmission) {
  TemporaryDirectory directory;
  FixedAuthorizer authorizer;
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      2U, {.directory_path = directory.path().string()}, {{group(), {1U, 2U}}});
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto receiver = RaftTransportReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .runtime = &*runtime});
  ASSERT_TRUE(receiver.has_value());
  std::vector<std::byte> damaged{std::byte{1U}};

  EXPECT_EQ(
      receiver->try_receive(damaged, {.authorized = false, .principal_id = 700U}).error().code(),
      common::StatusCode::kUnauthenticated);
  EXPECT_EQ(receiver->try_receive(vote_request(), {.authorized = true, .principal_id = 701U})
                .error()
                .code(),
            common::StatusCode::kUnauthenticated);
  EXPECT_EQ(
      receiver->try_receive(vote_request(group(), 3U), {.authorized = true, .principal_id = 700U})
          .error()
          .code(),
      common::StatusCode::kUnavailable);
  const auto metrics = runtime->metrics();
  EXPECT_EQ(metrics.admitted_batches, 0U);
  EXPECT_EQ(metrics.pending_batches, 0U);
  EXPECT_EQ(authorizer.calls, 2U);
  auto small_receiver =
      RaftTransportReceiver::create({.local_node_id = 2U,
                                     .authorizer = &authorizer,
                                     .runtime = &*runtime,
                                     .codec_limits = {.maximum_entry_bytes = 1U}});
  ASSERT_TRUE(small_receiver.has_value());
  EXPECT_EQ(small_receiver
                ->try_receive_decoded(
                    {.group_id = group(),
                     .source = 1U,
                     .destination = 2U,
                     .message =
                         raft::AppendEntriesRequest{
                             1U, 1U, 0U, 0U, {{1U, 1U, 1U, {std::byte{1U}, std::byte{2U}}}}, 0U}},
                    {.authorized = true, .principal_id = 700U})
                .error()
                .code(),
            common::StatusCode::kInvalidArgument);
  EXPECT_EQ(runtime->metrics().admitted_batches, 0U);
  ASSERT_TRUE(runtime->shutdown().is_ok());
}

TEST(RaftTransportReceiverTest, SurfacesUnknownGroupWithoutFailingRuntime) {
  TemporaryDirectory directory;
  FixedAuthorizer authorizer;
  auto runtime = raft::AsyncDurableMultiRaftRuntime::create_new(
      2U, {.directory_path = directory.path().string()}, {{group(), {1U, 2U}}});
  ASSERT_TRUE(runtime.has_value()) << runtime.error().to_string();
  auto receiver = RaftTransportReceiver::create(
      {.local_node_id = 2U, .authorizer = &authorizer, .runtime = &*runtime});
  ASSERT_TRUE(receiver.has_value());

  auto admission = receiver->try_receive(vote_request(group(std::byte{8U})),
                                         {.authorized = true, .principal_id = 700U});
  ASSERT_TRUE(admission.has_value()) << admission.error().to_string();
  EXPECT_EQ(admission->completion.submission_sequence(), 1U);
  auto completed = admission->completion.wait();
  ASSERT_TRUE(completed.has_value()) << completed.error().to_string();
  ASSERT_EQ(completed->size(), 2U);
  EXPECT_EQ(completed->front().status.code(), common::StatusCode::kNotFound);
  EXPECT_EQ((*completed)[1].status.code(), common::StatusCode::kNotFound);
  EXPECT_FALSE((*completed)[1].observation.has_value());
  EXPECT_TRUE(runtime->terminal_status().is_ok());
  EXPECT_EQ(
      encode_durable_raft_outbound_v1(group(std::byte{8U}), 2U, completed->front()).error().code(),
      common::StatusCode::kNotFound);
  ASSERT_TRUE(runtime->shutdown().is_ok());
}

} // namespace
} // namespace chronos::cluster
