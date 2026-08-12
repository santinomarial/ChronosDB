#include "chronos/service/replicated_group_config.hpp"

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <new>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace chronos::service {
namespace {

[[nodiscard]] common::Status invalid(const char* message) {
  return {common::StatusCode::kInvalidArgument, message};
}

[[nodiscard]] common::Status exhausted(const char* message) {
  return {common::StatusCode::kResourceExhausted, message};
}

[[nodiscard]] std::optional<std::uint8_t> hex(const char value) noexcept {
  if (value >= '0' && value <= '9')
    return static_cast<std::uint8_t>(value - '0');
  if (value >= 'a' && value <= 'f')
    return static_cast<std::uint8_t>(value - 'a' + 10);
  return std::nullopt;
}

[[nodiscard]] common::Result<common::Uuid> parse_uuid(const std::string_view text) {
  if (text.size() != 36U || text[8U] != '-' || text[13U] != '-' || text[18U] != '-' ||
      text[23U] != '-')
    return common::make_unexpected(invalid("replicated group UUID is not canonical"));
  common::Uuid::Bytes bytes{};
  std::size_t output = 0U;
  for (std::size_t input = 0U; input < text.size();) {
    if (input == 8U || input == 13U || input == 18U || input == 23U) {
      ++input;
      continue;
    }
    const auto high = hex(text[input]);
    const auto low = hex(text[input + 1U]);
    if (!high.has_value() || !low.has_value())
      return common::make_unexpected(invalid("replicated group UUID is not lowercase hex"));
    bytes[output++] = static_cast<std::byte>((*high << 4U) | *low);
    input += 2U;
  }
  common::Uuid value{bytes};
  if (value.is_nil())
    return common::make_unexpected(invalid("replicated group UUID is nil"));
  return value;
}

[[nodiscard]] common::Result<std::vector<raft::NodeId>>
parse_voters(const std::string_view text, const std::size_t maximum_voters) {
  if (text.empty())
    return common::make_unexpected(invalid("replicated group voter list is empty"));
  try {
    std::vector<raft::NodeId> voters;
    voters.reserve(std::min(maximum_voters, text.size()));
    std::size_t offset = 0U;
    while (offset < text.size()) {
      const std::size_t separator = text.find(',', offset);
      const std::size_t end = separator == std::string_view::npos ? text.size() : separator;
      const std::string_view token = text.substr(offset, end - offset);
      if (token.empty() || (token.size() > 1U && token.front() == '0'))
        return common::make_unexpected(
            invalid("replicated group voter ID is not canonical decimal"));
      raft::NodeId node_id{};
      const auto parsed = std::from_chars(token.data(), token.data() + token.size(), node_id);
      if (parsed.ec != std::errc{} || parsed.ptr != token.data() + token.size() || node_id == 0U)
        return common::make_unexpected(invalid("replicated group voter ID is invalid"));
      if (!voters.empty() && voters.back() >= node_id)
        return common::make_unexpected(
            invalid("replicated group voter IDs are not strictly increasing"));
      if (voters.size() >= maximum_voters)
        return common::make_unexpected(exhausted("replicated group voter limit exceeded"));
      voters.push_back(node_id);
      if (separator == std::string_view::npos)
        break;
      offset = separator + 1U;
    }
    return voters;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("replicated group voter allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("replicated group voter list exceeds limits"));
  }
}

} // namespace

common::Result<std::vector<raft::RaftGroupConfiguration>>
parse_replicated_group_config(const std::string_view text,
                              const ReplicatedGroupConfigLimits limits) {
  if (limits.maximum_bytes == 0U || limits.maximum_groups == 0U ||
      limits.maximum_voters_per_group == 0U || limits.maximum_voters_per_group > 1024U)
    return common::make_unexpected(invalid("replicated group config limits are invalid"));
  if (text.empty() || text.size() > limits.maximum_bytes)
    return common::make_unexpected(exhausted("replicated group config size is invalid"));
  if (text.find('\r') != std::string_view::npos)
    return common::make_unexpected(invalid("replicated group config contains a carriage return"));
  const std::size_t first_lf = text.find('\n');
  if (first_lf == std::string_view::npos ||
      text.substr(0U, first_lf) != kReplicatedGroupConfigV1Magic)
    return common::make_unexpected(invalid("replicated group config magic is invalid"));
  try {
    std::vector<raft::RaftGroupConfiguration> groups;
    groups.reserve(std::min(limits.maximum_groups, text.size() - first_lf - 1U));
    std::size_t offset = first_lf + 1U;
    while (offset < text.size()) {
      const std::size_t separator = text.find('\n', offset);
      const std::size_t end = separator == std::string_view::npos ? text.size() : separator;
      const std::string_view line = text.substr(offset, end - offset);
      if (line.empty())
        return common::make_unexpected(invalid("replicated group config contains a blank line"));
      const std::size_t equals = line.find('=');
      if (equals == std::string_view::npos || line.find('=', equals + 1U) != std::string_view::npos)
        return common::make_unexpected(invalid("replicated group config line is malformed"));
      auto group_id = parse_uuid(line.substr(0U, equals));
      if (!group_id.has_value())
        return common::make_unexpected(group_id.error());
      auto voters = parse_voters(line.substr(equals + 1U), limits.maximum_voters_per_group);
      if (!voters.has_value())
        return common::make_unexpected(voters.error());
      if (groups.size() >= limits.maximum_groups)
        return common::make_unexpected(exhausted("replicated group count limit exceeded"));
      groups.push_back({*group_id, std::move(*voters)});
      if (separator == std::string_view::npos)
        break;
      offset = separator + 1U;
    }
    if (groups.empty())
      return common::make_unexpected(invalid("replicated group config has no groups"));
    std::ranges::sort(groups, {}, &raft::RaftGroupConfiguration::group_id);
    if (std::ranges::adjacent_find(groups, {}, &raft::RaftGroupConfiguration::group_id) !=
        groups.end())
      return common::make_unexpected(invalid("replicated group config repeats a group"));
    return groups;
  } catch (const std::bad_alloc&) {
    return common::make_unexpected(exhausted("replicated group config allocation failed"));
  } catch (const std::length_error&) {
    return common::make_unexpected(exhausted("replicated group config exceeds container limits"));
  }
}

} // namespace chronos::service
