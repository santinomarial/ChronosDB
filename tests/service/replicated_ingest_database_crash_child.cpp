#include "chronos/ingest/async_raft_tablet_application.hpp"
#include "chronos/raft/async_durable_runtime.hpp"
#include "chronos/service/replicated_ingest_database.hpp"
#include "chronos/service/replicated_ingest_runtime.hpp"
#include "service/replicated_ingest_database_crash_fixture.hpp"

#include <exception>
#include <filesystem>
#include <iostream>
#include <string_view>
#include <unistd.h>
#include <utility>
#include <vector>

namespace chronos::service::test {
namespace {

[[nodiscard]] std::filesystem::path parse_directory(const int count, char** const values) {
  for (int index = 1; index + 1 < count; index += 2) {
    if (std::string_view{values[index]} == "--directory")
      return values[index + 1];
  }
  return {};
}

[[nodiscard]] common::Status submit(ReplicatedIngestRuntime& runtime,
                                    std::vector<raft::DurableRaftRequest> requests) {
  auto queued = runtime.runtime()->try_submit(std::move(requests));
  if (!queued.has_value())
    return queued.error();
  auto completed = queued->wait();
  return completed.has_value() ? common::Status::ok() : completed.error();
}

[[nodiscard]] common::Status prepare_and_reopen(const std::filesystem::path& root) {
  auto bootstrap = runtime::DatabaseBootstrap::open_or_create(new_crash_bootstrap_config(root));
  if (!bootstrap.has_value())
    return bootstrap.error();
  auto initial = ReplicatedIngestRuntime::create_new(crash_runtime_config(*bootstrap));
  if (!initial.has_value())
    return initial.error();
  common::Status status =
      submit(*initial, {{crash_metadata_group(), raft::StartElectionOperation{}}});
  if (status.is_ok())
    status = submit(*initial, {{crash_tablet_group(), raft::StartElectionOperation{}}});
  if (status.is_ok())
    status = submit(*initial, crash_metadata_requests());
  if (status.is_ok()) {
    status =
        submit(*initial,
               {{crash_tablet_group(),
                 raft::ProposeOperation{ingest::kRaftColumnarAppendEntryType, crash_command()}}});
  }
  if (status.is_ok()) {
    auto publication = initial->tablet_application()->snapshot(crash_tablet_group());
    if (!publication.has_value())
      status = publication.error();
    else if (publication->visible_row_count() != 2U || publication->retry_entry_count() != 1U)
      status = {common::StatusCode::kInternal,
                "crash child initial tablet publication is incomplete"};
  }
  const common::Status initial_shutdown = initial->shutdown();
  if (status.is_ok())
    status = initial_shutdown;
  const common::Status bootstrap_close = bootstrap->close();
  if (status.is_ok())
    status = bootstrap_close;
  if (!status.is_ok())
    return status;

  auto database = ReplicatedIngestDatabase::open_existing(
      {.bootstrap = existing_crash_bootstrap_config(root), .groups = crash_groups()});
  if (!database.has_value())
    return database.error();
  auto recovered = database->ingest_runtime()->tablet_application()->snapshot(crash_tablet_group());
  if (!recovered.has_value())
    return recovered.error();
  if (recovered->visible_row_count() != 2U || recovered->retry_entry_count() != 1U ||
      recovered->applied_position() != head::HeadCommitPosition::raft(crash_tablet_group(), 1U)) {
    return {common::StatusCode::kInternal,
            "crash child packaged tablet publication disagrees with durable state"};
  }

  std::cout << "READY " << recovered->visible_row_count() << ' ' << recovered->retry_entry_count()
            << '\n'
            << std::flush;
  for (;;)
    static_cast<void>(::pause());
}

} // namespace
} // namespace chronos::service::test

int main(const int count, char** const values) {
  try {
    const std::filesystem::path root = chronos::service::test::parse_directory(count, values);
    if (root.empty()) {
      std::cerr << "replicated database crash child requires --directory\n";
      return 2;
    }
    const chronos::common::Status status = chronos::service::test::prepare_and_reopen(root);
    if (!status.is_ok()) {
      std::cerr << status.to_string() << '\n';
      return 3;
    }
    return 0;
  } catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 4;
  } catch (...) {
    std::cerr << "unknown replicated database crash-child failure\n";
    return 5;
  }
}
