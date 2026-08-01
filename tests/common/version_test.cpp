#include "chronos/common/version.hpp"

#include <gtest/gtest.h>
#include <string>
#include <string_view>

namespace chronos::common {
namespace {

TEST(VersionTest, ExposesSemanticVersion) {
  EXPECT_EQ(semantic_version(), "0.1.0");
  EXPECT_EQ(version_info().semantic_version, semantic_version());
}

TEST(VersionTest, ExposesNonemptyBuildMetadata) {
  const VersionInfo info = version_info();
  EXPECT_FALSE(info.build_type.empty());
  EXPECT_FALSE(info.compiler.empty());
  EXPECT_FALSE(info.target_architecture.empty());
  EXPECT_FALSE(info.operating_system.empty());
  if (info.git_metadata_available) {
    EXPECT_FALSE(info.git_commit.empty());
    EXPECT_NE(info.git_commit, "unknown");
  }
}

TEST(VersionTest, ProducesHumanReadableOutput) {
  const std::string text = version_text();
  EXPECT_NE(text.find("ChronosDB 0.1.0"), std::string::npos);
  EXPECT_NE(text.find("git commit:"), std::string::npos);
  EXPECT_NE(text.find("build type:"), std::string::npos);
  EXPECT_NE(text.find("compiler:"), std::string::npos);
  EXPECT_NE(text.find("target architecture:"), std::string::npos);
  EXPECT_NE(text.find("operating system:"), std::string::npos);
}

TEST(VersionTest, EscapesJsonAndUsesStableFieldNames) {
  constexpr VersionInfo info{
      .semantic_version = "1\"2\\3\n",
      .git_commit = "abc\tdef",
      .git_metadata_available = true,
      .git_dirty = true,
      .build_type = "Debug\rBuild",
      .compiler = "compiler\x01",
      .target_architecture = "arm64",
      .operating_system = "TestOS",
  };

  EXPECT_EQ(
      version_json(info),
      R"({"version":"1\"2\\3\n","git_commit":"abc\tdef","git_dirty":true,"build_type":"Debug\rBuild","compiler":"compiler\u0001","target_architecture":"arm64","operating_system":"TestOS"})");
}

TEST(VersionTest, RepresentsUnavailableGitMetadata) {
  constexpr VersionInfo info{
      .semantic_version = "0.1.0",
      .git_commit = "unknown",
      .git_metadata_available = false,
      .git_dirty = false,
      .build_type = "Debug",
      .compiler = "Test Compiler",
      .target_architecture = "test-arch",
      .operating_system = "TestOS",
  };

  const std::string json = version_json(info);
  EXPECT_NE(json.find(R"("git_commit":null)"), std::string::npos);
  EXPECT_NE(json.find(R"("git_dirty":null)"), std::string::npos);
  EXPECT_NE(version_text(info).find("git commit: unavailable"), std::string::npos);
}

TEST(VersionTest, StableInputsProduceDeterministicOutput) {
  const VersionInfo info = version_info();
  EXPECT_EQ(version_text(info), version_text(info));
  EXPECT_EQ(version_json(info), version_json(info));
}

} // namespace
} // namespace chronos::common
