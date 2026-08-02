# Phase 1 Foundation Review

## Reviewed state

The audit began from a clean `main` working tree at commit
`d827b888db0f0a90b09e41e0eee8e071bc57e6eb`. The complete reachable Git history through that
commit was inspected with `git log`, `git show`, and per-commit name/status and statistics views.

During the audit, the repository owner committed and pushed the repair set as
`e8a29d3999cd04582b71f4cd1ca5ef5c5231b6b1`,
`8cb9fe174936e42c8ad489b22162e6919f21d82a`, and
`2109075a61af5fef04a4d536178b1b4b53c15bc6` while the reviewer continued validation. The reviewer
did not run `git commit` or `git push`. This document records the resulting tree relative to the
original reviewed base.

## Scope

This review covered every tracked source, public header, CMake module and preset, test,
microbenchmark, fuzz target, shell script, GitHub Actions workflow, and documentation file present
at the reviewed base. It also covered all accepted ADRs, the architecture invariants, the
correctness strategy, build instructions, and the two implemented Phase 1 learning documents.

The implemented target map after repair is:

- `chronos_version_header`: always-run metadata refresh target whose generator avoids rewriting
  unchanged output;
- `chronos_common` / `chronos::common`: the Phase 1 common static library;
- `chronosctl`: version-reporting command-line executable;
- `chronos_common_tests`: unit tests plus one translation unit for each public header;
- `chronos_common_benchmarks`: optional release microbenchmarks;
- `chronos_byte_reader_fuzz`: optional Clang/libFuzzer target;
- fetched `GTest` and Google Benchmark targets, which remain outside ChronosDB warning, sanitizer,
  and static-analysis policy.

No WAL, recovery log, storage engine, networking, SQL, queue, allocator, or other later subsystem
was reviewed or implemented. This is strictly a Phase 1 foundation review.

## Available platforms and tools

- macOS 26.5.2, Darwin 25.5.0, Apple Silicon arm64;
- AppleClang 21.0.0.21000101;
- Homebrew Clang, clang-format, and clang-tidy 22.1.8;
- CMake 4.2.3 and Ninja 1.13.2 on macOS;
- Ubuntu 24.04 arm64 in an isolated container, with CMake 3.28.3, Ninja 1.11.1, GCC 13.3.0,
  Clang 18.1.3, and libc++ 18.

Windows/MSVC, a physical Linux host, and x86-64 were not available. The repository owner's push
triggered a GitHub-hosted Actions run for `2109075`.

## Findings

### High severity

#### Undefined behavior did not necessarily fail sanitizer jobs

- **Root cause:** `CHRONOS_ENABLE_UBSAN` added `-fsanitize=undefined` but left Clang's recovering
  behavior enabled. A process could report undefined behavior and still exit zero, allowing CTest
  and CI to report success.
- **Evidence:** before repair, a deliberate signed-overflow probe compiled with the project's UBSan
  flag emitted a runtime diagnostic and exited `0`.
- **Fix:** ChronosDB-owned UBSan targets now compile and link with
  `-fno-sanitize-recover=undefined`.
- **Regression:** `BuildConfigurationTest.UbsanIsNonRecovering` checks every ChronosDB-owned entry in
  exported compile commands whenever UBSan is enabled. The same deliberate probe exits `134` after
  repair. ASan/UBSan tests pass on both macOS and Linux.

### Medium severity

#### Generated Git version metadata became stale without reconfiguration

- **Root cause:** commit and dirty-state values were collected only during CMake configuration.
  Editing or committing source followed by an ordinary build left the installed/runtime version
  metadata unchanged.
- **Evidence:** in a clean temporary clone, changing `README.md` followed by `cmake --build` printed
  `ninja: no work to do`; `chronosctl version --json` continued to report `"git_dirty":false`.
- **Fix:** an always-run `chronos_version_header` target refreshes Git state before ChronosDB targets
  build. `configure_file` preserves the generated file timestamp when its contents are unchanged,
  so an unchanged refresh does not trigger recompilation.
- **Regression:** `BuildMetadataTest.RefreshesGitStateWithoutReconfigure` verifies the clean state,
  dirty transition, commit-hash transition, and return to clean state without reconfiguring.

#### Git inspection failure could be mislabeled as a clean tree

- **Root cause:** the old metadata path marked Git metadata available immediately after a successful
  `rev-parse`, before checking whether `git status` could determine dirtiness. A later status error
  therefore collapsed to `git_dirty=false` rather than an unavailable state.
- **Fix:** metadata is available only if both commit and status queries succeed; otherwise commit is
  `unknown` and availability is false.
- **Regression:** the build-metadata test corrupts a temporary repository index and verifies the
  explicit unavailable state.

#### CI dependencies were movable and used an obsolete action runtime

- **Root cause:** workflow actions used major-version tags such as `@v4`; those are mutable references
  and the selected upload action used the Node 20 runtime. This disagreed with the immutable
  dependency policy.
- **Fix:** Checkout and Upload Artifact now use reviewed full commit SHAs for their current v6
  Node 24 releases. A repository script rejects external workflow actions that are not pinned to a
  lowercase 40-character SHA, and the normal check script runs it.
- **Regression:** `scripts/check-workflow-actions.sh` is an executable CI policy check; the current
  workflow passes it, future replacement with a tag fails, and Docker actions require a full sha256
  image digest.

### Low severity

#### Public-header self-containment was only partially automated

- **Root cause:** only `byte_writer.hpp` and `version.hpp` had standalone translation units. Six
  public headers could regress by relying on transitive includes without a test failure.
- **Fix and regression:** standalone translation units now cover all eight public headers. A separate
  `clang++ -x c++ -fsyntax-only` loop also passed for every installed-facing header.

#### CRC32C tests lacked independent binary vectors and chained-extension coverage

- **Root cause:** the implementation had a standard text check and self-consistency tests, but little
  protection against a mutually consistent one-shot/incremental implementation error on arbitrary
  bytes.
- **Fix and regression:** tests now include independent binary check vectors for all-`0xff`, ascending,
  and descending byte sequences. Deterministic randomized chunking checks both the stateful class and
  direct `extend_crc32c` chaining against one-shot CRC32C.

#### UTF-8 and fuzz success-state coverage was too narrow

- **Root cause:** JSON sanitization tested only a valid two-byte sequence and `0xff`; the fuzzer
  checked cursor immutability on failure but not exact advancement on success.
- **Fix and regression:** unit tests now exercise valid boundary sequences and reject overlong,
  surrogate, out-of-range, and truncated encodings. The fuzz oracle checks exact success advancement
  for every reader operation as well as failure atomicity.

#### Validation policy and documentation did not match actual enforcement

- **Root cause:** warnings-as-errors were enabled only for the GCC matrix job even though the same
  target warnings applied elsewhere. The correctness document described all testing as planned even
  though unit, sanitizer, and fuzz infrastructure existed, and tooling text implied all external
  declarations lived in CMake despite workflow actions.
- **Fix:** all normal compiler-matrix jobs use warnings-as-errors, and build, tooling, learning, and
  correctness documentation now describe actual behavior and limitations.
- **Regression:** clean warnings-as-errors builds passed with AppleClang, Linux GCC, and Linux Clang;
  the normal documentation-aligned `scripts/check.sh` path also passes.

### Investigation with no confirmed defect

Manual inspection did not find a production-code defect in the current `Status`/`Result` semantics,
checked arithmetic, byte order, floating-point bit transport, CRC32C polynomial/initialization/final
XOR, or JSON escaping after the coverage additions above. Specifically:

- `Status` owns error messages, cannot carry text while OK, and `Result` rejects an OK error state;
- reader/writer checks use subtraction-based bounds tests before cursor or destination mutation;
- shifts operate on fixed-width unsigned values and are bounded by the encoded primitive width;
- byte I/O is byte-wise little-endian and uses `std::bit_cast` for floating-point bit preservation,
  avoiding unaligned loads, aliasing violations, and native-struct serialization;
- failed reads preserve the cursor and failed writes preserve both cursor and destination;
- CRC32C uses the reflected Castagnoli polynomial `0x82f63b78`, initial/final complement, and a
  consistent incremental contract;
- benchmark results are consumed by the harness, setup is outside measured loops where intended,
  and the documentation makes no performance claim.

## Validation commands and outcomes

### Baseline before repair

All source trees began clean at `d827b88`.

- `cmake --preset debug`, `cmake --build --preset debug`, `ctest --preset debug`: passed, 37/37.
- The same configure/build/test sequence for `release`: passed, 37/37.
- The same sequence for `asan-ubsan`: passed, 37/37, but the independent UB probe proved that the
  configuration could still exit success after a UBSan finding.
- The same sequence for `tsan`: passed, 37/37.
- `scripts/format.sh --check`: could not locate `clang-format` on the default `PATH`;
  `CLANG_FORMAT=/opt/homebrew/opt/llvm/bin/clang-format scripts/format.sh --check`: passed.
- `CLANG_TIDY=/opt/homebrew/opt/llvm/bin/clang-tidy scripts/lint.sh debug`: passed.
- Benchmark configure/build plus
  `build/benchmark/chronos_common_benchmarks --benchmark_min_time=0.01s --benchmark_repetitions=1`:
  all eight smoke cases completed; macOS clock-frequency/affinity metadata warnings were emitted and
  no performance conclusion was drawn.
- Fuzz configure/build plus
  `ASAN_OPTIONS=detect_leaks=0 build/fuzz/chronos_byte_reader_fuzz -runs=10000 -max_len=4096`:
  10,000 executions completed without a ChronosDB finding; macOS symbolizer warnings were emitted.
- Standalone compilation of all eight public headers with AppleClang: passed.
- Clean AppleClang warnings-as-errors configure/build/test in an empty build directory: passed,
  37/37.
- Clean Ubuntu GCC 13.3 warnings-as-errors configure/build/test: passed, 37/37.
- Clean Ubuntu Clang 18/libc++ warnings-as-errors configure/build/test: passed, 37/37.
- `git diff --check`: passed on the initially clean tree.

### After repair

- Debug, release, ASan/UBSan, and TSan preset configure/build/test sequences on macOS: passed.
  Debug, release, and TSan ran 40/40 tests; ASan/UBSan ran 41/41, including the non-recovering
  configuration test.
- A clean AppleClang 21 warnings-as-errors configure/build/test in
  `build/review-post-appleclang`: passed, 40/40.
- Clean Ubuntu GCC 13.3 warnings-as-errors configure/build/test in
  `build/review-post-linux-gcc`: passed, 40/40.
- Clean Ubuntu Clang 18/libc++ warnings-as-errors configure/build/test in
  `build/review-post-linux-clang`: passed, 40/40.
- Ubuntu Clang ASan/UBSan configure/build/test in `build/review-post-linux-asan-ubsan`: passed,
  41/41.
- Ubuntu Clang TSan configure/build/test in `build/review-post-linux-tsan`: passed, 40/40.
- `CLANG_FORMAT=/opt/homebrew/opt/llvm/bin/clang-format scripts/format.sh --check`: passed.
- `CLANG_TIDY=/opt/homebrew/opt/llvm/bin/clang-tidy scripts/lint.sh debug`: passed.
- A clean rebuild followed by
  `CLANG_TIDY=/opt/homebrew/opt/llvm/bin/clang-tidy scripts/lint.sh fuzz -DCHRONOS_WARNINGS_AS_ERRORS=ON`:
  passed, including the fuzz target.
- `CLANG_TIDY=/opt/homebrew/opt/llvm/bin/clang-tidy scripts/lint.sh benchmark -DCHRONOS_WARNINGS_AS_ERRORS=ON`:
  passed, including benchmark sources.
- Benchmark smoke command with `--benchmark_min_time=0.01s --benchmark_repetitions=1`: all eight
  cases completed; the run remains only a validity smoke test.
- `build/fuzz/chronos_byte_reader_fuzz -runs=10000 -max_len=4096 -seed=424242`: passed 10,000
  reproducible executions; only external macOS symbolizer warnings were emitted.
- Standalone AppleClang syntax compilation for all eight public headers: passed.
- Installation to a temporary prefix followed by a separate `find_package(Chronos 0.1.0 CONFIG
  REQUIRED)` consumer build, link, and execution against `chronos::common`: passed.
- An unchanged second build ran only the metadata refresh and did not recompile a target.
- The repaired deliberate signed-overflow UBSan probe emitted the diagnostic and exited `134`.
- Inspection of `compile_commands.json` confirmed ChronosDB sources receive warnings, sanitizers, and
  fatal UBSan behavior while fetched GTest sources receive none of those target-scoped flags.

- `CLANG_FORMAT=/opt/homebrew/opt/llvm/bin/clang-format
  CLANG_TIDY=/opt/homebrew/opt/llvm/bin/clang-tidy scripts/check.sh`: passed after the review
  document was added; its integrated test run passed 40/40.
- `build/dev/chronosctl version --json`: passed and reported commit `8cb9fe174936` with
  `"git_dirty":true`, correctly reflecting the review documentation before the final
  repository-owner commit.
- Final `git diff --check`: passed. Immediately before this state-record correction, the repository
  was clean at the repository-owner-created and pushed `2109075`; no commit or push command was run
  by the reviewer.
- GitHub Actions run
  [30727622121](https://github.com/santinomarial/ChronosDB/actions/runs/30727622121) for exact commit
  `2109075`: passed all seven jobs (Linux GCC, Linux Clang, macOS AppleClang, ASan/UBSan, TSan,
  clang-tidy, and format/action-pin verification).

## Checks not performed

- Windows/MSVC and multi-config generator builds: unavailable on the review host.
- x86-64 compilation or execution: neither the Apple host nor the Ubuntu container was x86-64.
- Sustained or corpus-backed fuzzing: only bounded 10,000-run smoke tests were performed.
- Statistically controlled performance measurement: only benchmark validity smoke runs were
  performed, with no publication-quality environment or claim.

## Remaining risks

- The compiler matrix is strong for arm64 but still needs x86-64 and eventual Windows coverage.
- The fuzz target has no checked-in corpus and a short smoke run cannot establish parser safety for
  future durable formats.
- Generated metadata depends on the Git executable and repository being readable at build time. The
  unavailable state is now explicit and tested, but release packaging should continue to record the
  exact source identity independently.
- Phase 1 intentionally remains incomplete beyond the reviewed binary foundations. No conclusion in
  this review applies to future file I/O, clocks, identity generation, logging, WAL, recovery,
  storage, networking, SQL, or concurrency code.

## Conclusion

The confirmed Phase 1 foundation problems were repaired without adding a dependency, redesigning a
subsystem, weakening a warning or sanitizer, or adding Phase 2 functionality. The validation gate is
green. The repository owner committed and pushed the changes during the audit; the reviewer did not
run commit or push commands or rewrite the published history.
