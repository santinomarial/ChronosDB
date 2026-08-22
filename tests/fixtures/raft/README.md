# Raft v1 golden fixtures

These lowercase hexadecimal files were generated independently from the applicable format
specification with Python `struct` little-endian packing and a standalone bitwise CRC32C
implementation using reflected Castagnoli polynomial `0x82f63b78`. The implementation was checked
against the standard `123456789` CRC32C value `0xe3069283`; it did not call ChronosDB codec or
checksum code.

The eight transport files cover every message kind in Raft Transport Envelope v1. They use group
UUID bytes `09 00...00`, source node 1, destination node 2, and term 4. Variable-shape fixtures
additionally use the exact values constructed in `tests/raft/transport_codec_golden_test.cpp`.

The `multiplexed-state-minor-{0,1}.hex` pair describes one full-state record at physical sequence 1
for group UUID bytes `41...41`. Both encode term 4, vote 2, commit 7, apply 6, snapshot `(index=5,
term=3, manifest=9, checksum=a5...a5)`, and retained entries `(6,3,1,1122)` and `(7,4,2,empty)`.
Minor 0 has no membership checkpoint. Minor 1 is its canonical semantic upgrade with configuration
index 0 and voters `{1,2,3}`. The pair independently fixes both historical layout and the current
encoder output.

The `metadata-application-snapshot-minor-{0,1}.hex` pair uses metadata group UUID bytes
`07 00...00`, included index/generation 8, included term 3, configuration index 6, and voters
`{1,2,3}`. Both retain a valid Metadata Command v1 cluster-node entry at `(index=2, term=1)` for
node 7 and endpoint `n`; minor 1 adds a valid Tablet Group Binding v1 entry at `(index=8, term=3)`
for tablet UUID `01 00...00` and group UUID `02 00...00`. Their nested envelopes, application
identities, and outer bytes were independently derived with Python `struct`, `hashlib` SHA-256 over
the `CHRMASN\x01` domain, and the standalone CRC32C implementation described above.

The standard compiler CI matrix compares production encoding byte-for-byte and decodes these
immutable bytes under GCC, Clang/libc++, and AppleClang.
