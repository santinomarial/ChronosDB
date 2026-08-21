# Raft Transport Envelope v1 golden fixtures

These eight lowercase hexadecimal files cover every message kind in Raft Transport Envelope v1.
They were generated independently from `docs/formats/raft-transport-v1.md` with Python `struct`
little-endian packing and a standalone bitwise CRC32C implementation using reflected Castagnoli
polynomial `0x82f63b78`. The implementation was checked against the standard `123456789` CRC32C
value `0xe3069283`; it did not call ChronosDB codec or checksum code.

All fixtures use group UUID bytes `09 00...00`, source node 1, destination node 2, and term 4.
Variable-shape fixtures additionally use the exact values constructed in
`tests/raft/transport_codec_golden_test.cpp`. The standard compiler CI matrix compares production
encoding byte-for-byte and decodes these immutable bytes under GCC, Clang/libc++, and AppleClang.
