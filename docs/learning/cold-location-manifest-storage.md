# Durable cold-location manifests

## Purpose and public boundary

Cold Location Manifest v1 answers one narrow question: for an immutable CSEG part already described
by Manifest v2, which configured object-store key contains those exact bytes? The cold manifest is a
separate full-generation registry so frozen Manifest v2 bytes do not acquire hidden location
semantics. `cold_manifest.hpp` owns the canonical codec and pure validators;
`cold_manifest_storage.hpp` owns filesystem durability and restart selection.

The important public operations are:

- encode/decode a bounded checksummed generation;
- validate its database, generation, part length, and SHA-256 against one decoded Manifest v2;
- validate an add-only cold successor;
- install generation one or the exact next generation; and
- recover the highest consecutive generation only when it binds to the supplied Manifest v2.

## Data structures and invariants

Each descriptor contains part UUID, complete-file length, SHA-256, and a bounded UTF-8 object key.
Descriptors are sorted by part UUID; part IDs and keys are unique. The header binds the database,
deployment object-store UUID, cold generation, predecessor generation, and exact base Manifest v2
generation. CRC32C covers the header, each fixed descriptor, and the whole file.

Manifest v2 remains logical authority. A cold descriptor is usable only when the part UUID, length,
and SHA-256 all match that exact base generation. A normal cold successor is add-only: it cannot
remove, rekey, or alter an existing location, change the database/store, skip a generation, or move
its base generation backward.

## Ownership, lifetime, and synchronization

`ColdLocationManifestStorage` is move-only, single-threaded, and owns one directory descriptor plus
its advisory `LOCK` for its complete lifetime. The deployment must prevent out-of-band mutation.
Decoded loaded values own their key strings and encoded bytes; they do not borrow a caller buffer.

Installation ordering is deliberately conservative:

1. decode and bind the candidate before filesystem mutation;
2. load and validate the predecessor when one exists;
3. exclusively create the canonical temporary;
4. write, exact-read, decode, compare, and bind the readback;
5. synchronize and close the file;
6. atomically rename without replacement; and
7. synchronize the directory.

Only step 7 completes the durability promise. A failure there poisons the live owner because restart
may observe either namespace outcome. A retry after restart accepts an existing generation only if
all bytes match exactly.

## Recovery and failure behavior

Ownership acquisition removes only recognized regular `.tmp` files and synchronizes the directory
when it changed. Unknown names, symlinks, non-regular entries, and generation gaps fail closed.
Selection reads the highest final generation and never searches lower generations after decode,
checksum, identity, or binding failure. This prevents silent metadata rollback.

An empty directory means no cold authority and therefore local-only operation. A valid cold
generation does not itself make a local file deletable. Query publication must atomically pin a
compatible Manifest-v2/cold pair; later reclamation must wait for every older reader and verify the
remaining source before unlinking anything.

## Complexity and tradeoffs

Encoding and binding are linear in descriptor count and key bytes. Installation adds one complete
write and readback plus two synchronization calls. Recovery enumerates generation names and reads
only the selected generation. Full snapshots and retained generations amplify space, but avoid a
mutable pointer, edit-log repair, or listing-derived truth.

The object-store UUID intentionally names deployment configuration rather than embedding endpoints
or credentials. This keeps secrets out of durable bytes, but operators must preserve a stable UUID
to configuration mapping.

## Verification and likely interview questions

Focused tests exercise canonical round trips, every truncation, hostile versions and checksums,
binding mismatches, exclusive locks, idempotent installation, restart selection, temporary cleanup,
add-only rejection, corrupt-highest no-fallback behavior, and injected directory-sync poisoning.

Useful design questions include:

- Why is a separate cold manifest safer than reusing Manifest v2 reserved bytes?
- Why does recovery choose the highest generation instead of the highest valid generation?
- Why are both file sync and directory sync required?
- Why can a valid cold manifest still not authorize local deletion?
- What publication object must own the Manifest-v2/cold compatibility pair and reader pins?
