
## Target Plan

In this repo we want a rust implementation of SDS. The specification is [here](https://github.com/logos-co/logos-lips/blob/949faa9563027de366514247a2a62b0de2f8c45f/docs/anoncomms/raw/sds.md?plain=1) however it mostly contains the interop definitions.

There is an existing nim implementation here https://github.com/logos-messaging/nim-sds. Use it as a reference for behaviour and defaults only — its wire format diverges from the spec and we do not aim to interoperate with it.


## Requirements

- Runs natively in rust
- runs synchronously. Async is not allowed.
- Code documentation is minimal
- Rust library crate only. No FFI / C bindings.


## Decisions

### Wire format
- Match the spec's protobuf definitions exactly (field numbers, types, optionality).
- Use `prost` with hand-written derive structs, so no `build.rs` / `protoc` is required.
- `bloom_filter` is the raw bit array. The bit count `m` is `bytes.len() * 8`; the hash count `k` is part of the channel config and must be shared by all participants.

### Hashing
All protocol hashes are SHA-256 based so any implementation can reproduce them. Document both in the crate README.
- **Bloom filter:** Kirsch–Mitzenmacher double hashing. `h = SHA-256(message_id)`, `h1 = u64_be(h[0..8])`, `h2 = u64_be(h[8..16])`, bit `i = (h1 + i * h2) mod m` for `i in 0..k`.
- **SDS-R `hash(a, b, ...)`:** `u64_be(SHA-256(len_be32(a) || a || len_be32(b) || b ...)[0..8])`. The length prefixes stop `("ab","c")` and `("a","bc")` colliding. Used for `T_req`, `T_resp` and response groups.

### Scope
- Core SDS: send, receive, causal-dependency buffering, delivery ordered by lamport timestamp then message ID, ack review (causal history = acknowledged, bloom hit = possibly acknowledged), outgoing resend, periodic sync messages, ephemeral messages.
- Rolling bloom filter, rebuilt once it reaches capacity.
- SDS-R (repair) is included, behind a config flag to turn it off.

### Shape
- One `Channel` instance per channel ID. Multiple channels = multiple instances.
- No threads, timers or async. Every operation that depends on time takes `now_ms: u64` from the caller.
- Periodic work is driven by the caller calling `tick(now_ms)`.
- Operations return events instead of invoking callbacks. Events: message ready, message sent/acked, missing dependencies, resend, sync due, repair ready, irretrievably lost.
- Storage sits behind a trait. Ship an in-memory implementation only; replacing it must not require touching protocol logic.
- Defaults: causal history 10, local history 500, bloom 200 @ 0.1%, resend 60s, T_min 30s, T_max 300s, max 3 repair requests.
- Message IDs are supplied by the caller (e.g. the transport's message hash).
- Retrieval hints are optional, supplied by the caller on send and receive, and stored for use in later causal history entries.
- Resends, sync messages and repair rebroadcasts are returned from `tick` as encoded bytes ready to broadcast.
- Randomness (sync backoff) comes from a caller-supplied RNG (`rand_core::RngCore`).

### Protocol policy
- A possibly-acknowledged message becomes acknowledged once it appears in bloom filters from 2 distinct senders.
- An outgoing message is dropped after 5 resend attempts, emitting a `SendFailed` event.
- An incoming message with unmet dependencies is dropped after 600s (2 × `T_max`, so at least one repair round can complete), emitting a `Lost` event. The incoming buffer is capped at 500.
- SDS-R caches full message bytes only for messages this participant is an eligible responder for. Max message size is 64 KB.

### Project setup
- Crate `sds`, Rust edition 2021, latest stable compiler, MIT/Apache-2.0 dual licence.
- Dependencies: `prost`, `sha2`, `rand_core`.


## Done Criteria

- `cargo test` passes, `cargo clippy` is clean, `cargo fmt` applied.
- Unit tests for each protocol step in the spec (send, receive, deliver, conflict resolution, ack review, buffer sweeps, sync, ephemeral, every SDS-R rule).
- Multi-participant simulation tests with message loss and reordering show that all participants converge to the same log.
- Protobuf encode/decode round-trips, and encoded bytes match the spec's field numbers.
- Hash test vectors are fixed in tests and in the README.

Out of scope: nim-sds interop, persistence backends beyond in-memory, FFI.
