# sds

A synchronous Rust implementation of the [Scalable Data Sync (SDS) protocol](https://github.com/logos-co/logos-lips/blob/949faa9563027de366514247a2a62b0de2f8c45f/docs/anoncomms/raw/sds.md), including the SDS-R repair extension.

## Usage

```rust
use sds::{Channel, Config, Event, OutgoingMessage};

let mut channel = Channel::new(Config::new("alice", "my-channel"))?;

// Send: broadcast the returned bytes on your transport.
let wire_bytes = channel.send(
    OutgoingMessage { message_id: "msg-1".into(), content: b"hi".to_vec(), retrieval_hint: None },
    now_ms,
)?;

// Receive: hand over bytes from the transport, act on the events.
for event in channel.receive(&incoming_bytes, None, now_ms)? { /* ... */ }

// Every few seconds: broadcast any Resend / Sync / Repair bytes.
for event in channel.tick(now_ms, &mut rng) { /* ... */ }
```

The library has no threads, timers or I/O. The caller supplies the time, the randomness and the transport.

## Delivery

By default a message waits until every message in its causal history is delivered, as the spec requires. `MissingDependencies` names the waiting message, and one still waiting after `dependency_timeout_ms`, or pushed out once more than `max_incoming_buffer` wait, is reported `Lost`.

With `config.delivery = Delivery::OnArrival` a message is delivered and logged when it arrives. `MissingDependencies` then names a message already delivered, and with SDS-R on its missing dependencies are requested for repair. A dependency not received within `dependency_timeout_ms` of the last delivered message naming it, or pushed out once more than `max_incoming_buffer` are missing, is reported `Lost`. That is not final: a late copy is still delivered, and a dependency named again is tracked again. A message that has left the log (`max_history`) counts as missing when a later message names it.

Every participant in a channel must use the same delivery. An `OnArrival` participant names messages whose own dependencies are still missing, and a spec-following peer holds every later message that names them.

## Interoperability

Messages use the spec's protobuf definitions exactly. The spec leaves the hash functions open, so this crate defines them below. Any implementation joining the same channel must use the same hashes, the same `bloom_capacity`, `bloom_error_rate` and SDS-R settings, and the same delivery.

`u64_be(bytes)` reads 8 bytes as a big-endian unsigned integer.

### Bloom filter

- Size: `m = ceil(-capacity * ln(error_rate) / ln(2)^2)` bits, rounded up to whole bytes.
- Hash count: `k = round(m / capacity * ln(2))`, with `m` the byte-aligned bit count. The defaults (200, 0.001) give 360 bytes and `k = 10`.
- For a message ID: `d = SHA-256(message_id)`, `h1 = u64_be(d[0..8])`, `h2 = u64_be(d[8..16])`.
- Bit `i` for `i in 0..k` is `(h1 + i * h2) mod m`, with wrapping 64-bit arithmetic.
- Bit `b` is stored in byte `b / 8` at mask `1 << (b % 8)`.
- On the wire, `bloom_filter` is the raw byte array. Receivers take `m` from its length and `k` from their own config.

### SDS-R

`hash(a, b, ...) = u64_be(SHA-256(len(a) || a || len(b) || b || ...)[0..8])`, where `len` is the field's UTF-8 byte length as a big-endian u32.

- `T_req = now + hash(participant_id, message_id) % (T_max - T_min) + T_min`
- `T_resp = now + (hash(participant_id) XOR hash(sender_id)) * hash(message_id) % T_max`, with a wrapping 64-bit multiply.
- Response group: `hash(participant_id, message_id) % num_response_groups`.

### Test vectors

| Input | Output |
| --- | --- |
| `hash("alice")` | `0xe3e40cc57896dcda` |
| `hash("alice", "msg-1")` | `0x653f909d8e44fca9` |
| bloom `(h1, h2)` for `"msg-1"` | `(0x3a0de37932e8b197, 0x46f20b22414f862f)` |

## License

MIT OR Apache-2.0
