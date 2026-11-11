# sds

A synchronous Rust implementation of the [Scalable Data Sync (SDS) protocol](https://github.com/logos-co/logos-lips/blob/949faa9563027de366514247a2a62b0de2f8c45f/docs/anoncomms/raw/sds.md), including the SDS-R repair extension.

## Usage

```rust
use sds::{Channel, Config, Event, OutgoingMessage};

let mut channel = Channel::new(Config::new("alice", "my-channel"))?;

// Send: broadcast the returned record on your transport.
let record = channel.send(
    OutgoingMessage { message_id: "msg-1".into(), content: b"hi".to_vec(), retrieval_hint: None },
    now_ms,
)?;

// Receive: hand over a record from the transport, act on the events.
for event in channel.receive(incoming_record, None, now_ms)? { /* ... */ }

// Every few seconds: broadcast any Resend / Sync / Repair records.
for event in channel.tick(now_ms, &mut rng) { /* ... */ }
```

The library has no threads, timers or I/O. The caller supplies the time, the randomness and the transport.

An `SDSRecord` is the `Header`, the spec's `Message` without its content, and the content beside it, so a transport can carry the content apart, for instance encrypted. The crate has no wire format of its own: the caller encodes a record for its transport and decodes the ones that arrive.

## Joining a channel under way

A participant joining a channel that already has history sets `config.start` to a `StartPoint`, a message such as the one that admitted it. Its clock starts at that message's timestamp. Nothing stamped at or before that timestamp is released or named in the participant's own histories: such a message is logged on arrival. The start point and the IDs a history lists before it count as delivered, so they are never waited for or requested.

Pick a message every participant delivers before sending anything the joiner is to receive. A message whose history lists older IDs without naming the start point waits for them as usual. A history lists messages with equal timestamps by ID, so one stamped with the start point's own timestamp and a greater ID is listed after it: it is waited for, though never released.

## Interoperability

To interoperate, a caller encodes a record as the spec's protobuf `Message`. The spec leaves the hash functions open, so this crate defines them below. Any implementation joining the same channel must use the same hashes and the same `bloom_capacity`, `bloom_error_rate` and SDS-R settings, and list a causal history oldest first, an order the spec leaves open but a start point relies on.

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
