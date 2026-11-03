use prost::Message as _;
use rand::rngs::StdRng;
use rand::SeedableRng;
use sds::proto::{HistoryEntry, Message};
use sds::{Channel, Config, Delivery, Error, Event, OutgoingMessage};

const CHANNEL_ID: &str = "chan";
const MINUTE_MS: u64 = 60_000;
const TICK_MS: u64 = 5_000;

fn channel(participant_id: &str) -> Channel {
    Channel::new(Config::new(participant_id, CHANNEL_ID)).unwrap()
}

fn channel_delivering_on_arrival(participant_id: &str) -> Channel {
    let mut config = Config::new(participant_id, CHANNEL_ID);
    config.delivery = Delivery::OnArrival;
    Channel::new(config).unwrap()
}

fn send(channel: &mut Channel, message_id: &str, now_ms: u64) -> Vec<u8> {
    let message = OutgoingMessage {
        message_id: message_id.into(),
        content: message_id.as_bytes().to_vec(),
        retrieval_hint: None,
    };
    channel.send(message, now_ms).unwrap()
}

fn rng() -> StdRng {
    StdRng::seed_from_u64(7)
}

fn ready_ids(events: &[Event]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::MessageReady { message_id, .. } => Some(message_id.clone()),
            _ => None,
        })
        .collect()
}

fn lost_ids(events: &[Event]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::Lost { message_id } => Some(message_id.clone()),
            _ => None,
        })
        .collect()
}

fn sync_bytes(events: &[Event]) -> Vec<u8> {
    events
        .iter()
        .find_map(|event| match event {
            Event::Sync(bytes) => Some(bytes.clone()),
            _ => None,
        })
        .expect("tick should produce a sync message")
}

#[test]
fn delivers_message_with_met_dependencies() {
    let mut alice = channel("alice");
    let mut bob = channel("bob");

    let events = bob
        .receive(&send(&mut alice, "m1", 1000), None, 1000)
        .unwrap();

    assert_eq!(
        events,
        [Event::MessageReady {
            message_id: "m1".into(),
            sender_id: "alice".into(),
            content: b"m1".to_vec()
        }]
    );
    assert_eq!(bob.history(), ["m1"]);
}

#[test]
fn buffers_until_dependencies_arrive() {
    let mut alice = channel("alice");
    let mut bob = channel("bob");
    let m1 = send(&mut alice, "m1", 1000);
    let m2 = send(&mut alice, "m2", 2000);

    let events = bob.receive(&m2, None, 2000).unwrap();
    assert!(ready_ids(&events).is_empty());
    assert!(
        matches!(&events[0], Event::MissingDependencies { missing, .. } if missing[0].message_id == "m1")
    );

    let events = bob.receive(&m1, None, 2000).unwrap();
    assert_eq!(ready_ids(&events), ["m1", "m2"]);
    assert_eq!(bob.history(), ["m1", "m2"]);
}

#[test]
fn reports_the_sender_of_each_released_message() {
    let mut alice = channel("alice");
    let mut bob = channel("bob");
    let mut carol = channel("carol");
    let from_alice = send(&mut alice, "m1", 1000);
    bob.receive(&from_alice, None, 1000).unwrap();
    let from_bob = send(&mut bob, "m2", 2000);

    carol.receive(&from_bob, None, 2000).unwrap();
    let events = carol.receive(&from_alice, None, 2000).unwrap();

    let senders: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Event::MessageReady {
                message_id,
                sender_id,
                ..
            } => Some((message_id.as_str(), sender_id.as_str())),
            _ => None,
        })
        .collect();
    assert_eq!(senders, [("m1", "alice"), ("m2", "bob")]);
}

#[test]
fn orders_equal_timestamps_by_message_id() {
    let mut alice = channel("alice");
    let mut bob = channel("bob");
    let mut carol = channel("carol");
    let from_alice = send(&mut alice, "b-msg", 1000);
    let from_bob = send(&mut bob, "a-msg", 1000);

    carol.receive(&from_alice, None, 1000).unwrap();
    carol.receive(&from_bob, None, 1000).unwrap();

    assert_eq!(carol.history(), ["a-msg", "b-msg"]);
}

#[test]
fn acknowledges_via_causal_history() {
    let mut alice = channel("alice");
    let mut bob = channel("bob");
    bob.receive(&send(&mut alice, "m1", 1000), None, 1000)
        .unwrap();

    let events = alice
        .receive(&send(&mut bob, "m2", 2000), None, 2000)
        .unwrap();

    assert!(events.contains(&Event::MessageAcknowledged {
        message_id: "m1".into()
    }));
}

#[test]
fn acknowledges_via_bloom_filters_from_two_senders() {
    let mut alice = channel("alice");
    let mut dave = channel("dave");
    let m1 = send(&mut alice, "m1", 1000);
    let d1 = send(&mut dave, "d1", 2000);
    let mut syncs = Vec::new();
    for participant_id in ["bob", "carol"] {
        let mut config = Config::new(participant_id, CHANNEL_ID);
        config.max_causal_history = 1;
        let mut peer = Channel::new(config).unwrap();
        peer.receive(&m1, None, 2000).unwrap();
        peer.receive(&d1, None, 2000).unwrap();
        syncs.push(sync_bytes(&peer.tick(2000 + MINUTE_MS, &mut rng())));
    }

    let first = alice.receive(&syncs[0], None, 3000).unwrap();
    let second = alice.receive(&syncs[1], None, 3000).unwrap();

    let ack = Event::MessageAcknowledged {
        message_id: "m1".into(),
    };
    assert!(!first.contains(&ack));
    assert!(second.contains(&ack));
}

#[test]
fn resends_then_reports_failure() {
    let mut alice = channel("alice");
    send(&mut alice, "m1", 0);
    let mut rng = rng();

    let mut resends = 0;
    for minute in 1..=5 {
        let events = alice.tick(minute * MINUTE_MS, &mut rng);
        resends += events
            .iter()
            .filter(|event| matches!(event, Event::Resend(_)))
            .count();
    }
    let events = alice.tick(6 * MINUTE_MS, &mut rng);

    assert_eq!(resends, 5);
    assert!(events.contains(&Event::SendFailed {
        message_id: "m1".into()
    }));
}

#[test]
fn drops_buffered_message_after_timeout() {
    let mut alice = channel("alice");
    let mut bob = channel("bob");
    send(&mut alice, "m1", 1000);
    bob.receive(&send(&mut alice, "m2", 2000), None, 2000)
        .unwrap();

    let timeout_ms = Config::new("bob", CHANNEL_ID).dependency_timeout_ms;
    let events = bob.tick(2000 + timeout_ms, &mut rng());

    assert!(events.contains(&Event::Lost {
        message_id: "m2".into()
    }));
}

#[test]
fn delivers_on_arrival_ahead_of_dependencies() {
    let mut alice = channel("alice");
    let mut bob = channel_delivering_on_arrival("bob");
    let m1 = send(&mut alice, "m1", 1000);
    let m2 = send(&mut alice, "m2", 2000);

    let events = bob.receive(&m2, None, 2000).unwrap();
    assert_eq!(ready_ids(&events), ["m2"]);
    assert!(
        matches!(&events[1], Event::MissingDependencies { message_id, missing } if message_id == "m2" && missing[0].message_id == "m1")
    );

    let events = bob.receive(&m1, None, 2000).unwrap();
    assert_eq!(ready_ids(&events), ["m1"]);
    assert_eq!(bob.history(), ["m1", "m2"]);
}

#[test]
fn reports_missing_dependency_lost_after_timeout() {
    let mut alice = channel("alice");
    let mut bob = channel_delivering_on_arrival("bob");
    send(&mut alice, "m1", 1000);
    bob.receive(&send(&mut alice, "m2", 2000), None, 2000)
        .unwrap();

    let timeout_ms = Config::new("bob", CHANNEL_ID).dependency_timeout_ms;
    let events = bob.tick(2000 + timeout_ms, &mut rng());

    assert_eq!(lost_ids(&events), ["m1"]);
}

#[test]
fn restarts_wait_when_dependency_is_named_again() {
    let mut alice = channel("alice");
    let mut bob = channel_delivering_on_arrival("bob");
    let mut rng = rng();
    send(&mut alice, "m1", 1000);
    bob.receive(&send(&mut alice, "m2", 2000), None, 2000)
        .unwrap();
    let named_again_ms = 2000 + 8 * MINUTE_MS;
    bob.receive(
        &send(&mut alice, "m3", named_again_ms),
        None,
        named_again_ms,
    )
    .unwrap();

    let timeout_ms = Config::new("bob", CHANNEL_ID).dependency_timeout_ms;
    assert!(lost_ids(&bob.tick(2000 + timeout_ms, &mut rng)).is_empty());
    assert_eq!(
        lost_ids(&bob.tick(named_again_ms + timeout_ms, &mut rng)),
        ["m1"]
    );
}

#[test]
fn drops_oldest_missing_dependency_past_capacity() {
    let mut alice = channel("alice");
    let mut carol = channel("carol");
    let mut config = Config::new("bob", CHANNEL_ID);
    config.delivery = Delivery::OnArrival;
    config.max_incoming_buffer = 1;
    let mut bob = Channel::new(config).unwrap();
    send(&mut alice, "m1", 1000);
    send(&mut carol, "c1", 1000);
    bob.receive(&send(&mut alice, "m2", 2000), None, 2000)
        .unwrap();

    let events = bob
        .receive(&send(&mut carol, "c2", 2000), None, 2000)
        .unwrap();

    assert_eq!(lost_ids(&events), ["m1"]);
}

#[test]
fn ignores_a_message_naming_itself_as_dependency() {
    let mut alice = channel("alice");
    let mut config = Config::new("bob", CHANNEL_ID);
    config.delivery = Delivery::OnArrival;
    config.max_incoming_buffer = 1;
    let mut bob = Channel::new(config).unwrap();
    let mut naming_itself = Message::decode(&send(&mut alice, "m1", 1000)[..]).unwrap();
    naming_itself.causal_history.push(HistoryEntry {
        message_id: "m1".into(),
        retrieval_hint: None,
        sender_id: None,
    });
    let events = bob
        .receive(&naming_itself.encode_to_vec(), None, 1000)
        .unwrap();
    assert_eq!(
        events,
        [Event::MessageReady {
            message_id: "m1".into(),
            sender_id: "alice".into(),
            content: b"m1".to_vec()
        }]
    );

    let timeout_ms = Config::new("bob", CHANNEL_ID).dependency_timeout_ms;
    assert!(lost_ids(&bob.tick(1000 + timeout_ms, &mut rng())).is_empty());
}

#[test]
fn forgets_missing_dependency_once_it_arrives() {
    let mut alice = channel("alice");
    let mut bob = channel_delivering_on_arrival("bob");
    let m1 = send(&mut alice, "m1", 1000);
    bob.receive(&send(&mut alice, "m2", 2000), None, 2000)
        .unwrap();
    bob.receive(&m1, None, 3000).unwrap();

    let timeout_ms = Config::new("bob", CHANNEL_ID).dependency_timeout_ms;
    let events = bob.tick(2000 + timeout_ms, &mut rng());

    assert!(lost_ids(&events).is_empty());
}

#[test]
fn keeps_requesting_repair_of_missing_dependency() {
    let mut alice = channel("alice");
    let mut bob = channel_delivering_on_arrival("bob");
    let mut rng = rng();
    send(&mut alice, "m1", 1000);
    bob.receive(&send(&mut alice, "m2", 2000), None, 2000)
        .unwrap();

    let timeout_ms = Config::new("bob", CHANNEL_ID).dependency_timeout_ms;
    let requests = (1..timeout_ms / TICK_MS)
        .flat_map(|step| bob.tick(2000 + step * TICK_MS, &mut rng))
        .filter_map(|event| match event {
            Event::Sync(bytes) => Some(Message::decode(&bytes[..]).unwrap().repair_request),
            _ => None,
        })
        .filter(|requested| requested.iter().any(|entry| entry.message_id == "m1"))
        .count();

    assert!(requests >= 2);
}

#[test]
fn sync_messages_are_not_logged() {
    let mut alice = channel("alice");
    let mut bob = channel("bob");
    let sync = sync_bytes(&bob.tick(MINUTE_MS, &mut rng()));

    let events = alice.receive(&sync, None, MINUTE_MS).unwrap();

    assert!(events.is_empty());
    assert!(alice.history().is_empty());
    assert!(Message::decode(&sync[..]).unwrap().content.is_none());
}

#[test]
fn delivers_ephemeral_messages_without_logging() {
    let alice = channel("alice");
    let mut bob = channel("bob");
    let ephemeral = alice.send_ephemeral("e1".into(), b"hi".to_vec()).unwrap();

    let events = bob.receive(&ephemeral, None, 1000).unwrap();

    assert_eq!(
        events,
        [Event::MessageReady {
            message_id: "e1".into(),
            sender_id: "alice".into(),
            content: b"hi".to_vec()
        }]
    );
    assert!(bob.history().is_empty());
}

#[test]
fn ignores_own_messages() {
    let mut alice = channel("alice");
    let m1 = send(&mut alice, "m1", 1000);
    assert!(alice.receive(&m1, None, 1000).unwrap().is_empty());
}

#[test]
fn rejects_invalid_messages() {
    let mut alice = channel("alice");
    let mut other = Channel::new(Config::new("bob", "other")).unwrap();
    let empty = OutgoingMessage {
        message_id: "m1".into(),
        content: Vec::new(),
        retrieval_hint: None,
    };

    assert!(matches!(alice.send(empty, 1000), Err(Error::EmptyContent)));
    let wrong_channel = send(&mut other, "m1", 1000);
    assert!(matches!(
        alice.receive(&wrong_channel, None, 1000),
        Err(Error::WrongChannel { .. })
    ));
}

#[test]
fn repairs_message_missed_by_one_participant() {
    let mut alice = channel("alice");
    let mut bob = channel("bob");
    let mut carol = channel("carol");
    let mut rng = rng();
    let m1 = send(&mut alice, "m1", 1000);
    bob.receive(&m1, None, 1000).unwrap();
    let m2 = send(&mut bob, "m2", 2000);
    alice.receive(&m2, None, 2000).unwrap();
    carol.receive(&m2, None, 2000).unwrap();

    let request_time_ms = 2000 + 5 * MINUTE_MS;
    let sync = sync_bytes(&carol.tick(request_time_ms, &mut rng));
    let requested = Message::decode(&sync[..]).unwrap().repair_request;
    assert_eq!(requested[0].message_id, "m1");

    alice.receive(&sync, None, request_time_ms).unwrap();
    let repair = alice
        .tick(request_time_ms, &mut rng)
        .into_iter()
        .find_map(|event| match event {
            Event::Repair(bytes) => Some(bytes),
            _ => None,
        })
        .expect("original sender should respond immediately");

    let events = carol.receive(&repair, None, request_time_ms).unwrap();
    assert_eq!(ready_ids(&events), ["m1", "m2"]);
}
