use prost::Message as _;
use rand::rngs::StdRng;
use rand::SeedableRng;
use sds::proto::Message;
use sds::{Channel, Config, Error, Event, OutgoingMessage, SDSRecord, StartPoint};

const CHANNEL_ID: &str = "chan";
const MINUTE_MS: u64 = 60_000;

fn channel(participant_id: &str) -> Channel {
    Channel::new(Config::new(participant_id, CHANNEL_ID)).unwrap()
}

fn send(channel: &mut Channel, message_id: &str, now_ms: u64) -> SDSRecord {
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

fn sync_record(events: &[Event]) -> SDSRecord {
    events
        .iter()
        .find_map(|event| match event {
            Event::Sync(record) => Some(record.clone()),
            _ => None,
        })
        .expect("tick should produce a sync message")
}

fn joiner(participant_id: &str, start: &SDSRecord) -> Channel {
    let header = Message::decode(start.header.as_slice()).unwrap();
    let mut config = Config::new(participant_id, CHANNEL_ID);
    config.start = Some(StartPoint {
        message_id: header.message_id,
        lamport_timestamp: header.lamport_timestamp.unwrap(),
    });
    Channel::new(config).unwrap()
}

#[test]
fn sends_the_content_beside_the_header() {
    let mut alice = channel("alice");

    let record = send(&mut alice, "m1", 1000);

    assert_eq!(record.content, Some(b"m1".to_vec()));
    let header = Message::decode(record.header.as_slice()).unwrap();
    assert_eq!(header.message_id, "m1");
    assert!(header.content.is_none());
}

#[test]
fn delivers_message_with_met_dependencies() {
    let mut alice = channel("alice");
    let mut bob = channel("bob");

    let events = bob
        .receive(send(&mut alice, "m1", 1000), None, 1000)
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

    let events = bob.receive(m2, None, 2000).unwrap();
    assert!(ready_ids(&events).is_empty());
    assert!(
        matches!(&events[0], Event::MissingDependencies { missing, .. } if missing[0].message_id == "m1")
    );

    let events = bob.receive(m1, None, 2000).unwrap();
    assert_eq!(ready_ids(&events), ["m1", "m2"]);
    assert_eq!(bob.history(), ["m1", "m2"]);
}

#[test]
fn reports_the_sender_of_each_released_message() {
    let mut alice = channel("alice");
    let mut bob = channel("bob");
    let mut carol = channel("carol");
    let from_alice = send(&mut alice, "m1", 1000);
    bob.receive(from_alice.clone(), None, 1000).unwrap();
    let from_bob = send(&mut bob, "m2", 2000);

    carol.receive(from_bob, None, 2000).unwrap();
    let events = carol.receive(from_alice, None, 2000).unwrap();

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

    carol.receive(from_alice, None, 1000).unwrap();
    carol.receive(from_bob, None, 1000).unwrap();

    assert_eq!(carol.history(), ["a-msg", "b-msg"]);
}

#[test]
fn acknowledges_via_causal_history() {
    let mut alice = channel("alice");
    let mut bob = channel("bob");
    bob.receive(send(&mut alice, "m1", 1000), None, 1000)
        .unwrap();

    let events = alice
        .receive(send(&mut bob, "m2", 2000), None, 2000)
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
        peer.receive(m1.clone(), None, 2000).unwrap();
        peer.receive(d1.clone(), None, 2000).unwrap();
        syncs.push(sync_record(&peer.tick(2000 + MINUTE_MS, &mut rng())));
    }

    let first = alice.receive(syncs[0].clone(), None, 3000).unwrap();
    let second = alice.receive(syncs[1].clone(), None, 3000).unwrap();

    let ack = Event::MessageAcknowledged {
        message_id: "m1".into(),
    };
    assert!(!first.contains(&ack));
    assert!(second.contains(&ack));
}

#[test]
fn resends_the_record_as_sent() {
    let mut alice = channel("alice");
    let record = send(&mut alice, "m1", 0);

    let events = alice.tick(MINUTE_MS, &mut rng());

    assert!(events.contains(&Event::Resend(record)));
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
    bob.receive(send(&mut alice, "m2", 2000), None, 2000)
        .unwrap();

    let timeout_ms = Config::new("bob", CHANNEL_ID).dependency_timeout_ms;
    let events = bob.tick(2000 + timeout_ms, &mut rng());

    assert!(events.contains(&Event::Lost {
        message_id: "m2".into()
    }));
}

#[test]
fn sync_messages_are_not_logged() {
    let mut alice = channel("alice");
    let mut bob = channel("bob");
    let sync = sync_record(&bob.tick(MINUTE_MS, &mut rng()));

    assert!(sync.content.is_none());
    let events = alice.receive(sync, None, MINUTE_MS).unwrap();

    assert!(events.is_empty());
    assert!(alice.history().is_empty());
}

#[test]
fn delivers_ephemeral_messages_without_logging() {
    let alice = channel("alice");
    let mut bob = channel("bob");
    let ephemeral = alice.send_ephemeral("e1".into(), b"hi".to_vec()).unwrap();

    let events = bob.receive(ephemeral, None, 1000).unwrap();

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
    assert!(alice.receive(m1, None, 1000).unwrap().is_empty());
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
        alice.receive(wrong_channel, None, 1000),
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
    bob.receive(m1, None, 1000).unwrap();
    let m2 = send(&mut bob, "m2", 2000);
    alice.receive(m2.clone(), None, 2000).unwrap();
    carol.receive(m2, None, 2000).unwrap();

    let request_time_ms = 2000 + 5 * MINUTE_MS;
    let sync = sync_record(&carol.tick(request_time_ms, &mut rng));
    let requested = Message::decode(sync.header.as_slice())
        .unwrap()
        .repair_request;
    assert_eq!(requested[0].message_id, "m1");

    alice.receive(sync, None, request_time_ms).unwrap();
    let repair = alice
        .tick(request_time_ms, &mut rng)
        .into_iter()
        .find_map(|event| match event {
            Event::Repair(record) => Some(record),
            _ => None,
        })
        .expect("original sender should respond immediately");

    let events = carol.receive(repair, None, request_time_ms).unwrap();
    assert_eq!(ready_ids(&events), ["m1", "m2"]);
}

#[test]
fn starts_the_clock_at_the_start_point() {
    let mut alice = channel("alice");
    let start = send(&mut alice, "start", 5000);
    let mut carol = joiner("carol", &start);

    let sent = send(&mut carol, "m1", 1000);

    let header = Message::decode(sent.header.as_slice()).unwrap();
    assert_eq!(header.lamport_timestamp, Some(5001));
}

#[test]
fn never_acknowledges_the_start_point() {
    let mut alice = channel("alice");
    let start = send(&mut alice, "start", 0);
    let mut carol = joiner("carol", &start);
    let mut rng = rng();

    let sync = sync_record(&carol.tick(MINUTE_MS / 2, &mut rng));
    alice.receive(sync, None, MINUTE_MS / 2).unwrap();
    let events = alice.tick(MINUTE_MS, &mut rng);

    assert!(events.contains(&Event::Resend(start)));
}

#[test]
fn names_nothing_from_before_the_start_point() {
    let mut alice = channel("alice");
    let mut bob = channel("bob");
    let b1 = send(&mut bob, "b1", 1000);
    let start = send(&mut alice, "start", 2000);
    bob.receive(start.clone(), None, 2000).unwrap();
    let mut carol = joiner("carol", &start);
    carol.receive(b1, None, 3000).unwrap();
    carol.receive(start, None, 3000).unwrap();
    carol
        .receive(send(&mut bob, "b2", 3000), None, 3000)
        .unwrap();

    let sent = send(&mut carol, "c1", 4000);

    let header = Message::decode(sent.header.as_slice()).unwrap();
    let named: Vec<_> = header
        .causal_history
        .iter()
        .map(|entry| entry.message_id.as_str())
        .collect();
    assert_eq!(named, ["b2"]);
}

#[test]
fn never_releases_the_start_point() {
    let mut alice = channel("alice");
    let start = send(&mut alice, "start", 5000);
    let mut carol = joiner("carol", &start);

    assert!(carol.receive(start, None, 5000).unwrap().is_empty());
}

#[test]
fn never_releases_a_message_stamped_with_the_start_points_timestamp() {
    let mut alice = channel("alice");
    let mut dave = channel("dave");
    let start = send(&mut alice, "start", 2000);
    let tie = send(&mut dave, "tie", 2000);
    let mut carol = joiner("carol", &start);

    assert!(carol.receive(tie, None, 2000).unwrap().is_empty());
}

#[test]
fn releases_a_message_whose_history_names_the_start_point() {
    let mut alice = channel("alice");
    let mut bob = channel("bob");
    bob.receive(send(&mut alice, "m1", 1000), None, 1000)
        .unwrap();
    let start = send(&mut alice, "start", 2000);
    bob.receive(start.clone(), None, 2000).unwrap();
    let mut carol = joiner("carol", &start);

    let events = carol
        .receive(send(&mut bob, "m2", 3000), None, 3000)
        .unwrap();

    assert_eq!(
        events,
        [Event::MessageReady {
            message_id: "m2".into(),
            sender_id: "bob".into(),
            content: b"m2".to_vec()
        }]
    );
    assert_eq!(carol.history(), ["m2"]);
}

#[test]
fn never_requests_what_precedes_the_start_point() {
    let mut alice = channel("alice");
    let mut bob = channel("bob");
    let mut dave = channel("dave");
    let mut rng = rng();
    let m1 = send(&mut alice, "m1", 1000);
    bob.receive(m1.clone(), None, 1000).unwrap();
    dave.receive(m1, None, 1000).unwrap();
    let start = send(&mut alice, "start", 2 * MINUTE_MS);
    bob.receive(start.clone(), None, 2 * MINUTE_MS).unwrap();
    let mut carol = joiner("carol", &start);

    let lagging_sync = sync_record(&dave.tick(MINUTE_MS, &mut rng));
    let sync = sync_record(&bob.tick(3 * MINUTE_MS, &mut rng));
    carol.receive(lagging_sync, None, 3 * MINUTE_MS).unwrap();
    carol.receive(sync, None, 3 * MINUTE_MS).unwrap();

    let request_time_ms = 3 * MINUTE_MS + 5 * MINUTE_MS;
    let carol_sync = sync_record(&carol.tick(request_time_ms, &mut rng));
    let requested = Message::decode(carol_sync.header.as_slice())
        .unwrap()
        .repair_request;
    assert!(requested.is_empty());
}

#[test]
fn waits_on_a_history_that_does_not_name_the_start_point() {
    let mut alice = channel("alice");
    let mut dave = channel("dave");
    dave.receive(send(&mut alice, "m1", 1000), None, 1000)
        .unwrap();
    let start = send(&mut alice, "start", 2000);
    let mut carol = joiner("carol", &start);

    let events = carol
        .receive(send(&mut dave, "d1", 3000), None, 3000)
        .unwrap();

    assert!(ready_ids(&events).is_empty());
    assert!(
        matches!(&events[0], Event::MissingDependencies { missing, .. } if missing[0].message_id == "m1")
    );
}

#[test]
fn logs_without_releasing_a_message_from_before_the_start_point() {
    let mut alice = channel("alice");
    let mut dave = channel("dave");
    dave.receive(send(&mut alice, "m1", 1000), None, 1000)
        .unwrap();
    let d1 = send(&mut dave, "d1", 1500);
    let start = send(&mut alice, "start", 2000);
    let mut carol = joiner("carol", &start);

    let events = carol.receive(d1, None, 2000).unwrap();

    assert!(events.is_empty());
    assert_eq!(carol.history(), ["d1"]);
}

#[test]
fn releases_what_waits_on_a_message_from_before_the_start_point() {
    let mut alice = channel("alice");
    let mut dave = channel("dave");
    let d1 = send(&mut dave, "d1", 1000);
    let d2 = send(&mut dave, "d2", 3000);
    let start = send(&mut alice, "start", 2000);
    let mut carol = joiner("carol", &start);

    carol.receive(d2, None, 3000).unwrap();
    let events = carol.receive(d1, None, 3000).unwrap();

    assert_eq!(ready_ids(&events), ["d2"]);
    assert_eq!(carol.history(), ["d1", "d2"]);
}
