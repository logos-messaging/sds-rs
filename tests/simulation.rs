use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use sds::{Channel, Config, Delivery, Event, OutgoingMessage};

const PARTICIPANTS: [&str; 3] = ["alice", "bob", "carol"];
const STEP_MS: u64 = 5_000;
const SENDING_STEPS: u64 = 60;
const SETTLING_STEPS: u64 = 400;
const DROP_PERCENT: u64 = 10;

struct Network {
    channels: Vec<Channel>,
    delivered: Vec<Vec<String>>,
    lost: Vec<String>,
    in_flight: Vec<(usize, Vec<u8>)>,
    rng: StdRng,
}

impl Network {
    fn new(seed: u64, delivery: Delivery) -> Self {
        let channels = PARTICIPANTS
            .iter()
            .map(|id| {
                let mut config = Config::new(*id, "chan");
                config.delivery = delivery;
                Channel::new(config).unwrap()
            })
            .collect();
        Self {
            channels,
            delivered: vec![Vec::new(); PARTICIPANTS.len()],
            lost: Vec::new(),
            in_flight: Vec::new(),
            rng: StdRng::seed_from_u64(seed),
        }
    }

    fn broadcast(&mut self, from: usize, wire_bytes: Vec<u8>) {
        for to in 0..self.channels.len() {
            if to != from {
                self.in_flight.push((to, wire_bytes.clone()));
            }
        }
    }

    fn send(&mut self, from: usize, step: u64) {
        let message_id = format!("{}-{step}", PARTICIPANTS[from]);
        let message = OutgoingMessage {
            message_id: message_id.clone(),
            content: message_id.into_bytes(),
            retrieval_hint: None,
        };
        let wire_bytes = self.channels[from].send(message, step * STEP_MS).unwrap();
        self.broadcast(from, wire_bytes);
    }

    fn tick(&mut self, now_ms: u64) {
        for from in 0..self.channels.len() {
            for event in self.channels[from].tick(now_ms, &mut self.rng) {
                match event {
                    Event::Resend(bytes) | Event::Sync(bytes) | Event::Repair(bytes) => {
                        self.broadcast(from, bytes)
                    }
                    Event::Lost { message_id } => self.lost.push(message_id),
                    _ => {}
                }
            }
        }
    }

    fn deliver_with_loss(&mut self, now_ms: u64) {
        let mut packets = std::mem::take(&mut self.in_flight);
        shuffle(&mut packets, &mut self.rng);
        for (to, wire_bytes) in packets {
            if self.rng.next_u64() % 100 < DROP_PERCENT {
                continue;
            }
            let events = self.channels[to]
                .receive(&wire_bytes, None, now_ms)
                .unwrap();
            for event in events {
                match event {
                    Event::MessageReady { message_id, .. } => self.delivered[to].push(message_id),
                    Event::Lost { message_id } => self.lost.push(message_id),
                    _ => {}
                }
            }
        }
    }
}

fn shuffle<T>(items: &mut [T], rng: &mut StdRng) {
    for i in (1..items.len()).rev() {
        let j = (rng.next_u64() % (i as u64 + 1)) as usize;
        items.swap(i, j);
    }
}

fn run_with_loss_and_reordering(delivery: Delivery) -> Network {
    let mut network = Network::new(42, delivery);
    for step in 0..SENDING_STEPS + SETTLING_STEPS {
        let now_ms = step * STEP_MS;
        if step < SENDING_STEPS {
            network.send((step as usize) % PARTICIPANTS.len(), step);
        }
        network.tick(now_ms);
        network.deliver_with_loss(now_ms);
    }
    network
}

fn assert_converged_and_delivered_once(network: &Network) {
    let histories: Vec<Vec<String>> = network.channels.iter().map(Channel::history).collect();
    assert_eq!(histories[0].len(), SENDING_STEPS as usize);
    assert!(histories.iter().all(|history| *history == histories[0]));
    for (participant, delivered) in network.delivered.iter().enumerate() {
        let mut from_others: Vec<&String> = histories[0]
            .iter()
            .filter(|message_id| !message_id.starts_with(PARTICIPANTS[participant]))
            .collect();
        let mut delivered: Vec<&String> = delivered.iter().collect();
        from_others.sort();
        delivered.sort();
        assert_eq!(delivered, from_others);
    }
    assert!(network.lost.is_empty());
}

#[test]
fn participants_converge_despite_loss_and_reordering() {
    assert_converged_and_delivered_once(&run_with_loss_and_reordering(Delivery::Causal));
}

#[test]
fn participants_converge_when_delivering_on_arrival() {
    assert_converged_and_delivered_once(&run_with_loss_and_reordering(Delivery::OnArrival));
}
