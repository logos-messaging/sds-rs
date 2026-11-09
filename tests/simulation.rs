use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use sds::{Channel, Config, Event, OutgoingMessage, SDSRecord};

const PARTICIPANTS: [&str; 3] = ["alice", "bob", "carol"];
const STEP_MS: u64 = 5_000;
const SENDING_STEPS: u64 = 60;
const SETTLING_STEPS: u64 = 400;
const DROP_PERCENT: u64 = 10;

struct Network {
    channels: Vec<Channel>,
    in_flight: Vec<(usize, SDSRecord)>,
    rng: StdRng,
}

impl Network {
    fn new(seed: u64) -> Self {
        let channels = PARTICIPANTS
            .iter()
            .map(|id| Channel::new(Config::new(*id, "chan")).unwrap())
            .collect();
        Self {
            channels,
            in_flight: Vec::new(),
            rng: StdRng::seed_from_u64(seed),
        }
    }

    fn broadcast(&mut self, from: usize, record: SDSRecord) {
        for to in 0..self.channels.len() {
            if to != from {
                self.in_flight.push((to, record.clone()));
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
        let record = self.channels[from].send(message, step * STEP_MS).unwrap();
        self.broadcast(from, record);
    }

    fn tick(&mut self, now_ms: u64) {
        for from in 0..self.channels.len() {
            for event in self.channels[from].tick(now_ms, &mut self.rng) {
                if let Event::Resend(record) | Event::Sync(record) | Event::Repair(record) = event {
                    self.broadcast(from, record);
                }
            }
        }
    }

    fn deliver_with_loss(&mut self, now_ms: u64) {
        let mut records = std::mem::take(&mut self.in_flight);
        shuffle(&mut records, &mut self.rng);
        for (to, record) in records {
            if self.rng.next_u64() % 100 < DROP_PERCENT {
                continue;
            }
            self.channels[to].receive(record, None, now_ms).unwrap();
        }
    }
}

fn shuffle<T>(items: &mut [T], rng: &mut StdRng) {
    for i in (1..items.len()).rev() {
        let j = (rng.next_u64() % (i as u64 + 1)) as usize;
        items.swap(i, j);
    }
}

#[test]
fn participants_converge_despite_loss_and_reordering() {
    let mut network = Network::new(42);

    for step in 0..SENDING_STEPS + SETTLING_STEPS {
        let now_ms = step * STEP_MS;
        if step < SENDING_STEPS {
            network.send((step as usize) % PARTICIPANTS.len(), step);
        }
        network.tick(now_ms);
        network.deliver_with_loss(now_ms);
    }

    let histories: Vec<Vec<String>> = network.channels.iter().map(Channel::history).collect();
    assert_eq!(histories[0].len(), SENDING_STEPS as usize);
    assert!(histories.iter().all(|history| *history == histories[0]));
}
