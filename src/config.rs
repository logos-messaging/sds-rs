use crate::error::Error;

const DEFAULT_MAX_CAUSAL_HISTORY: usize = 10;
const DEFAULT_MAX_HISTORY: usize = 500;
const DEFAULT_BLOOM_CAPACITY: usize = 200;
const DEFAULT_BLOOM_ERROR_RATE: f64 = 0.001;
const DEFAULT_RESEND_INTERVAL_MS: u64 = 60_000;
const DEFAULT_MAX_RESEND_ATTEMPTS: u32 = 5;
const DEFAULT_BLOOM_ACKS_REQUIRED: usize = 2;
const DEFAULT_SYNC_INTERVAL_MS: u64 = 30_000;
const DEFAULT_DEPENDENCY_TIMEOUT_MS: u64 = 600_000;
const DEFAULT_MAX_INCOMING_BUFFER: usize = 500;
const DEFAULT_MAX_CONTENT_SIZE: usize = 64 * 1024;
const DEFAULT_REPAIR_T_MIN_MS: u64 = 30_000;
const DEFAULT_REPAIR_T_MAX_MS: u64 = 300_000;
const DEFAULT_NUM_RESPONSE_GROUPS: u64 = 1;
const DEFAULT_MAX_REPAIR_REQUESTS: usize = 3;

/// Bloom, repair and delivery settings must match across all participants in a channel.
#[derive(Clone, Debug)]
pub struct Config {
    pub participant_id: String,
    pub channel_id: String,
    pub max_causal_history: usize,
    pub max_history: usize,
    pub bloom_capacity: usize,
    pub bloom_error_rate: f64,
    pub resend_interval_ms: u64,
    pub max_resend_attempts: u32,
    pub bloom_acks_required: usize,
    pub sync_interval_ms: u64,
    pub dependency_timeout_ms: u64,
    pub max_incoming_buffer: usize,
    pub max_content_size: usize,
    pub delivery: Delivery,
    pub repair: Option<RepairConfig>,
}

/// When a received message with unmet dependencies is delivered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Delivery {
    /// Held until its dependencies are delivered, as the spec requires.
    Causal,
    /// Delivered at once; its missing dependencies are still reported and, with SDS-R, requested.
    OnArrival,
}

#[derive(Clone, Debug)]
pub struct RepairConfig {
    pub t_min_ms: u64,
    pub t_max_ms: u64,
    pub num_response_groups: u64,
    pub max_repair_requests: usize,
}

impl Config {
    pub fn new(participant_id: impl Into<String>, channel_id: impl Into<String>) -> Self {
        Self {
            participant_id: participant_id.into(),
            channel_id: channel_id.into(),
            max_causal_history: DEFAULT_MAX_CAUSAL_HISTORY,
            max_history: DEFAULT_MAX_HISTORY,
            bloom_capacity: DEFAULT_BLOOM_CAPACITY,
            bloom_error_rate: DEFAULT_BLOOM_ERROR_RATE,
            resend_interval_ms: DEFAULT_RESEND_INTERVAL_MS,
            max_resend_attempts: DEFAULT_MAX_RESEND_ATTEMPTS,
            bloom_acks_required: DEFAULT_BLOOM_ACKS_REQUIRED,
            sync_interval_ms: DEFAULT_SYNC_INTERVAL_MS,
            dependency_timeout_ms: DEFAULT_DEPENDENCY_TIMEOUT_MS,
            max_incoming_buffer: DEFAULT_MAX_INCOMING_BUFFER,
            max_content_size: DEFAULT_MAX_CONTENT_SIZE,
            delivery: Delivery::Causal,
            repair: Some(RepairConfig::default()),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.bloom_capacity == 0 {
            return Err(Error::InvalidConfig("bloom_capacity must be positive"));
        }
        if !(self.bloom_error_rate > 0.0 && self.bloom_error_rate < 1.0) {
            return Err(Error::InvalidConfig(
                "bloom_error_rate must be between 0 and 1",
            ));
        }
        let Some(repair) = &self.repair else {
            return Ok(());
        };
        if repair.t_max_ms <= repair.t_min_ms {
            return Err(Error::InvalidConfig("repair t_max_ms must exceed t_min_ms"));
        }
        if repair.num_response_groups == 0 {
            return Err(Error::InvalidConfig("num_response_groups must be positive"));
        }
        Ok(())
    }
}

impl Default for RepairConfig {
    fn default() -> Self {
        Self {
            t_min_ms: DEFAULT_REPAIR_T_MIN_MS,
            t_max_ms: DEFAULT_REPAIR_T_MAX_MS,
            num_response_groups: DEFAULT_NUM_RESPONSE_GROUPS,
            max_repair_requests: DEFAULT_MAX_REPAIR_REQUESTS,
        }
    }
}
