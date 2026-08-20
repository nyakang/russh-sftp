use super::{impl_packet_for, impl_request_id, Packet, RequestId};

/// Implementation for `SSH_FXP_STAT`
#[derive(Debug, Serialize, Deserialize)]
pub struct Stat {
    pub id: u32,
    #[serde(with = "serde_bytes")]
    pub path: Vec<u8>,
}

impl_request_id!(Stat);
impl_packet_for!(Stat);
