use super::{impl_packet_for, impl_request_id, Packet, RequestId};

/// Implementation for `SSH_FXP_LSTAT`
#[derive(Debug, Serialize, Deserialize)]
pub struct Lstat {
    pub id: u32,
    #[serde(with = "serde_bytes")]
    pub path: Vec<u8>,
}

impl_request_id!(Lstat);
impl_packet_for!(Lstat);
