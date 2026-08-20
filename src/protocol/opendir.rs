use super::{impl_packet_for, impl_request_id, Packet, RequestId};

/// Implementation for `SSH_FXP_OPENDIR`
#[derive(Debug, Serialize, Deserialize)]
pub struct OpenDir {
    pub id: u32,
    #[serde(with = "serde_bytes")]
    pub path: Vec<u8>,
}

impl_request_id!(OpenDir);
impl_packet_for!(OpenDir);
