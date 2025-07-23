use std::net::{Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

use libp2p::PeerId;
use nockapp::{AtomExt, NockAppError};
use nockvm::noun::Noun;
use tracing::warn;

// The warn logs are specifically constructed for fail2ban
// Changing these breaks the integration with the fail2ban regex
pub fn log_fail2ban_ipv4(peer_id: &PeerId, ip: &Ipv4Addr) {
    warn!("fail2ban: Blocked peer {peer_id} with IPv4 address: {ip}");
}
pub fn log_fail2ban_ipv6(peer_id: &PeerId, ip: &Ipv6Addr) {
    warn!("fail2ban: Blocked peer {peer_id} with IPv6 address: {ip}");
}

pub trait PeerIdExt {
    fn from_noun(noun: Noun) -> Result<PeerId, NockAppError>;
}

impl PeerIdExt for PeerId {
    fn from_noun(noun: Noun) -> Result<PeerId, NockAppError> {
        let peer_id_bytes = noun.as_atom()?.to_bytes_until_nul()?;
        let peer_id_str = String::from_utf8(peer_id_bytes)?;
        PeerId::from_str(&peer_id_str).map_err(|_| NockAppError::OtherError)
    }
}
