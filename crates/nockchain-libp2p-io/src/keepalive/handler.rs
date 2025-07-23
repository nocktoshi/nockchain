use std::convert::Infallible;
use std::task::{Context, Poll};

use libp2p::core::upgrade::DeniedUpgrade;
use libp2p::swarm::{ConnectionHandler, ConnectionHandlerEvent, SubstreamProtocol};

/// Custom connection handler that implements connection_keep_alive() to return true
/// This prevents connections from being closed due to idle timeout
#[derive(Clone)]
pub struct KeepAliveConnectionHandler;

impl ConnectionHandler for KeepAliveConnectionHandler {
    type FromBehaviour = Infallible;
    type ToBehaviour = Infallible;
    type InboundProtocol = DeniedUpgrade;
    type OutboundProtocol = DeniedUpgrade;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol> {
        SubstreamProtocol::new(DeniedUpgrade, ())
    }

    fn on_behaviour_event(&mut self, _event: Self::FromBehaviour) {
        unreachable!("KeepAliveConnectionHandler doesn't receive behaviour events")
    }

    fn poll(
        &mut self,
        _: &mut Context<'_>,
    ) -> Poll<ConnectionHandlerEvent<Self::OutboundProtocol, (), Self::ToBehaviour>> {
        Poll::Pending
    }

    fn on_connection_event(
        &mut self,
        _event: libp2p::swarm::handler::ConnectionEvent<
            Self::InboundProtocol,
            Self::OutboundProtocol,
        >,
    ) {
        // Since we use DeniedUpgrade, no events should ever be received
        // This is a no-op handler that just keeps connections alive
    }

    /// This is the key method that prevents connection churn
    /// By returning true, we tell libp2p to keep this connection alive
    /// even when there are no active streams or pending operations
    fn connection_keep_alive(&self) -> bool {
        true
    }
}
