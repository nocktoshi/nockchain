use libp2p::swarm::NetworkBehaviour;

use crate::keepalive::handler::KeepAliveConnectionHandler;

/// Custom behavior that uses KeepAliveConnectionHandler to prevent connection churn
/// This behavior doesn't handle any protocols but keeps connections alive
pub struct KeepAliveBehaviour;

impl NetworkBehaviour for KeepAliveBehaviour {
    type ConnectionHandler = KeepAliveConnectionHandler;
    type ToSwarm = ();

    fn handle_established_inbound_connection(
        &mut self,
        _: libp2p::swarm::ConnectionId,
        _: libp2p::identity::PeerId,
        _: &libp2p::Multiaddr,
        _: &libp2p::Multiaddr,
    ) -> Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        Ok(KeepAliveConnectionHandler)
    }

    fn handle_established_outbound_connection(
        &mut self,
        _: libp2p::swarm::ConnectionId,
        _: libp2p::identity::PeerId,
        _: &libp2p::Multiaddr,
        _: libp2p::core::Endpoint,
        _: libp2p::core::transport::PortUse,
    ) -> Result<libp2p::swarm::THandler<Self>, libp2p::swarm::ConnectionDenied> {
        Ok(KeepAliveConnectionHandler)
    }

    fn on_swarm_event(&mut self, _event: libp2p::swarm::behaviour::FromSwarm) {}

    fn on_connection_handler_event(
        &mut self,
        _: libp2p::identity::PeerId,
        _: libp2p::swarm::ConnectionId,
        _event: libp2p::swarm::THandlerOutEvent<Self>,
    ) {
        // No events to handle
    }

    fn poll(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<libp2p::swarm::ToSwarm<Self::ToSwarm, libp2p::swarm::THandlerInEvent<Self>>>
    {
        std::task::Poll::Pending
    }
}
