use std::net::SocketAddr;

use nockapp::driver::{NockAppHandle, PokeResult};
use nockapp::noun::slab::NounSlab;
use nockvm::noun::{D, T};
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

use crate::error::{NockAppGrpcError, Result};
use crate::pb::common::v1::{ErrorCode, ErrorStatus};
use crate::pb::private::v1::nock_app_service_server::{
    NockAppService as PrivateNockApp, NockAppServiceServer as PrivateNockAppServer,
};
use crate::pb::private::v1::*;
use crate::wire_conversion::grpc_wire_to_nockapp;

pub struct PrivateNockAppGrpcServer {
    handle: NockAppHandle,
}

impl PrivateNockAppGrpcServer {
    pub fn new(handle: NockAppHandle) -> Self {
        Self { handle }
    }

    pub async fn serve(self, addr: SocketAddr) -> Result<()> {
        info!("Starting gRPC server on {}", addr);

        let service = PrivateNockAppServer::new(self);

        Server::builder()
            .add_service(service)
            .serve(addr)
            .await
            .map_err(NockAppGrpcError::Transport)?;

        Ok(())
    }

    /// Convert path strings to a NounSlab for peek operations
    fn path_to_noun_slab(&self, path: &[String]) -> Result<NounSlab> {
        if path.is_empty() {
            return Err(NockAppGrpcError::InvalidRequest(
                "Path cannot be empty".to_string(),
            ));
        }

        let mut slab = NounSlab::new();
        let mut path_nouns = Vec::new();

        for segment in path {
            // Convert path segment to a tag atom
            let atom = nockapp::utils::make_tas(&mut slab, segment);
            path_nouns.push(atom.as_noun());
        }

        // Add terminating D(0)
        path_nouns.push(D(0));

        let path_noun = T(&mut slab, &path_nouns);
        slab.set_root(path_noun);

        Ok(slab)
    }

    /// Build error response with proper error status
    fn build_error_response<T>(&self, error: NockAppGrpcError) -> T
    where
        T: From<ErrorStatus>,
    {
        let error_status = ErrorStatus {
            code: match &error {
                NockAppGrpcError::PeekFailed => ErrorCode::PeekFailed as i32,
                NockAppGrpcError::PokeFailed => ErrorCode::PokeFailed as i32,
                NockAppGrpcError::Timeout => ErrorCode::Timeout as i32,
                NockAppGrpcError::InvalidRequest(_) => ErrorCode::InvalidRequest as i32,
                _ => ErrorCode::InternalError as i32,
            },
            message: error.to_string(),
            details: None,
        };
        T::from(error_status)
    }
}

#[tonic::async_trait]
impl PrivateNockApp for PrivateNockAppGrpcServer {
    async fn peek(
        &self,
        request: Request<PeekRequest>,
    ) -> std::result::Result<Response<PeekResponse>, Status> {
        let req = request.into_inner();
        info!("CorePeek request: pid={}, path={:?}", req.pid, req.path);

        let path_slab = match self.path_to_noun_slab(&req.path) {
            Ok(slab) => slab,
            Err(e) => {
                warn!("Invalid path in Peek: {}", e);
                let response = PeekResponse {
                    result: Some(peek_response::Result::Error(self.build_error_response(e))),
                };
                return Ok(Response::new(response));
            }
        };

        match self.handle.peek(path_slab).await {
            Ok(Some(result_slab)) => {
                // Convert result to JAM-encoded bytes
                let jam_bytes = result_slab.jam();

                let response = PeekResponse {
                    result: Some(peek_response::Result::Data(jam_bytes.to_vec())),
                };
                Ok(Response::new(response))
            }
            Ok(None) => {
                debug!("Peek returned no result");
                let response = PeekResponse {
                    result: Some(peek_response::Result::Error(
                        self.build_error_response(NockAppGrpcError::PeekFailed),
                    )),
                };
                Ok(Response::new(response))
            }
            Err(e) => {
                error!("Peek operation failed: {}", e);
                let response = PeekResponse {
                    result: Some(peek_response::Result::Error(
                        self.build_error_response(NockAppGrpcError::NockApp(e)),
                    )),
                };
                Ok(Response::new(response))
            }
        }
    }

    async fn poke(
        &self,
        request: Request<PokeRequest>,
    ) -> std::result::Result<Response<PokeResponse>, Status> {
        let req = request.into_inner();
        debug!("Poke request: pid={}", req.pid);

        let wire = match req.wire {
            Some(wire) => match grpc_wire_to_nockapp(&wire) {
                Ok(w) => w,
                Err(e) => {
                    warn!("Invalid wire in Poke: {}", e);
                    let response = PokeResponse {
                        result: Some(poke_response::Result::Error(self.build_error_response(e))),
                    };
                    return Ok(Response::new(response));
                }
            },
            None => {
                warn!("Missing wire in Poke request");
                let response = PokeResponse {
                    result: Some(poke_response::Result::Error(self.build_error_response(
                        NockAppGrpcError::InvalidRequest("Wire is required".to_string()),
                    ))),
                };
                return Ok(Response::new(response));
            }
        };

        // Decode JAM payload
        let mut payload_slab = NounSlab::new();
        let _payload_noun = match payload_slab.cue_into(bytes::Bytes::from(req.payload)) {
            Ok(noun) => noun,
            Err(e) => {
                warn!("Failed to decode JAM payload: {:?}", e);
                let response = PokeResponse {
                    result: Some(poke_response::Result::Error(self.build_error_response(
                        NockAppGrpcError::Serialization(format!("JAM decoding failed: {:?}", e)),
                    ))),
                };
                return Ok(Response::new(response));
            }
        };

        match self.handle.poke(wire, payload_slab).await {
            Ok(PokeResult::Ack) => {
                debug!("Poke operation acknowledged");
                let response = PokeResponse {
                    result: Some(poke_response::Result::Acknowledged(true)),
                };
                Ok(Response::new(response))
            }
            Ok(PokeResult::Nack) => {
                debug!("Poke operation nacked");
                let response = PokeResponse {
                    result: Some(poke_response::Result::Error(
                        self.build_error_response(NockAppGrpcError::PokeFailed),
                    )),
                };
                Ok(Response::new(response))
            }
            Err(e) => {
                error!("Poke operation failed: {}", e);
                let response = PokeResponse {
                    result: Some(poke_response::Result::Error(
                        self.build_error_response(NockAppGrpcError::NockApp(e)),
                    )),
                };
                Ok(Response::new(response))
            }
        }
    }
}
