use std::{
    str::FromStr,
    time::{Duration, Instant},
};

use std::net::SocketAddr;

use anyhow::Result;
use dittolive_ditto::{
    experimental::{
        bus::{Inbound, Reliability, SendStatus, Stream},
        peer_pubkey::PeerPubkey,
    },
    Ditto,
};
use opencv::imgcodecs::imdecode;
use opencv::{imgcodecs::ImreadModes, prelude::*};
use str0m::{
    net::{Protocol, Receive},
    Candidate, Event, IceConnectionState, Input, Output, Rtc,
};
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver},
        watch,
    },
    time::timeout,
};
use tracing::{error, trace, warn};

use super::{socket_from_peer_key, StreamError, WebRtcEvent, WebRtcUserApi};

pub struct StreamClient {
    rtc: Rtc,
    ditto_stream: Stream<UnboundedReceiver<Inbound>>,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    video: watch::Sender<(Mat, Instant)>,
}

impl StreamClient {
    pub async fn new(
        ditto: &Ditto,
        stream_key: PeerPubkey,
        stream_name: &str,
        video: watch::Sender<(Mat, Instant)>,
    ) -> Result<Self> {
        let remote_addr = socket_from_peer_key(&stream_key);
        let local_addr = socket_from_peer_key(&PeerPubkey::from_str(
            &ditto.presence().graph().local_peer.peer_key_string,
        )?);
        let mut stream = ditto
            .bus()
            .connect(stream_key, stream_name)
            .reliability(Reliability::Unreliable)
            .on_receive_factory(unbounded_channel)
            .finish_async()
            .await?;
        let mut rtc = Rtc::new();
        rtc.add_local_candidate(Candidate::host(local_addr, "udp")?);

        let incoming = stream.recv().await.ok_or(StreamError::DittoStreamClosed)?;

        let offer = match serde_cbor::from_slice::<WebRtcUserApi>(&incoming)? {
            WebRtcUserApi::SdpOffer(x) => x,
            other => {
                return Err(StreamError::ApiOutOfSequence(other))?;
            }
        };
        let answer = rtc.sdp_api().accept_offer(offer)?;
        let send_handle = stream
            .message(serde_cbor::to_vec(&WebRtcUserApi::SdpAnswer(answer))?)
            .send();

        let status = timeout(Duration::from_secs(5), async move {
            loop {
                let status = send_handle.changed().await;
                if status != SendStatus::Pending {
                    break status;
                }
            }
        })
        .await?;
        assert_eq!(status, SendStatus::Sent, "Failed to send SDP answer");

        Ok(Self {
            rtc,
            ditto_stream: stream,
            local_addr,
            remote_addr,
            video,
        })
    }

    pub async fn run(mut self) -> Result<()> {
        let start = Instant::now();
        let mut input_buf = Vec::new();
        loop {
            let duration = match self.run_rtc().await {
                WebRtcEvent::Continue(x) => x,
                WebRtcEvent::Disconnected => {
                    return Err(StreamError::WebRtcDisconnect)?;
                }
                WebRtcEvent::Media(media_data) => {
                    match imdecode(
                        &media_data.data.as_slice(),
                        ImreadModes::IMREAD_UNCHANGED as i32,
                    ) {
                        Ok(mat) => {
                            self.video.send_replace((mat, start));
                        }
                        Err(err) => {
                            error!(%err, "Error decoding frame");
                        }
                    }
                    continue;
                }
                WebRtcEvent::Channel(channel) => {
                    warn!(?channel, "Received unexpected channel data");
                    continue;
                }
            };

            let sel_timeout = tokio::time::sleep(duration);
            tokio::select! {
                biased;
                _ = sel_timeout => {
                    self.rtc.handle_input(Input::Timeout(Instant::now()))?;
                }
                inbound_opt = self.ditto_stream.recv() =>{
                    let inbound = inbound_opt.ok_or(StreamError::DittoStreamClosed)?;
                    let rtc_input = self.handle_inbound(inbound, &mut input_buf);
                    self.rtc.handle_input(rtc_input)?;
                }
            }
        }
    }

    async fn run_rtc(&mut self) -> WebRtcEvent {
        loop {
            // Poll output until we get a timeout. The timeout means we
            // are either awaiting stream input or the timeout to happen.
            let poll = { self.rtc.poll_output().unwrap() };
            let timeout_inst = match poll {
                // Stop polling when we get the timeout.
                Output::Timeout(v) => v,

                // Transmit this data to the remote peer. Typically via
                // a UDP socket. The destination IP comes from the ICE
                // agent. It might change during the session.
                Output::Transmit(v) => {
                    let send_handle = self
                        .ditto_stream
                        .message(safer_ffi::bytes::Bytes::copied_from_slice(&v.contents))
                        .send();
                    let status = async move {
                        loop {
                            let status = send_handle.changed().await;
                            if status != SendStatus::Pending {
                                break status;
                            }
                        }
                    }
                    .await;

                    if status != SendStatus::Sent {
                        warn!(transmit=?v,"Failed to send transmit")
                    }
                    continue;
                }

                // Events are mainly incoming media data from the remote
                // peer, but also data channel data and statistics.
                Output::Event(v) => {
                    // Abort if we disconnect.
                    match v {
                        Event::IceConnectionStateChange(IceConnectionState::Disconnected) => {
                            return WebRtcEvent::Disconnected;
                        }
                        Event::MediaData(data) => {
                            return WebRtcEvent::Media(data);
                        }
                        _ => {
                            tracing::debug!("unhandled event");
                            tracing::trace!(?v)
                        }
                    }

                    // TODO: handle more cases of v here, such as incoming media data.

                    continue;
                }
            };

            let duration = timeout_inst - Instant::now();
            return WebRtcEvent::Continue(duration);
        }
    }

    fn handle_inbound<'a>(&self, inbound: Inbound, buf: &'a mut Vec<u8>) -> Input<'a> {
        trace!(
            len = inbound.len(),
            src = %self.remote_addr,
            "Recevied bytes on ditto stream"
        );
        if let Ok(WebRtcUserApi::RtcData(payload)) = serde_cbor::from_slice(&inbound) {
            buf.clear();
            buf.extend_from_slice(&payload);
            Input::Receive(
                Instant::now(),
                Receive {
                    proto: Protocol::Udp,
                    source: self.remote_addr,
                    destination: self.local_addr,
                    contents: buf
                        .as_slice()
                        .try_into()
                        .expect("failed to convert bytes to bytes?"),
                },
            )
        } else {
            Input::Timeout(Instant::now())
        }
    }
}
