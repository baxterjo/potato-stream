use std::{
    net::SocketAddr,
    str::FromStr,
    time::{Duration, Instant},
};

use anyhow::Result;
use dittolive_ditto::{
    experimental::{
        bus::{Acceptor, Inbound, Reliability, SendStatus, Stream},
        peer_pubkey::PeerPubkey,
    },
    Ditto,
};
use opencv::{core::Vector, imgcodecs, prelude::*};
use str0m::{
    format::Codec,
    media::{MediaTime, Mid},
    net::{Protocol, Receive},
    Candidate, Event, IceConnectionState, Input, Output, Rtc,
};
use tokio::sync::{
    mpsc::{self, unbounded_channel},
    watch,
};
use tokio::{sync::mpsc::UnboundedReceiver, task::JoinSet, time::timeout};
use tracing::{debug, error, instrument, trace, warn};

use super::{socket_from_peer_key, StreamError, WebRtcEvent, WebRtcUserApi};

struct ClientHandler {
    rtc: Rtc,
    ditto_stream: Stream<UnboundedReceiver<Inbound>>,
    local_addr: SocketAddr,
    remote_addr: SocketAddr,
    video_mid: Mid,
    video: watch::Receiver<(Mat, Instant)>,
}

impl ClientHandler {
    async fn new(
        mut stream: Stream<UnboundedReceiver<Inbound>>,
        local_addr: SocketAddr,
        video: watch::Receiver<(Mat, Instant)>,
        stream_name: String,
    ) -> Result<Self> {
        let remote_addr = socket_from_peer_key(&stream.peer_pubkey());

        // Set up RTC instance with initial offering.
        let mut rtc = Rtc::new();
        rtc.add_local_candidate(Candidate::host(local_addr, "udp")?);
        let mut change = rtc.sdp_api();
        // TODO: Simulcast would be implemented here.
        let video_mid = change.add_media(
            str0m::media::MediaKind::Video,
            str0m::media::Direction::SendOnly,
            Some(stream_name),
            None,
            None,
        );

        let (offer, pending) = change.apply().expect("Initial SDP offer");
        let send_handle = stream
            .message(serde_cbor::to_vec(&WebRtcUserApi::SdpOffer(offer))?)
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

        assert_eq!(status, SendStatus::Sent, "Sdp offer failed to send.");

        let message = stream.recv().await.expect("SDP answer message");
        if let WebRtcUserApi::SdpAnswer(answer) =
            serde_cbor::from_slice::<WebRtcUserApi>(&message.payload())?
        {
            rtc.sdp_api().accept_answer(pending, answer)?;
        } else {
            panic!("Received message out of order")
        }

        Ok(Self {
            rtc,
            ditto_stream: stream,
            local_addr,
            remote_addr,
            video_mid,
            video,
        })
    }

    async fn run(mut self) -> Result<()> {
        let mut input_buf = Vec::new();
        let mut vid_buf = Vector::new();

        loop {
            // Poll for WebRTC output until all outputs have been retrieved
            let duration = match self.run_rtc().await {
                WebRtcEvent::Continue(x) => x,
                WebRtcEvent::Disconnected => {
                    return Err(StreamError::WebRtcDisconnect)?;
                }
                event => {
                    warn!(?event, "Unhandled WebRtcEvent");
                    continue;
                }
            };

            let sel_timeout = tokio::time::sleep(duration);
            tokio::select! {
                // Use biased because incoming info will be less common than video frames.
                biased;
                _ = sel_timeout =>{
                    self.rtc.handle_input(Input::Timeout(Instant::now()))?;
                    continue;
                }
                incoming_opt = self.ditto_stream.recv() =>{
                    let input = self.handle_input_opt(incoming_opt, &mut input_buf)?;
                    self.rtc.handle_input(input)?;
                    continue;
                }
                _ = self.video.changed()=>{
                    self.send_video(&mut vid_buf)?;
                    continue;
                }

            }
        }
    }

    async fn run_rtc(&mut self) -> WebRtcEvent {
        loop {
            // Poll output until we get a timeout. The timeout means we
            // are either awaiting UDP socket input or the timeout to happen.
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
                            warn!(?data, "Streamer received media data, this shouldn't happen");
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

    fn handle_input_opt<'a>(
        &self,
        input_opt: Option<Inbound>,
        buf: &'a mut Vec<u8>,
    ) -> Result<Input<'a>> {
        match input_opt {
            Some(incoming) => {
                trace!(
                    len = incoming.len(),
                    src = %self.remote_addr,
                    "Recevied byes on ditto stream"
                );
                if let Ok(WebRtcUserApi::RtcData(payload)) = serde_cbor::from_slice(&incoming) {
                    buf.clear();
                    buf.extend_from_slice(&payload);
                    Ok(Input::Receive(
                        Instant::now(),
                        Receive {
                            proto: Protocol::Udp,
                            source: self.remote_addr,
                            destination: self.local_addr,
                            contents: buf
                                .as_slice()
                                .try_into()
                                .expect("Failed to convert bytes to bytes?"),
                        },
                    ))
                } else {
                    warn!(payload=?incoming.payload(),"Received junk data");
                    Ok(Input::Timeout(Instant::now()))
                }
            }
            None => Err(StreamError::DittoStreamClosed)?,
        }
    }
    fn send_video(&mut self, vid_buf: &mut Vector<u8>) -> Result<()> {
        let vid: watch::Ref<'_, (Mat, Instant)> = self.video.borrow_and_update();
        vid_buf.clear();
        imgcodecs::imencode(".jpg", &vid.0, vid_buf, &Vector::new())
            .expect("Failed to encode image");

        let params = &self
            .rtc
            .codec_config()
            .find(|p| {
                debug!("payload: {:?}", p);
                p.spec().codec == Codec::H264
                    && p.spec().format.profile_level_id.unwrap_or(0) == 4382751
            })
            .cloned()
            .ok_or(StreamError::PayloadParams)?;

        let pts = Instant::now() - vid.1;
        if let Some(writer) = self.rtc.writer(self.video_mid) {
            let freq = params.spec().clock_rate;
            let media_time: MediaTime = pts.into();
            writer.write(
                params.pt(),
                Instant::now(),
                media_time.rebase(freq),
                vid_buf.as_slice().to_vec(),
            )?;
        }

        Ok(())
    }
}

pub struct Server {
    stream_name: String,
    ditto_ip: SocketAddr,
    acceptor: Acceptor<UnboundedReceiver<Stream<UnboundedReceiver<Inbound>>>>,
    video: watch::Receiver<(Mat, Instant)>,
    clients: JoinSet<Result<()>>,
}

impl Server {
    #[instrument(skip(video, ditto))]
    pub fn new(
        ditto: &Ditto,
        stream_name: &str,
        video: watch::Receiver<(Mat, Instant)>,
    ) -> Result<Self> {
        let pp_key = PeerPubkey::from_str(&ditto.presence().graph().local_peer.peer_key_string)?;

        let bus = ditto.bus();
        let acceptor = bus
            .bind_topic(stream_name)
            .on_receive_factory(unbounded_channel)
            .reliability(Reliability::Unreliable)
            .finish(mpsc::unbounded_channel())?;
        Ok(Self {
            stream_name: stream_name.to_string(),
            acceptor,
            video,
            clients: JoinSet::new(),
            ditto_ip: socket_from_peer_key(&pp_key),
        })
    }

    pub async fn run(mut self) -> Result<()> {
        loop {
            // If clients is empty, wait for new client.
            while self.clients.is_empty() {
                match self.acceptor.recv().await {
                    Some(stream) => {
                        let client = ClientHandler::new(
                            stream,
                            self.ditto_ip.clone(),
                            self.video.clone(),
                            self.stream_name.clone(),
                        )
                        .await?;
                        self.clients.spawn(client.run());
                    }
                    None => {
                        return Err(StreamError::DittoStreamClosed)?;
                    }
                }
            }

            // If clients is not empty clean old clients and wait for new clients
            tokio::select! {
                result = self.clients.join_next()=>{
                    match result {
                        Some(Ok(Ok(()))) => {},
                        Some(Ok(Err(err))) => error!(%err, "Client errored"),
                        Some(Err(err)) if err.is_panic() => {
                            error!(%err, "Client panicked");
                        },
                        Some(Err(_err)) => {},
                        None=>{}
                    };
                }
                stream_opt = self.acceptor.recv()=>{
                    match stream_opt {
                        Some(stream)=>{
                            let client = ClientHandler::new(
                                stream,
                                self.ditto_ip.clone(),
                                self.video.clone(),
                                self.stream_name.clone(),
                            )
                            .await?;
                            self.clients.spawn(client.run());
                        }
                        None=>{return Err(StreamError::DittoStreamClosed)?;}
                    }
                }
            }
            if self.clients.is_empty() {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}
