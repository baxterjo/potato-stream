use bytes::Bytes;
use std::collections::{HashMap, VecDeque};
use std::time::Instant;
use str0m::channel::ChannelId;
use str0m::net::{Protocol, Receive};
use tokio::sync::mpsc::error::TryRecvError;
use tracing::{debug, info, instrument, span, trace, Instrument, Level};

use str0m::media::{Direction, MediaKind};
use str0m::{Candidate, Event, IceConnectionState, Input, Output, Rtc};
use tokio::sync::mpsc;

#[tokio::test]
async fn test_str0m_sans_io() {
    let _ = tracing_subscriber::fmt::try_init();
    // Instantiate a new Rtc instance.
    let mut act_rtc = Rtc::new();
    let mut pass_rtc = Rtc::new();

    let (pass_tx, pass_rx) = mpsc::channel(100);
    let (act_tx, act_rx) = mpsc::channel(100);

    // Add some ICE candidate such as a locally bound UDP port.
    let act_addr = "1.2.3.4:5000".parse().unwrap();
    let candidate = Candidate::host(act_addr, "udp").unwrap();
    act_rtc.add_local_candidate(candidate);

    // Create a `SdpApi`. The change lets us make multiple changes
    // before sending the offer.
    let mut change = act_rtc.sdp_api();

    // Do some change. A valid OFFER needs at least one "m-line" (media).
    let _mid = change.add_media(MediaKind::Audio, Direction::SendRecv, None, None, None);
    let _cid = change.add_channel("super_secret".to_string());

    // Get the offer.
    let (offer, pending) = change.apply().unwrap();

    // Forward the offer to the remote peer and await the answer.
    // How to transfer this is outside the scope for this library.
    let pass_addr = "1.2.3.4:5000".parse().unwrap();
    let candidate = Candidate::host(pass_addr, "udp").unwrap();
    pass_rtc.add_local_candidate(candidate);
    let answer = pass_rtc.sdp_api().accept_offer(offer).unwrap();

    // Apply answer.
    act_rtc.sdp_api().accept_answer(pending, answer).unwrap();

    info!("RTC instances up");

    // Go to _run loop_
    let _act_task = tokio::task::spawn_blocking(move || run_loop(act_rtc, true, pass_tx, act_rx))
        .instrument(span!(Level::INFO, "streamer"));
    let _pass_task =
        tokio::task::spawn_blocking(move || run_loop(pass_rtc, false, act_tx, pass_rx))
            .instrument(span!(Level::INFO, "receiver"));
}

#[instrument(skip_all, fields(streamer=is_streamer))]
fn run_loop(
    mut rtc: Rtc,
    is_streamer: bool,
    outbound: mpsc::Sender<Bytes>,
    mut inbound: mpsc::Receiver<Bytes>,
) {
    info!("Starting run loop");
    // Buffer for reading incoming UDP packets.
    let mut buf: Vec<u8>;

    let mut channels_incoming: HashMap<ChannelId, (String, VecDeque<Bytes>)> = HashMap::new();

    let mut last_sent = Instant::now();
    'super_loop: loop {
        if is_streamer && (Instant::now() - last_sent).as_secs() >= 5 {
            for (cid, (name, _queue)) in channels_incoming.iter() {
                debug!(?cid, name, "sending hello");
                rtc.channel(*cid)
                    .expect("channel error on send")
                    .write(false, b"hello")
                    .expect("send error");
            }
            last_sent = Instant::now();
        }
        // Poll output until we get a timeout. The timeout means we
        // are either awaiting UDP socket input or the timeout to happen.
        let poll = { rtc.poll_output().unwrap() };
        let timeout = match poll {
            // Stop polling when we get the timeout.
            Output::Timeout(v) => v,

            // Transmit this data to the remote peer. Typically via
            // a UDP socket. The destination IP comes from the ICE
            // agent. It might change during the session.
            Output::Transmit(v) => {
                outbound
                    .try_send(Bytes::copy_from_slice(&v.contents))
                    .expect("Full buffer");
                continue;
            }

            // Events are mainly incoming media data from the remote
            // peer, but also data channel data and statistics.
            Output::Event(v) => {
                // Abort if we disconnect.
                match v {
                    Event::IceConnectionStateChange(IceConnectionState::Disconnected) => {
                        return;
                    }
                    Event::MediaData(data) => {
                        tracing::info!(data=?data.data, "media_data")
                    }
                    Event::ChannelOpen(cid, name) => {
                        info!(name, "Got channel open");
                        {
                            channels_incoming.insert(cid, (name, VecDeque::new()));
                        }
                    }
                    Event::ChannelData(data) => {
                        info!(?data, "Received channel data");
                        if let Some((name, channel)) = channels_incoming.get_mut(&data.id) {
                            info!(name, len = data.data.len(), "Incoming channel data");
                            channel.push_back(data.data.into());
                        }
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

        // Duration until timeout.
        let duration = timeout - Instant::now();

        // socket.set_read_timeout(Some(0)) is not ok
        if duration.is_zero() {
            // Drive time forwards in rtc straight away.
            rtc.handle_input(Input::Timeout(Instant::now())).unwrap();
            continue;
        }

        // Try to receive. Because we have a timeout on the socket,
        // we will either receive a packet, or timeout.
        // This is where having an async loop shines. We can await multiple things to
        // happen such as outgoing media data, the timeout and incoming network traffic.
        // When using async there is no need to set timeout on the socket.

        let start_poll = Instant::now();

        let input = loop {
            match inbound.try_recv() {
                Ok(data) => {
                    // UDP data received.
                    trace!(len = data.len(), "Received 'I/O' data");
                    buf = data.into();
                    break Input::Receive(
                        Instant::now(),
                        Receive {
                            proto: Protocol::Udp,
                            source: "1.2.3.4:5000".parse().unwrap(),
                            destination: "1.2.3.4:5000".parse().unwrap(),
                            contents: buf.as_slice().try_into().unwrap(),
                        },
                    );
                }

                Err(e) => match e {
                    TryRecvError::Empty => {
                        if Instant::now() - start_poll >= duration {
                            break Input::Timeout(Instant::now());
                        }
                    }
                    TryRecvError::Disconnected => {
                        // abort
                        return;
                    }
                },
            }
        };

        // Input is either a Timeout or Receive of data. Both drive the state forward.
        rtc.handle_input(input).unwrap();

        for (_cid, (name, queue)) in channels_incoming.iter_mut() {
            if let Some(message) = queue.pop_front() {
                let message_str = String::from_utf8_lossy(&message[..]);
                info!(ch_label=name, message=%message_str, "Received message");
                break 'super_loop;
            }
        }
    }
}
