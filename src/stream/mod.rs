pub mod advertise_stream;
pub mod find_stream;

#[cfg(feature = "media")]
pub mod stream_client;
#[cfg(feature = "media")]
pub mod stream_server;

use dittolive_ditto::experimental::peer_pubkey::PeerPubkey;
use serde::{Deserialize, Serialize};
use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};
use std::time::Duration;
use str0m::{
    change::{SdpAnswer, SdpOffer},
    channel::ChannelData,
    media::MediaData,
};
use thiserror::Error;
use tracing::{debug, instrument};

const IPV6_HEADER: u128 = 0xd1770 << u128::BITS - 20;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum WebRtcUserApi {
    SdpOffer(SdpOffer),
    SdpAnswer(SdpAnswer),
    RtcData(Vec<u8>),
}

#[derive(Debug, PartialEq)]
pub enum WebRtcEvent {
    Continue(Duration),
    Media(MediaData),
    Channel(ChannelData),
    Disconnected,
}

#[derive(Error, Debug, PartialEq)]
pub enum StreamError {
    #[error("Ditto data stream closed unexpectedly")]
    DittoStreamClosed,
    #[error("Failed to fetch payload params for video.")]
    PayloadParams,
    #[error("WebRTC instance disconnected unexpectedly")]
    WebRtcDisconnect,
    #[error("Received unexpected WebRtcUserApi message: {0:?}")]
    ApiOutOfSequence(WebRtcUserApi),
}

/// Grab a socket address from a peer key.
/// This address is not intended to be used with a normal UDP or TCP socket,
/// it is only meant to be used in the WebRTC state machine.
#[instrument]
pub fn socket_from_peer_key(value: &PeerPubkey) -> SocketAddr {
    // Grab the last 80 bits
    let last_chunk = &value[value.len() - 10..];
    let mut bits: u128 = IPV6_HEADER;
    for (i, val) in last_chunk.iter().enumerate() {
        let shift = 72 - (i * 8);
        bits |= (*val as u128) << shift;
    }
    let ipv6_addr = Ipv6Addr::from_bits(bits);
    let sock = SocketAddr::V6(SocketAddrV6::new(ipv6_addr, 0xd177, 0, 0));
    debug!(ipv6_sock = %sock);
    sock
}

#[cfg(test)]
mod test {
    use crate::utils::init_ditto;
    use std::{net::IpAddr, str::FromStr};

    use super::*;
    #[test]
    fn socket_from_peer_key_works() {
        let _ = tracing_subscriber::fmt::try_init();
        let ditto = init_ditto().expect("Failed to init ditto");
        let pp_key =
            PeerPubkey::from_str(&ditto.presence().graph().local_peer.peer_key_string).unwrap();
        let socket_addr = socket_from_peer_key(&pp_key);
        if let IpAddr::V6(address) = socket_addr.ip() {
            let address_octets = address.octets();
            assert_eq!(address_octets[..3], [0xd1, 0x77, 0x00]);
            assert_eq!(&address_octets[6..], &pp_key[pp_key.len() - 10..]);
        } else {
            panic!("Ipv4 not supported")
        }
    }
}
