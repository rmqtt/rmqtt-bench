use std::convert::TryFrom;

use ntex::util::Bytes;
use ntex::util::ByteString;
use ntex_mqtt::v3::codec;
use structopt::StructOpt;

#[derive(StructOpt, Debug, Clone)]
pub struct Options {
    #[structopt(subcommand)]
    pub command: Command,
}

#[derive(Debug, StructOpt, Clone)]
pub enum Command {
    #[structopt(name = "v3")]
    V3(V3),
    #[structopt(name = "v5")]
    V5(V5),
}

#[derive(StructOpt, Debug, Clone)]
pub struct V3 {
    ///MQTT broker endpoint list, "host1:port host2:port host3:port"
    #[structopt(long, default_value = "localhost:1883")]
    pub addrs: Vec<String>,

    ///Bind local IP address, "127.0.0.1 127.0.0.2 127.0.0.3"
    #[structopt(long)]
    pub ifaddrs: Vec<String>,

    /// The number of connections
    #[structopt(short, long, default_value = "1000")]
    pub conns: usize,

    /// Interval of connecting to the broker, millisecond
    #[structopt(name = "interval", short = "i", long, default_value = "0")]
    pub conn_interval: u64,

    /// Client id pattern, {no} - Connection Serial Number, {random} - The random number
    #[structopt(name = "id-pattern", short = "E", long, default_value = "{no}")]
    pub client_id_pattern: String,

    ///Username
    #[structopt(short = "u", long)]
    pub username: Option<String>,

    ///Password
    #[structopt(short = "p", long)]
    pub password: Option<String>,

    ///Handshake timeout, Seconds
    #[structopt(name = "handshake-timeout", short = "h", long, default_value = "30")]
    pub handshake_timeout: u16,

    /// Keepalive, Seconds
    #[structopt(short = "k", long, default_value = "60")]
    pub keepalive: u16,

    /// Clean session, default value: false
    #[structopt(name = "clean", short = "C", long)]
    pub clear_session: bool,

    ///Subscribe switch, default value: false
    #[structopt(name = "sub", short = "S", long)]
    pub sub_switch: bool,

    ///Publish switch, default value: false
    #[structopt(name = "pub", short = "P", long)]
    pub pub_switch: bool,

    /// Subscription or Publish Topic pattern, {cid} - Client id, {no} - Connection Serial Number
    #[structopt(name = "topic", short = "t", long, default_value = "{cid}")]
    pub topic_pattern: String,

    /// QoS, Currently, only 0 and 1 are supported
    #[structopt(short = "q", long, default_value = "1")]
    pub qos: u8,

    /// Auto reconnect interval, millisecond
    #[structopt(name = "reconn-interval", short = "a", long, default_value = "5000")]
    pub auto_reconn_interval: u64,

    /// Publish message interval, millisecond
    #[structopt(name = "pub-interval", short = "I", long, default_value = "1000")]
    pub pub_interval: u64,

    /// Publish message length
    #[structopt(name = "size", short = "s", long, default_value = "256")]
    pub pub_payload_len: usize,

    /// Set the message content for publish
    #[structopt(name = "message", short = "m", long)]
    pub pub_payload: Option<String>,

    ///Total number of published messages, 0 will not be limited
    #[structopt(name = "max-limit", short = "l", long, default_value = "0")]
    pub pub_max_limit: isize,

    /// Publish topic serial number range, format: -R 0 10000
    #[structopt(name = "topic-no-range", short = "R", long)]
    pub pub_topic_no_range: Option<Vec<u64>>,

    ///Retain message, default value: false
    #[structopt(short = "r", long)]
    pub retain: bool,

    ///Last will
    #[structopt(flatten)]
    pub last_will: LastWill,

    ///Console output interval, Seconds
    #[structopt(name = "output-interval", short = "o", long, default_value = "5")]
    pub output_interval: u64,

    ///Control enable, default value: false
    #[structopt(short = "T", long)]
    pub control: bool,

    /// Control interval, millisecond
    #[structopt(name = "ctrl-interval", short = "L", long, default_value = "1000")]
    pub ctrl_interval: u64,

    /// Disconnected and Reconnection ratio
    #[structopt(name = "ctrl-disconn-ratio", short = "D", long, default_value = "0.4")]
    pub ctrl_disconn_ratio: f32,
}

#[derive(StructOpt, Clone, Debug)]
pub struct LastWill {
    #[structopt(long = "lw-qos")]
    pub lw_qos: Option<u8>,
    #[structopt(long = "lw-retain")]
    pub lw_retain: Option<bool>,
    #[structopt(long = "lw-topic")]
    pub lw_topic: Option<String>,
    #[structopt(long = "lw-msg")]
    pub lw_message: Option<String>,
}

impl LastWill {
    pub fn to_last_will(&self) -> Option<codec::LastWill> {
        if let (Some(topic), Some(message), Some(qos), retain) = (
            self.lw_topic.as_ref(),
            self.lw_message.as_ref(),
            self.lw_qos,
            self.lw_retain,
        ) {
            Some(codec::LastWill {
                topic: ByteString::from(topic.clone()),
                message: Bytes::from(message.clone()),
                qos: codec::QoS::try_from(qos).unwrap(),
                retain: if let Some(retain) = retain {
                    retain
                } else {
                    false
                },
            })
        } else {
            None
        }
    }
}

impl V3 {
    pub fn addr(&self) -> String {
        let idx = rand::prelude::random::<usize>() % self.addrs.len();
        self.addrs.get(idx).unwrap().to_owned()
    }
}

#[derive(StructOpt, Debug, Clone)]
pub struct V5 {}
