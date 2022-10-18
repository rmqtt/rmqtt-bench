use std::convert::TryFrom;
use ntex::util::Bytes;
use ntex_mqtt::v3::codec;
use ntex::util::ByteString;
use structopt::StructOpt;

#[derive(StructOpt, Debug, Clone)]
pub struct Options {
    ///MQTT broker endpoint list, "host1:port host2:port host3:port" (default "localhost:1883")
    #[structopt(long, default_value = "localhost:1883")]
    pub addrs: Vec<String>,

    /// The number of connections
    #[structopt(short, long, default_value = "1000")]
    pub conns: usize,

    /// Keepalive, Seconds
    #[structopt(short = "k", long, default_value = "60")]
    pub keepalive: u16,

    /// Whether to clear the session
    #[structopt(name = "clear-session", long)]
    pub clear_session: bool,

    /// Client id pattern, {no} - Connection Serial Number, {random} - The random number
    #[structopt(name = "id-pattern", long, default_value = "{no}")]
    pub client_id_pattern: String,

    ///Subscribe switch, default value: false
    #[structopt(name = "sub", long)]
    pub sub_switch: bool,

    ///Publish switch, default value: false
    #[structopt(name = "pub", long)]
    pub pub_switch: bool,

    /// Subscription or Publish Topic pattern, {cid} - Client id, {no} - Connection Serial Number
    #[structopt(name = "topic-pattern", long, default_value = "{cid}")]
    pub topic_pattern: String,

    /// QoS, Currently, only 0 and 1 are supported
    #[structopt(short = "q", long, default_value = "1")]
    pub qos: u8,

    /// Auto reconnect interval, millisecond
    #[structopt(name = "reconn-interval", long, default_value = "5000")]
    pub auto_reconn_interval: u64,

    /// Publish message interval, millisecond
    #[structopt(name = "pub-interval", long, default_value = "3000")]
    pub pub_interval: u64,

    /// Publish message length
    #[structopt(name = "payload-len", long, default_value = "10")]
    pub pub_payload_len: usize,

    ///Total number of published messages, 0 will not be limited
    #[structopt(name = "max-limit", short = "l", long, default_value = "0")]
    pub pub_max_limit: isize,

    /// Publish topic serial number range, format: -r 0 10000
    #[structopt(name = "topic-no-range", short = "r", long)]
    pub pub_topic_no_range: Option<Vec<u64>>,

    ///Retain message, default value: false
    #[structopt(long)]
    pub retain: bool,

    ///Handshake timeout, Seconds
    #[structopt(name = "handshake-timeout", long, default_value = "30")]
    pub handshake_timeout: u16,

    ///Username
    #[structopt(short = "u", long)]
    pub username: Option<String>,

    ///Password
    #[structopt(short = "p", long)]
    pub password: Option<String>,

    ///Last will
    #[structopt(flatten)]
    pub last_will: LastWill,

    ///Control enable, default value: false
    #[structopt(long)]
    pub control: bool,

    /// Control interval, millisecond
    #[structopt(name = "ctrl-interval", long, default_value = "1000")]
    pub ctrl_interval: u64,

    /// Disconnected and Reconnection ratio
    #[structopt(name = "ctrl-disconn-ratio", long, default_value = "0.4")]
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

impl Options {
    pub fn addr(&self) -> String {
        let idx = rand::prelude::random::<usize>() % self.addrs.len();
        self.addrs.get(idx).unwrap().to_owned()
    }
}
