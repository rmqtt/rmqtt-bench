use futures::channel::mpsc;
use futures::SinkExt;
use futures::StreamExt;
use ntex::time;
use ntex::time::{sleep, Seconds};
use ntex::util::ByteString;
use ntex::util::Bytes;
use ntex::util::Ready;
use ntex_mqtt::error::SendPacketError;
use ntex_mqtt::v3::codec::SubscribeReturnCode;
use ntex_mqtt::{self, v3};
use parking_lot::{Mutex, RwLock};
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::spawn_local;
use uuid::Uuid;

use super::connector::ConnectorFactory;
use super::Stats;
use super::V3Options;
use super::{ControlManager, PacketId};

pub enum Message {
    Connect,
    Disconnect,
    Close,
}

#[derive(Clone)]
pub struct Client {
    pub client_id: String,
    pub seq_no: String,
    pub sink: Arc<RwLock<Option<v3::MqttSink>>>,
    pub mgr: ControlManager,
    ifaddr: Arc<RwLock<Option<String>>>,
    subs: Arc<AtomicUsize>,
    msg_tx: Arc<RwLock<Option<mpsc::Sender<Message>>>>,
    disconnected: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
    packet_id_gen: Arc<Mutex<u16>>,
}

impl Client {
    pub fn new(seq_no: usize, mgr: ControlManager) -> Self {
        let seq_no = format!("{}", seq_no);

        //{no}-{random}
        let client_id = mgr
            .opts
            .client_id_pattern
            .clone()
            .replace("{random}", Uuid::new_v4().to_string().as_str())
            .replace("{no}", seq_no.as_str());

        Self {
            client_id,
            seq_no,
            sink: Arc::new(RwLock::new(None)),
            mgr,
            ifaddr: Arc::new(RwLock::new(None)),
            subs: Arc::new(AtomicUsize::new(0)),
            msg_tx: Arc::new(RwLock::new(None)),
            disconnected: Arc::new(AtomicBool::new(false)),
            closed: Arc::new(AtomicBool::new(false)),
            packet_id_gen: Arc::new(Mutex::new(1)),
        }
    }

    pub fn gen_packet_id(&self) -> PacketId {
        let mut id = self.packet_id_gen.lock();
        let packet_id = *id;
        if packet_id < u16::MAX {
            *id += 1;
            packet_id
        } else {
            *id = 2;
            1
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    pub fn close(&self) -> bool {
        let mut ret = false;
        if let Some(sink) = self.sink() {
            if sink.is_open() {
                Stats::instance().closeds.inc();
                sink.close();
                self.closed.store(true, Ordering::SeqCst);
                ret = true;
                if let Some(mut msg_tx) = self.msg_tx() {
                    let _ = msg_tx.try_send(Message::Close);
                }
            }
        }
        ret
    }

    pub fn is_disconnected(&self) -> bool {
        self.disconnected.load(Ordering::SeqCst)
    }

    pub fn disconnect(&self) -> bool {
        let mut ret = false;
        if let Some(sink) = self.sink() {
            if sink.is_open() {
                sink.close();
                self.disconnected.store(true, Ordering::SeqCst);
                ret = true;
                if let Some(mut msg_tx) = self.msg_tx() {
                    let _ = msg_tx.try_send(Message::Disconnect);
                }
            }
        }
        ret
    }

    pub fn connect(&self) {
        self.disconnected.store(false, Ordering::SeqCst);
        if let Some(mut msg_tx) = self.msg_tx() {
            let _ = msg_tx.try_send(Message::Connect);
        }
    }

    fn set_sink(&self, sink: v3::MqttSink) {
        self.sink.write().replace(sink);
    }

    pub fn sink(&self) -> Option<v3::MqttSink> {
        self.sink.read().as_ref().cloned()
    }

    fn msg_tx(&self) -> Option<mpsc::Sender<Message>> {
        self.msg_tx.read().as_ref().cloned()
    }

    fn subs_inc(&self) {
        Stats::instance().subs.inc();
        self.subs.fetch_add(1, Ordering::SeqCst);
    }

    fn subs_clear(&self) {
        Stats::instance().subs.decs(self.subs_count() as isize);
        self.subs.store(0, Ordering::SeqCst);
    }

    pub fn subs_count(&self) -> usize {
        self.subs.load(Ordering::SeqCst)
    }

    pub fn ifaddr(&self) -> Option<String> {
        self.ifaddr.read().as_ref().cloned()
    }

    pub async fn start(&self) -> mpsc::Receiver<()> {
        let client = self.clone();

        let client_id = client.client_id.clone();
        let opts = client.mgr.opts.clone();
        let no = client.seq_no.clone();
        let mgr = client.mgr.clone();

        let (mut exit_tx, exit_rx) = futures::channel::mpsc::channel(10);
        let (mut msg_tx, mut msg_rx) = mpsc::channel::<Message>(100);

        self.msg_tx.write().replace(msg_tx.clone());
        let _ = msg_tx.send(Message::Connect).await;

        let addr = opts.addr();

        let mut baddr: Option<SocketAddr> = None;
        if !opts.ifaddrs.is_empty() {
            let idx = rand::prelude::random::<usize>() % opts.ifaddrs.len();
            client.ifaddr.write().replace(opts.ifaddrs[idx].clone());
            baddr = Some(format!("{}:0", opts.ifaddrs[idx]).parse().unwrap());
        }

        let mut builder = v3::client::MqttConnector::new(addr.clone())
            .connector(ConnectorFactory::new(baddr))
            .client_id(client_id.clone())
            .keep_alive(Seconds(opts.keepalive))
            .handshake_timeout(Seconds(opts.handshake_timeout));

        if let (Some(username), Some(password)) = (opts.username.as_ref(), opts.password.as_ref()) {
            builder = builder
                .username(username.clone())
                .password(Bytes::from(password.clone()));
        }

        if opts.clear_session {
            builder = builder.clean_session()
        };

        if let Some(last_will) = opts.last_will.to_last_will() {
            builder = builder.last_will(last_will);
        }

        let qos = v3::codec::QoS::try_from(opts.qos).unwrap();
        let runner = async move {
            let sleep_interval: Duration = Duration::from_millis(opts.auto_reconn_interval);

            use tokio::time::timeout;

            while !client.is_closed() {
                match timeout(sleep_interval, msg_rx.next()).await {
                    Err(_) => {
                        log::debug!("timeout ...");
                        if client.is_disconnected() {
                            continue;
                        }
                    }
                    Ok(None) => break,
                    Ok(Some(Message::Connect)) => {
                        log::debug!("Message::Connect ...");
                    }
                    Ok(Some(Message::Disconnect)) => {
                        log::debug!("Message::Disconnect ...");
                        continue;
                    }
                    Ok(Some(Message::Close)) => {
                        log::debug!("Message::Close ...");
                        break;
                    }
                }

                client.subs_clear();

                let connect_enable = mgr.on_connect.fire(client.clone());

                if connect_enable {
                    match builder.connect().await {
                        Ok(c) => {
                            mgr.on_connected.fire(client.clone());

                            let sink = c.sink();

                            client.set_sink(sink.clone());

                            Stats::instance().conns.inc();

                            //subscribe
                            if opts.sub_switch {
                                let topic = ByteString::from(
                                    opts.topic_pattern
                                        .clone()
                                        .replace("{cid}", client_id.as_str())
                                        .replace("{no}", no.as_str()),
                                );

                                let subscribe_enable =
                                    mgr.on_subscribe.fire((client.clone(), topic.clone(), qos));

                                if subscribe_enable {
                                    spawn_local(Self::subscribe(
                                        client.clone(),
                                        sink.clone(),
                                        topic.clone(),
                                        qos,
                                        client_id.clone(),
                                    ));
                                }
                            }

                            //publish
                            if opts.pub_switch && opts.pub_payload_len > 0 {
                                let opts = opts.clone();
                                let client_id = client_id.clone();
                                let mut exit_tx = exit_tx.clone();
                                let client = client.clone();
                                spawn_local(async move {
                                    Self::publishs(client.clone(), sink.clone(), opts, client_id)
                                        .await;
                                    client.close();
                                    let _ = exit_tx.send(()).await;
                                });
                            }

                            //client event loop
                            Self::ev_loop(c, client.clone()).await;
                        }
                        Err(e) => {
                            log::debug!("{:?} connect to {:?} fail, {:?}", client_id, addr, e);
                            Stats::instance().set_last_err(format!("{:?}", e));
                            Stats::instance().conn_fails.inc();
                        }
                    }
                }
            }
            let _ = exit_tx.send(()).await;
        };
        spawn_local(runner);

        exit_rx
    }

    async fn subscribe(
        c: Client,
        sink: v3::MqttSink,
        sub_topic: ByteString,
        qos: v3::QoS,
        client_id: String,
    ) {
        'subscribe: loop {
            match sink
                .subscribe()
                .topic_filter(sub_topic.clone(), qos)
                .send()
                .await
            {
                Ok(rets) => {
                    for ret in rets {
                        if let SubscribeReturnCode::Failure = ret {
                            log::debug!("{:?} subscribe failure", client_id);
                            Stats::instance().set_last_err("subscribe failure".into());
                            time::sleep(Duration::from_secs(5)).await;
                            continue 'subscribe;
                        } else {
                            c.subs_inc();
                        }
                    }
                    c.mgr
                        .on_subscribed
                        .fire((c.clone(), sub_topic.clone(), qos));
                    break;
                }
                Err(SendPacketError::Disconnected) => {
                    log::debug!("{:?} subscribe error, Disconnected", client_id);
                    break;
                }
                Err(e) => {
                    log::debug!("{:?} subscribe error, {:?}", client_id, e);
                    Stats::instance().set_last_err(format!("{:?}", e));
                    break;
                }
            }
        }
    }

    async fn publishs(c: Client, sink: v3::MqttSink, opts: Arc<V3Options>, client_id: String) {
        let (no_start, no_end) = if let Some(range) = &opts.pub_topic_no_range {
            match range.len() {
                0 => (0, opts.conns as u64),
                1 => (0, range[0]),
                _ => (range[0], range[1]),
            }
        } else {
            (0, opts.conns as u64)
        };

        let pub_topic_len = no_end - no_start;

        let qos = v3::codec::QoS::try_from(opts.qos).unwrap();

        loop {
            sleep(Duration::from_millis(opts.pub_interval)).await;

            let no = (rand::prelude::random::<u64>() % pub_topic_len) + no_start;
            let pub_topic = ByteString::from(
                opts.topic_pattern
                    .clone()
                    .replace("{cid}", &client_id)
                    .replace("{no}", &format!("{}", no)),
            );

            let payload = if let Some(payload) = &opts.pub_payload {
                ntex::util::Bytes::from(payload.clone())
            } else {
                ntex::util::Bytes::from("0".repeat(opts.pub_payload_len))
            };

            if sink.is_open() {
                let packet_id = c.gen_packet_id();

                let publish_enable = c.mgr.on_publish.fire((
                    c.clone(),
                    packet_id,
                    pub_topic.clone(),
                    qos,
                    opts.retain,
                    payload.clone(),
                ));

                if publish_enable {
                    let mut pub_builder = sink.publish(pub_topic, payload);
                    if opts.retain {
                        pub_builder = pub_builder.retain();
                    }
                    //pub_builder.packet_id()
                    if opts.pub_max_limit > 0 {
                        if !Stats::instance().sends.inc_limit(opts.pub_max_limit) {
                            break;
                        }
                    } else {
                        Stats::instance().sends.inc();
                    }
                    if opts.qos == 0 {
                        if let Err(e) = pub_builder.send_at_most_once() {
                            log::debug!("{:?} publish error, {:?}", client_id, e);
                            Stats::instance().set_last_err(format!("{:?}", e));
                        }
                    } else {
                        let pub_builder = pub_builder.packet_id(packet_id);
                        if let Err(e) = pub_builder.send_at_least_once().await {
                            log::debug!("{:?} publish error, {:?}", client_id, e);
                            Stats::instance().set_last_err(format!("{:?}", e));
                        } else {
                            c.mgr.on_publish_ack.fire((c.clone(), packet_id));
                        }
                    }
                }
            } else if Stats::instance().sends.value() >= opts.pub_max_limit {
                break;
            } else {
                sleep(Duration::from_secs(5)).await;
            }
        }
    }

    async fn ev_loop(c: v3::Client, client: Client) {
        if let Err(e) = c
            .start(
                move |control: v3::client::ControlMessage<()>| match control {
                    v3::client::ControlMessage::Publish(publish) => {
                        Stats::instance().recvs.inc();
                        client
                            .mgr
                            .on_message
                            .fire((client.clone(), publish.packet().clone()));
                        Ready::Ok(publish.ack())
                    }
                    //                    #[allow(deprecated)]
                    //                    v3::client::ControlMessage::Disconnect(msg) => {
                    //                        log::debug!("Server disconnecting: {:?}", msg);
                    //                        Ready::Ok(msg.ack())
                    //                    }
                    v3::client::ControlMessage::Error(msg) => {
                        log::debug!("Codec error: {:?}", msg);
                        Stats::instance().set_last_err(format!("{:?}", msg));
                        Ready::Ok(msg.ack())
                    }
                    v3::client::ControlMessage::ProtocolError(msg) => {
                        log::debug!("Protocol error: {:?}", msg);
                        Stats::instance().set_last_err(format!("{:?}", msg));
                        Ready::Ok(msg.ack())
                    }
                    v3::client::ControlMessage::PeerGone(msg) => {
                        log::debug!("Peer closed connection: {:?}", msg.err());
                        Stats::instance().set_last_err(format!("{:?}", msg));
                        Ready::Ok(msg.ack())
                    }
                    v3::client::ControlMessage::Closed(msg) => {
                        log::debug!("Server closed connection: {:?}", msg);
                        client.subs_clear();
                        Stats::instance().conns.dec();
                        client.mgr.on_disconnected.fire(client.clone());
                        Ready::Ok(msg.ack())
                    }
                },
            )
            .await
        {
            log::error!("start ev_loop error! {:?}", e);
        }
    }
}
