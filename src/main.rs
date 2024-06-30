use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use ntex::rt::spawn;
use ntex::time::sleep;
use ntex::util::ByteString;
use ntex_mqtt::v3::codec::Publish;
use ntex_mqtt::v3::QoS;
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use rust_box::event::Event;
use rust_box::std_ext::ArcExt;
use structopt::StructOpt;

use options::{Command, Options, V3 as V3Options};
use stats::Stats;
use v3::Client;

type DashMap<K, V> = dashmap::DashMap<K, V, ahash::RandomState>;

mod connector;
mod options;
mod script;
mod stats;
mod v3;

#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[ntex::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("RUST_LOG", "rmqtt_bench=info");
    env_logger::init();

    let opts = Options::from_args();
    println!("{:?}", opts);

    match opts.command {
        Command::V3(v3) => {
            v3_benchmarks(*v3).await;
        }
        Command::V5(_v5) => {
            println!("Unrealized.")
        }
    }

    Ok(())
}

async fn v3_benchmarks(opts: V3Options) {
    let mgr = ControlManager::init(Arc::new(opts.clone()));
    for i in 0..opts.conns {
        let c = v3::Client::new(i, mgr.clone());
        mgr.add_client(c);
    }

    let output_interval = Duration::from_secs(if opts.output_interval > 0 {
        opts.output_interval
    } else {
        1
    });
    std::thread::spawn(move || loop {
        std::thread::sleep(output_interval);
        println!("{}", Stats::instance().to_str());
    });

    spawn(async move {
        let conn_interval = Duration::from_millis(opts.conn_interval);
        let mut exit_futs = Vec::new();
        for i in 0..mgr.clients_len() {
            if let Some(c) = mgr.client(i) {
                let exit_fut = c.start().await;
                exit_futs.push(exit_fut.into_future());
                if !conn_interval.is_zero() {
                    sleep(conn_interval).await;
                }
            }
        }
        futures::future::join_all(exit_futs).await;
    })
    .await
    .unwrap();

    println!("{}", Stats::instance().to_str());
}

pub type TopicFilter = ByteString;
pub type Topic = ByteString;
pub type Retain = bool;
pub type Payload = ntex::util::Bytes;
pub type PacketId = u16;

#[derive(Clone)]
pub struct ControlManager {
    opts: Arc<V3Options>,
    clients: Arc<RwLock<Vec<Client>>>,
    connecteds: Arc<DashMap<String, Client>>,
    disconnecteds: Arc<DashMap<String, Client>>,

    on_connect: Arc<Event<Client, bool>>,
    on_connected: Arc<Event<Client, ()>>,
    on_disconnected: Arc<Event<Client, ()>>,
    on_subscribe: Arc<Event<(Client, TopicFilter, QoS), bool>>,
    on_subscribed: Arc<Event<(Client, TopicFilter, QoS), ()>>,
    #[allow(clippy::type_complexity)]
    on_publish: Arc<Event<(Client, PacketId, Topic, QoS, Retain, Payload), bool>>,
    on_publish_ack: Arc<Event<(Client, PacketId), ()>>,
    on_message: Arc<Event<(Client, Publish), ()>>,
}

unsafe impl Send for ControlManager {}

unsafe impl Sync for ControlManager {}

static CONTROL_MANAGER: OnceCell<ControlManager> = OnceCell::new();

impl ControlManager {
    pub fn init(opts: Arc<V3Options>) -> &'static ControlManager {
        CONTROL_MANAGER.set(Self::new(opts)).ok().unwrap();
        CONTROL_MANAGER.get().unwrap()
    }

    pub fn instance() -> &'static ControlManager {
        CONTROL_MANAGER.get().unwrap()
    }

    #[allow(clippy::arc_with_non_send_sync)]
    fn new(opts: Arc<V3Options>) -> Self {
        Self {
            opts,
            clients: Arc::new(RwLock::new(Vec::new())),
            connecteds: Arc::new(DashMap::default()),
            disconnecteds: Arc::new(DashMap::default()),

            on_connect: Event::listen(|_args, _next| true).finish().arc(),
            on_connected: Event::listen(|c: Client, _next| {
                Stats::instance().ifaddrs_inc(c.ifaddr());
                ControlManager::instance().add_connected(c);
            })
            .finish()
            .arc(),
            on_disconnected: Event::listen(|c: Client, _next| {
                Stats::instance().ifaddrs_dec(c.ifaddr().as_ref());
                ControlManager::instance().add_disconnected(c);
            })
            .finish()
            .arc(),
            on_subscribe: Event::listen(|_args, _next| true).finish().arc(),
            on_subscribed: Event::listen(|_args, _next| ()).finish().arc(),
            on_publish: Event::listen(|_args, _next| true).finish().arc(),
            on_publish_ack: Event::listen(|_args, _next| ()).finish().arc(),
            on_message: Event::listen(|_args, _next| ()).finish().arc(),
        }
        .control()
    }

    fn add_client(&self, c: Client) {
        self.clients.write().push(c);
    }

    fn clients_len(&self) -> usize {
        self.clients.read().len()
    }

    fn client(&self, idx: usize) -> Option<Client> {
        self.clients.read().get(idx).cloned()
    }

    fn connecteds_len(&self) -> usize {
        self.connecteds.len()
    }

    fn add_connected(&self, c: Client) {
        self.disconnecteds.remove(&c.client_id);
        self.connecteds.insert(c.client_id.clone(), c);
    }

    fn add_disconnected(&self, c: Client) {
        self.connecteds.remove(&c.client_id);
        self.disconnecteds.insert(c.client_id.clone(), c);
    }

    fn control(self) -> Self {
        //The control center
        let mgr = self.clone();
        let runner = async move {
            let ctrl_interval = Duration::from_millis(mgr.opts.ctrl_interval);
            let disconn_ratio = mgr.opts.ctrl_disconn_ratio;
            let count_8 = (mgr.opts.conns as f32 * (1.0 - disconn_ratio)) as usize;
            let count_2 = (mgr.opts.conns as f32 * disconn_ratio) as usize;
            let mgr1 = mgr.clone();
            let disconnects = |n: usize| {
                let mut count = 0;
                let mut cids = Vec::new();
                for c in mgr1.connecteds.iter() {
                    if c.disconnect() {
                        cids.push(c.client_id.clone());
                        count += 1;
                    }
                    if count >= n {
                        break;
                    }
                }
                for cid in &cids {
                    mgr1.connecteds.remove(cid);
                }
            };

            let mgr2 = mgr.clone();
            let connects = |n: usize| {
                let mut cids = Vec::new();
                for (i, c) in mgr2.disconnecteds.iter().enumerate() {
                    if i >= n {
                        break;
                    }
                    c.connect();
                    cids.push(c.client_id.clone());
                }
                for cid in &cids {
                    mgr2.disconnecteds.remove(cid);
                }
            };

            loop {
                // log::info!("total: {:?}, connecteds: {}, disconnecteds: {}", mgr.clients_len(), mgr.connecteds_len(), mgr.disconnecteds_len());
                if mgr.connecteds_len() >= count_8 {
                    disconnects(count_2);
                }

                if mgr.connecteds_len() < mgr.opts.conns {
                    connects(count_2);
                }

                sleep(ctrl_interval).await;
            }
        };
        if self.opts.control {
            spawn(runner);
        }
        self
    }
}

#[inline]
pub fn timestamp_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|t| t.as_millis() as i64)
        .unwrap_or_else(|_| chrono::Local::now().timestamp_millis())
}
