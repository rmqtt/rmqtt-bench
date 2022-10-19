use std::sync::atomic::{AtomicIsize, Ordering};

use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use update_rate::{DiscreteRateCounter, RateCounter};

pub struct Stats {
    pub conns: Counter,
    pub conn_fails: Counter,
    pub subs: Counter,
    pub recvs: Counter,
    pub sends: Counter,
    pub closeds: Counter,
    pub last_err: RwLock<Option<String>>,
}

impl Stats {
    pub fn instance() -> &'static Self {
        static INSTANCE: OnceCell<Stats> = OnceCell::new();
        INSTANCE.get_or_init(|| Self {
            conns: Counter::new(),
            conn_fails: Counter::new(),
            subs: Counter::new(),
            recvs: Counter::new(),
            sends: Counter::new(),
            closeds: Counter::new(),
            last_err: RwLock::new(None),
        })
    }

    pub fn set_last_err(&self, err: String) {
        self.last_err.write().replace(err);
    }

    pub fn to_string(&self) -> String {
        let stats = Stats::instance();
        let conns = stats.conns.value();
        let conns_rate = stats.conns.rate();
        let subs = stats.subs.value();
        let conn_fails = stats.conn_fails.value();
        let recvs = stats.recvs.value();
        let recvs_rate = stats.recvs.rate();
        let sends = stats.sends.value();
        let sends_rate = stats.sends.rate();
        let closeds = stats.closeds.value();
        format!("* Connecteds:{} {:0.2?}/s, conn_fails:{}, subs:{}, sends:{} {:0.2?}/s, recvs:{} {:0.2?}/s, closeds:{}, last err: {:?}",
                conns, conns_rate, conn_fails, subs, sends, sends_rate, recvs, recvs_rate, closeds, stats.last_err.write().take())
    }
}

#[derive(Debug)]
pub struct Counter(AtomicIsize, RwLock<DiscreteRateCounter>);

impl Counter {
    #[inline]
    fn new() -> Self {
        Counter(
            AtomicIsize::new(0),
            RwLock::new(DiscreteRateCounter::new(10)),
        )
    }

    #[inline]
    pub fn inc(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        self.1.write().update();
    }

    #[inline]
    pub fn inc_limit(&self, limit: isize) -> bool {
        let res = self
            .0
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |x| {
                if x < limit {
                    Some(x + 1)
                } else {
                    None
                }
            });
        if res.is_ok() {
            self.1.write().update();
            true
        } else {
            false
        }
    }

    #[inline]
    pub fn dec(&self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }

    #[inline]
    pub fn decs(&self, v: isize) {
        self.0.fetch_sub(v, Ordering::SeqCst);
    }

    #[inline]
    pub fn value(&self) -> isize {
        self.0.load(Ordering::SeqCst)
    }

    #[inline]
    pub fn rate(&self) -> f64 {
        self.1.read().rate()
    }
}
