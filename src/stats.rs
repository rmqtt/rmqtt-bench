use std::fmt;
use std::fmt::Debug;

use dashmap::DashMap;
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
    pub ifaddrs: DashMap<String, Counter>,
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
            ifaddrs: DashMap::new(),
            last_err: RwLock::new(None),
        })
    }

    pub fn set_last_err(&self, err: String) {
        self.last_err.write().replace(err);
    }

    pub fn ifaddrs_inc(&self, ifaddr: Option<String>) {
        if let Some(ifaddr) = ifaddr {
            self.ifaddrs
                .entry(ifaddr)
                .or_insert_with(|| Counter::new())
                .value()
                .inc();
        }
    }

    pub fn ifaddrs_dec(&self, ifaddr: Option<&String>) {
        if let Some(ifaddr) = ifaddr {
            if let Some(entry) = self.ifaddrs.get_mut(ifaddr) {
                entry.value().dec()
            }
        }
    }

    pub fn to_string(&self) -> String {
        let stats = Stats::instance();

        let conns = stats.conns.value();
        let conns_rate = stats.conns.rate();
        let conns_avg_rate  = stats.conns.avg_rate();
        let conns_medi_rate = stats.conns.medi_rate();

        let subs = stats.subs.value();
        let subs_rate = stats.subs.rate();
        let subs_avg_rate  = stats.subs.avg_rate();
        let subs_medi_rate = stats.subs.medi_rate();

        let conn_fails = stats.conn_fails.value();
        let recvs = stats.recvs.value();
        let recvs_rate = stats.recvs.rate();
        let recvs_avg_rate  = stats.recvs.avg_rate();
        let recvs_medi_rate = stats.recvs.medi_rate();

        let sends = stats.sends.value();
        let sends_rate = stats.sends.rate();
        let closeds = stats.closeds.value();
        let ifaddrs = &stats
            .ifaddrs
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().value()))
            .collect::<Vec<(String, isize)>>();
        format!("* Connecteds:{} ({:0.2?}/s, {:0.2?}/s, {:0.2?}/s), conn_fails:{}, subs:{} ({:0.2?}/s, {:0.2?}/s, {:0.2?}/s), sends:{} {:0.2?}/s, recvs:{} ({:0.2?}/s, {:0.2?}/s, {:0.2?}/s), closeds:{}, ifaddrs: {:?}, last err: {:?}",
                conns, conns_rate, conns_avg_rate, conns_medi_rate, conn_fails, subs, subs_rate, subs_avg_rate,subs_medi_rate, sends, sends_rate, recvs, recvs_rate, recvs_avg_rate, recvs_medi_rate, closeds, ifaddrs, stats.last_err.write().take())
    }
}

pub type StartMillis = i64;
pub type EndMillis = i64;

pub struct Counter{
    inner: RwLock<CounterInner>
}

pub struct CounterInner{
    nums: isize,
    rate: DiscreteRateCounter,
    cost_times: Vec<(StartMillis, EndMillis)>,
}

impl Debug for Counter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self.inner.read();
        write!(f, "(nums: {}, rate: {:?}, cost_times_len: {})", inner.nums, inner.rate.rate(), inner.cost_times.len())
    }
}

impl Counter {
    #[inline]
    fn new() -> Self {
        Counter {
            inner: RwLock::new(
                CounterInner {
                    nums: 0,
                    rate: DiscreteRateCounter::new(100),
                    cost_times: Vec::new()
                })
        }
    }

    #[inline]
    pub fn inc(&self) {
        let mut inner = self.inner.write();
        inner.nums += 1;
        inner.rate.update();
    }

    #[inline]
    pub fn inc2(&self, start: StartMillis, end: EndMillis) {
        let mut inner = self.inner.write();
        inner.nums += 1;
        inner.rate.update();
        inner.cost_times.push((start, end));
    }

    pub fn inc_limit(&self, limit: isize) -> bool {
        let mut inner = self.inner.write();
        if inner.nums < limit {
            inner.nums += 1;
            inner.rate.update();
            true
        }else{
            false
        }
    }

    #[inline]
    pub fn inc_limit_cost_times(&self, limit_cost_times: usize, start: StartMillis, end: EndMillis) {
        let mut inner = self.inner.write();
        inner.nums += 1;
        inner.rate.update();
        if inner.cost_times.len() > limit_cost_times {
            inner.cost_times.remove(0);

        }
        inner.cost_times.push((start, end));
    }

    #[inline]
    pub fn dec(&self) {
        self.inner.write().nums += 1;
    }

    #[inline]
    pub fn decs(&self, v: isize) {
        self.inner.write().nums += v;
    }

    #[inline]
    pub fn value(&self) -> isize {
        self.inner.read().nums
    }

    #[inline]
    pub fn rate(&self) -> f64 {
        self.inner.read().rate.rate()
    }

    #[inline]
    pub fn avg_rate(&self) -> f64 {
        let inner = self.inner.read();
        if inner.cost_times.len() > 1 {
            //average rate
            let (first, _) = inner.cost_times.first().unwrap();
            let (_, last) = inner.cost_times.last().unwrap();
            //println!("avg_rate inner.cost_times.len(): {}, {:?}, first: {}, last: {}", inner.cost_times.len(), ((last - first) as f64 / 1000.0), first, last);
            inner.cost_times.len() as f64 / ((last - first) as f64 / 1000.0)
        }else{
            1.0
        }

    }

    #[inline]
    pub fn medi_rate(&self) -> f64 {
        let inner = self.inner.read();
        if inner.cost_times.len() > 1 {
            //intermediate rate
            let len = inner.cost_times.len();
            let start_idx = (len as f64 * 0.15) as usize;
            let end_idx = len - start_idx - 1;
            let (first, _) = inner.cost_times[start_idx];
            let (_, last) = inner.cost_times[end_idx];
            //println!("medi_rate inner.cost_times.len(): {}, {:?}", end_idx - start_idx, ((last - first) as f64 / 1000.0));
            (end_idx - start_idx) as f64 / ((last - first) as f64 / 1000.0)
        }else{
            1.0
        }

    }
}
