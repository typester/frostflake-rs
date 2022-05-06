use std::sync::Arc;
use std::thread;

use crossbeam::channel::unbounded;
use crossbeam::channel::Sender;

use super::{Generator, GeneratorOptions};

#[derive(Clone)]
pub struct GeneratorPoolOptions {
    bits: (u8, u8, u8, u8), // time, pool, node, seq
    node: u64,
    base_ts: u64,
    time_fn: fn() -> u64,
}

impl Default for GeneratorPoolOptions {
    fn default() -> GeneratorPoolOptions {
        GeneratorPoolOptions {
            bits: (42, 4, 6, 12),
            base_ts: 1483228800000, // 2017-01-01T00:00:00Z as milliseconds
            node: 0,
            time_fn: super::default_time_fn,
        }
    }
}

impl GeneratorPoolOptions {
    pub fn bits(mut self, ts_bits: u8, pool_bits: u8, node_bits: u8, seq_bits: u8) -> Self {
        assert!(
            64 == ts_bits + pool_bits + node_bits + seq_bits,
            "bits set should be total 64bit"
        );
        assert!(
            self.base_ts <= super::max(ts_bits),
            "base_ts exceeds ts_bits limit, set base_ts first"
        );
        assert!(
            self.node <= super::max(node_bits),
            "node number exceeds node_bits limit, set node number first"
        );

        self.bits = (ts_bits, pool_bits, node_bits, seq_bits);
        self
    }

    pub fn base_ts(mut self, base_ts: u64) -> Self {
        assert!(
            base_ts <= super::max(self.bits.0),
            "base_ts exceeds ts_bits limit, set bit width first"
        );

        self.base_ts = base_ts;
        self
    }

    pub fn node(mut self, node: u64) -> Self {
        assert!(
            node <= super::max(self.bits.2),
            "node number exceeds node_bits limit, set bit width first"
        );

        self.node = node;
        self
    }

    pub fn time_fn(mut self, time_fn: fn() -> u64) -> Self {
        self.time_fn = time_fn;
        self
    }
}

enum Message {
    Job(Sender<u64>),
}

pub struct GeneratorPool {
    size: usize,
    opts: GeneratorPoolOptions,
    tx: Sender<Message>,
}

impl GeneratorPool {
    pub fn new(size: usize, opts: GeneratorPoolOptions) -> Arc<GeneratorPool> {
        let generator_opts = GeneratorPool::generator_opts(opts.clone());

        let (tx, rx) = unbounded::<Message>();

        for i in 0..size {
            let rx = rx.clone();

            let (_, _, node_bits, _) = opts.bits;
            let pool_mask = super::bitmask(node_bits);
            let node_mask = super::max(node_bits);

            let opts = generator_opts
                .clone()
                .node((((i as u64) << node_bits) & pool_mask) | (opts.node & node_mask));

            thread::spawn(move || {
                let mut generator = Generator::new_raw(opts);

                while let Ok(msg) = rx.recv() {
                    match msg {
                        Message::Job(tx) => {
                            if let Err(e) = tx.send(generator.generate()) {
                                eprintln!("Failed to send generated result: {:?}", e);
                            }
                        }
                    }
                }
            });
        }

        Arc::new(GeneratorPool {
            size: size,
            opts: opts,
            tx,
        })
    }

    fn generator_opts(opts: GeneratorPoolOptions) -> GeneratorOptions {
        GeneratorOptions::default()
            .base_ts(0)
            .bits(opts.bits.0, opts.bits.1 + opts.bits.2, opts.bits.3)
            .base_ts(opts.base_ts)
            .time_fn(opts.time_fn)
    }

    pub fn generate(&self) -> u64 {
        let (tx, rx) = unbounded();

        if let Err(e) = self.tx.send(Message::Job(tx)) {
            eprintln!("failed to send generate request: {:?}", e);
        }

        rx.recv().unwrap()
    }

    pub fn extract(&self, id: u64) -> (u64, u64, u64, u64) {
        let g = Generator::new_raw(GeneratorPool::generator_opts(self.opts.clone()));
        let (_, pool_bits, node_bits, _) = self.opts.bits;
        let (ts, poolnode, seq) = g.extract(id);

        let pool = (poolnode >> node_bits) & super::max(pool_bits);
        let node = poolnode & super::max(node_bits);

        (ts, pool, node, seq)
    }
}

#[cfg(test)]
use std::sync::Mutex;

#[test]
fn test_options_default() {
    let opts = GeneratorPoolOptions::default();
    assert_eq!(opts.bits, (42, 4, 6, 12));
    assert_eq!(opts.node, 0);
    assert_eq!(opts.base_ts, 1483228800000);
}

#[test]
fn test_options_set_base() {
    let opts = GeneratorPoolOptions::default().base_ts(123);
    assert_eq!(opts.base_ts, 123);
}

#[test]
#[should_panic]
fn test_options_set_base_crash() {
    let max = super::max(42);
    let _ = GeneratorPoolOptions::default().base_ts(max + 1);
}

#[test]
fn test_options_set_node() {
    let opts = GeneratorPoolOptions::default().node(10);
    assert_eq!(opts.node, 10);
}

#[test]
#[should_panic]
fn test_options_set_node_crash() {
    let max = super::max(6);
    let _ = GeneratorPoolOptions::default().node(max + 1);
}

#[test]
fn test_options_set_time_fn() {
    fn test_fn() -> u64 {
        1483228800000 + 123
    }

    let opts = GeneratorPoolOptions::default().time_fn(test_fn);
    assert_eq!((opts.time_fn)(), 1483228800000 + 123);
}

#[cfg(test)]
use std::collections::HashMap;

#[test]
fn test_pool() {
    let pool = GeneratorPool::new(3, GeneratorPoolOptions::default());

    let mut handles = vec![];
    let results = Arc::new(Mutex::new(vec![]));

    for _ in 0..10 {
        let pool = pool.clone();
        let results = results.clone();

        handles.push(thread::spawn(move || {
            for _ in 0..10 {
                let mut results = results.lock().unwrap();
                results.push(pool.generate());
            }
        }));
    }

    for h in handles {
        let _ = h.join();
    }

    let mut hash: HashMap<u64, u64> = HashMap::new();
    let results = results.lock().unwrap();
    for r in results.iter() {
        // check uniqueness
        assert_eq!(hash.contains_key(r), false);
        hash.insert(*r, 1);
        assert_eq!(hash.contains_key(r), true);
    }
}

#[test]
fn test_pool_extract() {
    fn test_fn() -> u64 {
        1483228800000 + 12345
    }

    let opts = GeneratorPoolOptions::default().time_fn(test_fn);

    let pool = GeneratorPool::new(1, opts.clone());

    let g = Generator::new_raw(GeneratorPool::generator_opts(opts.clone()));

    let id = pool.generate();
    let (ts, node, seq) = g.extract(id);
    assert_eq!(ts, 12345);
    assert_eq!(node, 0);
    assert_eq!(seq, 0);

    let id = pool.generate();
    let (ts, node, seq) = g.extract(id);
    assert_eq!(ts, 12345);
    assert_eq!(node, 0);
    assert_eq!(seq, 1);
}

#[test]
fn test_pool_extract_poolnode() {
    fn test_fn() -> u64 {
        1483228800000 + 12345
    }

    let opts = GeneratorPoolOptions::default().time_fn(test_fn).node(3);

    let pool = GeneratorPool::new(2, opts);

    let mut ok = (false, false);
    let mut counters = (0, 0);
    for _ in 1..1000 {
        let id = pool.generate();
        let (ts, pool_id, node, seq) = pool.extract(id);

        assert_eq!(ts, 12345);
        assert_eq!(node, 3);
        assert!(pool_id == 0 || pool_id == 1);

        if pool_id == 0 {
            assert_eq!(counters.0, seq);
            counters.0 += 1;
            ok.0 = true;
        } else {
            assert_eq!(counters.1, seq);
            counters.1 += 1;
            ok.1 = true;
        }
    }

    assert!(ok.0 && ok.1);
}
