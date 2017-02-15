use std::sync::{Arc, Mutex, Condvar};
use std::collections::VecDeque;

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
        assert!(64 == ts_bits + pool_bits + node_bits + seq_bits,
                "bits set should be total 64bit");
        assert!(self.base_ts <= super::max(ts_bits),
                "base_ts exceeds ts_bits limit, set base_ts first");
        assert!(self.node <= super::max(node_bits),
                "node number exceeds node_bits limit, set node number first");

        self.bits = (ts_bits, pool_bits, node_bits, seq_bits);
        self
    }

    pub fn base_ts(mut self, base_ts: u64) -> Self {
        assert!(base_ts <= super::max(self.bits.0),
                "base_ts exceeds ts_bits limit, set bit width first");

        self.base_ts = base_ts;
        self
    }

    pub fn node(mut self, node: u64) -> Self {
        assert!(node <= super::max(self.bits.2),
                "node number exceeds node_bits limit, set bit width first");

        self.node = node;
        self
    }

    pub fn time_fn(mut self, time_fn: fn() -> u64) -> Self {
        self.time_fn = time_fn;
        self
    }
}

#[derive(Clone)]
pub struct GeneratorPool {
    size: usize,
    opts: GeneratorPoolOptions,
    inner: Arc<(Mutex<InnerPool>, Condvar)>,
}

impl GeneratorPool {
    pub fn new(size: usize, opts: GeneratorPoolOptions) -> GeneratorPool {
        let pool = InnerPool::new(size, opts.clone());

        GeneratorPool {
            size: size,
            opts: opts,
            inner: Arc::new((Mutex::new(pool), Condvar::new())),
        }
    }

    pub fn get_generator(&self) -> PooledGenerator {
        let &(ref inner_pool, ref cv) = &*self.inner;

        let out_generator;
        let mut pool = inner_pool.lock().unwrap();
        loop {
            if let Some(generator) = pool.pool.pop_front() {
                drop(pool);
                out_generator = generator;
                break;
            } else {
                pool = cv.wait(pool).unwrap();
            }
        }

        PooledGenerator {
            pool: self.clone(),
            generator: Some(out_generator),
        }
    }
}

struct InnerPool {
    pool: VecDeque<Generator>,
}

impl InnerPool {
    fn new(size: usize, opts: GeneratorPoolOptions) -> InnerPool {
        assert!(size > 0, "pool size should be positive");

        let mut pool = InnerPool { pool: VecDeque::with_capacity(size) };

        let generator_opts = GeneratorOptions::default()
            .base_ts(0)
            .bits(opts.bits.0, opts.bits.1 + opts.bits.2, opts.bits.3)
            .base_ts(opts.base_ts);

        for i in 0..size {
            let (_, pool_bits, node_bits, _) = opts.bits;
            let pool_mask = super::bitmask(node_bits);
            let node_mask = super::max(node_bits);

            let opts = generator_opts.clone()
                .node((((i as u64) << node_bits) & pool_mask) | (opts.node & node_mask));

            let generator = Generator::new_raw(opts);
            pool.pool.push_back(generator);
        }

        pool
    }
}

pub struct PooledGenerator {
    pool: GeneratorPool,
    generator: Option<Generator>,
}

impl Drop for PooledGenerator {
    fn drop(&mut self) {
        let mut pool = (self.pool.inner).0.lock().unwrap();
        pool.pool.push_back(self.generator.take().unwrap());
        drop(pool);
        (self.pool.inner).1.notify_one();
    }
}

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
use std::thread;

#[test]
fn test_pool() {
    let pool = GeneratorPool::new(10, GeneratorPoolOptions::default());

    println!("generated: {}",
             pool.get_generator().generator.as_mut().unwrap().generate());
    println!("generated: {}",
             pool.get_generator().generator.as_mut().unwrap().generate());
    println!("generated: {}",
             pool.get_generator().generator.as_mut().unwrap().generate());
    println!("generated: {}",
             pool.get_generator().generator.as_mut().unwrap().generate());
    println!("generated: {}",
             pool.get_generator().generator.as_mut().unwrap().generate());
    println!("generated: {}",
             pool.get_generator().generator.as_mut().unwrap().generate());
    println!("generated: {}",
             pool.get_generator().generator.as_mut().unwrap().generate());
}
