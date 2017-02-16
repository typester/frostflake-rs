//!Customizable and thread-safe distributed id generator, like twitter's [snowflake](https://github.com/twitter/snowflake).
//!
//!## Basic usage for single generator
//!
//!```rust
//!use frostflake::{Generator, GeneratorOptions};
//!use std::thread;
//!
//!let generator = Generator::new(GeneratorOptions::default());
//!
//!{
//!    // generator can be shared with threads by std::sync::Arc
//!    let generator = generator.clone();
//!    thread::spawn(move || {
//!        let mut generator = generator.lock().unwrap();
//!        let id = generator.generate();
//!    }).join();
//!}
//!```
//!
//!## Use multi generator by GeneratorPool
//!
//!```rust
//!use frostflake::{GeneratorPool, GeneratorPoolOptions};
//!use std::thread;
//!
//!// create 10 generators
//!let pool = GeneratorPool::new(10, GeneratorPoolOptions::default());
//!
//!{
//!    // pool also can be shared with threads by std::sync::Arc
//!    let pool = pool.clone();
//!    thread::spawn(move || {
//!        let id = pool.generate();
//!    }).join();
//!}
//!```
//!
//!## Configurations
//!
//!frostflake is highly configurable.
//!
//!```rust
//!use frostflake::{Generator, GeneratorOptions};
//!
//!let opts = GeneratorOptions::default()
//!    .bits(42, 10, 12)           // 42bit timestamp, 10bit node, 12bit sequence
//!    .base_ts(1483228800000)     // base time 2017-01-01T00:00:00Z as milliseonds
//!    .node(3);                   // node number
//!
//!let generator = Generator::new(opts);
//!```
//!
//!Also, time function is can be set.
//!If you want to use plain seconds unit instead of millisedond, you can do by this:
//!
//!```rust
//!use frostflake::{Generator, GeneratorOptions};
//!use std::time::{SystemTime, UNIX_EPOCH};
//!
//!fn my_time() -> u64 {
//!    let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
//!    t.as_secs()
//!}
//!
//!// use smaller time bits (because this is not milliseconds)
//!// use larger sequence bits
//!let opts = GeneratorOptions::default()
//!    .base_ts(0) // need this for avoid time exceeding on smaller bit size
//!    .bits(36, 10, 18)
//!    .base_ts(1483228800) // base time should be second too
//!    .time_fn(my_time); // set my time function
//!
//!let generator = Generator::new(opts);
//!```
//!
//!### Default configurations
//!
//!#### Generator
//!
//!|bits| 42=timestamp, 10=node, 12=sequence |
//!|base\_ts|1483228800000 (2017-01-01T00:00:00Z as milliseonds)|
//!|node|0|
//!|time\_fn|return current milliseonds|
//!
//!#### GeneratorPool
//!
//!Almost same as Generator, but GeneratorPool uses `pool_id` bit for distinguish each pools.
//!
//!So default bit widths is:
//!
//!|bits| 42=timestamp, 4=pool_id, 6=node, 12=sequence |

use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use std::u64::MAX;

pub mod pool;

pub use pool::{GeneratorPool, GeneratorPoolOptions};

#[derive(Clone)]
pub struct GeneratorOptions {
    bits: (u8, u8, u8),
    base_ts: u64,
    node: u64,
    time_fn: fn() -> u64,
}

pub struct Generator {
    opts: GeneratorOptions,
    last_ts: u64,
    seq: u64,
}

fn default_time_fn() -> u64 {
    let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    t.as_secs() * 1000 + (t.subsec_nanos() as u64) / 1000000
}

impl GeneratorOptions {
    pub fn default() -> GeneratorOptions {
        GeneratorOptions {
            bits: (42, 10, 12),
            base_ts: 1483228800000, // 2017-01-01T00:00:00Z as milliseconds
            node: 0,
            time_fn: default_time_fn,
        }
    }

    pub fn time_fn(mut self, time_fn: fn() -> u64) -> Self {
        self.time_fn = time_fn;
        self
    }

    pub fn bits(mut self, ts_bits: u8, node_bits: u8, seq_bits: u8) -> Self {
        assert!(64 == ts_bits + node_bits + seq_bits,
                "bits set should be total 64bit");
        assert!(self.base_ts <= max(ts_bits),
                "base_ts exceeds ts_bits limit, set base_ts first");
        assert!(self.node <= max(node_bits),
                "node number exceeeds node_bits limit, set node number first");

        self.bits = (ts_bits, node_bits, seq_bits);
        self
    }

    pub fn node(mut self, node: u64) -> Self {
        assert!(node <= max(self.bits.1),
                "node number exceeds node_bits limit, set bit width first");

        self.node = node;
        self
    }

    pub fn base_ts(mut self, base_ts: u64) -> Self {
        assert!(base_ts <= max(self.bits.0),
                "base_ts exceeds ts_bits limit, set bit width first");

        self.base_ts = base_ts;
        self
    }
}

impl Generator {
    pub fn new(opts: GeneratorOptions) -> Arc<Mutex<Generator>> {
        Arc::new(Mutex::new(Generator::new_raw(opts)))
    }

    pub fn new_raw(opts: GeneratorOptions) -> Generator {
        Generator {
            opts: opts,
            last_ts: 0,
            seq: 0,
        }
    }

    pub fn generate(&mut self) -> u64 {
        let now = (self.opts.time_fn)();
        assert!(now > self.opts.base_ts,
                "time_fn returned the time before base_ts");
        assert!(now >= self.last_ts,
                "clock moved backwards. check your NTP setup");

        let elapsed = now - self.opts.base_ts;

        let seq = if now == self.last_ts { self.seq + 1 } else { 0 };

        let (_, node_bits, seq_bits) = self.opts.bits;

        assert!(seq <= max(seq_bits), "seq number exceeds seq_bits!");

        let ts_mask = bitmask(node_bits + seq_bits);
        let node_mask = bitmask(seq_bits) ^ ts_mask;

        self.last_ts = now;
        self.seq = seq;

        ((elapsed << (node_bits + seq_bits)) & ts_mask) |
        ((self.opts.node << seq_bits) & node_mask) | seq & max(seq_bits)
    }

    pub fn extract(&self, id: u64) -> (u64, u64, u64) {
        let (ts_bits, node_bits, seq_bits) = self.opts.bits;

        let ts = (id >> (node_bits + seq_bits)) & max(ts_bits);
        let node = (id >> seq_bits) & max(node_bits);
        let seq = id & max(seq_bits);

        (ts, node, seq)
    }
}

fn bitmask(shift: u8) -> u64 {
    MAX << shift
}

fn max(shift: u8) -> u64 {
    !bitmask(shift)
}

#[test]
fn test_basic() {
    fn my_time_fn() -> u64 {
        1483228800000 + 123
    }

    let opts = GeneratorOptions::default().time_fn(my_time_fn);

    let g = Generator::new(opts);

    let mut g = g.lock().unwrap();
    assert_eq!(g.generate(), (123 << 22));
    assert_eq!(g.generate(), (123 << 22) + 1);
    assert_eq!(g.generate(), (123 << 22) + 2);

}

#[test]
fn test_extract() {
    fn my_time_fn() -> u64 {
        1483228800000 + 123
    }

    let opts = GeneratorOptions::default().time_fn(my_time_fn).node(3);

    let g = Generator::new(opts);

    let mut g = g.lock().unwrap();

    let id = g.generate();
    let (ts, node, seq) = g.extract(id);
    assert_eq!(ts, 123);
    assert_eq!(node, 3);
    assert_eq!(seq, 0);

    let id = g.generate();
    let (ts, node, seq) = g.extract(id);
    assert_eq!(ts, 123);
    assert_eq!(node, 3);
    assert_eq!(seq, 1);
}

#[test]
fn test_bitmask() {
    assert_eq!(bitmask(1), 0xFFFFFFFFFFFFFFFE);
    assert_eq!(bitmask(4), 0xFFFFFFFFFFFFFFF0);
    assert_eq!(bitmask(8), 0xFFFFFFFFFFFFFF00);
}

#[test]
fn test_max() {
    assert_eq!(max(1), 1);
    assert_eq!(max(2), 3);
    assert_eq!(max(8), 255);
}

#[cfg(test)]
use std::thread;
#[cfg(test)]
use std::collections::HashMap;

#[test]
fn test_threaded() {
    let g = Generator::new(GeneratorOptions::default());
    let mut h = vec![];
    let results = Arc::new(Mutex::new(vec![]));

    for _ in 1..10 {
        let g = g.clone();
        let r = results.clone();

        let handle = thread::spawn(move || for _ in 1..100 {
            let mut g = g.lock().unwrap();
            let mut r = r.lock().unwrap();
            r.push(g.generate());
        });
        h.push(handle);
    }

    for handle in h {
        let _ = handle.join();
    }

    let mut hash: HashMap<u64, u64> = HashMap::new();
    for r in results.lock().unwrap().iter() {
        // check uniqueness
        assert_eq!(hash.contains_key(r), false);
        hash.insert(*r, 1);
        assert_eq!(hash.contains_key(r), true);
    }
}
