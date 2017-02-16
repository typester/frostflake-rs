# frostflake

[![Build Status](https://travis-ci.org/typester/frostflake-rs.svg?branch=master)](https://travis-ci.org/typester/frostflake-rs)

Customizable and thread-safe distributed id generator, like twitter's [snowflake](https://github.com/twitter/snowflake).

## Basic usage for single generator

```rust
use frostflake::{Generator, GeneratorOptions};
use std::thread;

let generator = Generator::new(GeneratorOptions::default());

{
    // generator can be shared with threads by std::sync::Arc
    let generator = generator.clone();
    thread::spawn(move || {
        let mut generator = generator.lock().unwrap();
        let id = generator.generate();
    }).join();
}
```

## Use multi generator by GeneratorPool

```rust
use frostflake::{GeneratorPool, GeneratorPoolOptions};
use std::thread;

// create 10 generators
let pool = GeneratorPool::new(10, GeneratorPoolOptions::default());

{
    // pool also can be shared with threads by std::sync::Arc
    let pool = pool.clone();
    thread::spawn(move || {
        let id = pool.generate();
    }).join();
}
```

## Configurations

frostflake is highly configurable.

```rust
use frostflake::{Generator, GeneratorOptions};

let opts = GeneratorOptions::default()
    .base_ts(0) // need this for avoid time exceeding on smaller bit size
    .bits(42, 10, 12)           // 42bit timestamp, 10bit node, 12bit sequence
    .base_ts(1483228800000)     // base time 2017-01-01T00:00:00Z as milliseonds
    .node(3);                   // node number

let generator = Generator::new(opts);
```

Also, time function is can be set.
If you want to use plain seconds unit instead of millisedond, you can do by this:

```rust
use frostflake::{Generator, GeneratorOptions};
use std::time::{SystemTime, UNIX_EPOCH};

fn my_time() -> u64 {
    let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    t.as_secs()
}

// use smaller time bits (because this is not milliseconds)
// use larger sequence bits
let opts = GeneratorOptions::default()
    .bits(36, 10, 18)
    .base_ts(1483228800) // base time should be second too
    .time_fn(my_time); // set my time function

let generator = Generator::new(opts);
```

### Default configurations

#### Generator

|bits| 42=timestamp, 10=node, 12=sequence |
|base\_ts|1483228800000 (2017-01-01T00:00:00Z as milliseonds)|
|node|0|
|time\_fn|return current milliseonds|

#### GeneratorPool

Almost same as Generator, but GeneratorPool uses `pool_id` bit for distinguish each pools.

So default bit widths is:

|bits| 42=timestamp, 4=pool_id, 6=node, 12=sequence |
