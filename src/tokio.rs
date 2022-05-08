use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use crate::{Generator, GeneratorOptions};

#[derive(Debug)]
pub enum Event {
    Generate(oneshot::Sender<u64>),
}

pub struct GeneratorAsync {
    tx: mpsc::Sender<Event>,
}

impl GeneratorAsync {
    pub fn spawn(opts: GeneratorOptions) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(10);
        tokio::spawn(async move { generator_task(rx, opts).await });
        Arc::new(GeneratorAsync { tx })
    }

    pub async fn generate(&self) -> anyhow::Result<u64> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(Event::Generate(tx)).await?;
        Ok(rx.await?)
    }
}

async fn generator_task(
    mut rx: mpsc::Receiver<Event>,
    opts: GeneratorOptions,
) -> anyhow::Result<()> {
    let mut generator = Generator::new(opts);
    while let Some(evt) = rx.recv().await {
        match evt {
            Event::Generate(tx) => {
                let id = generator.generate();
                tx.send(id).expect("failed to send oneshot message");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_simple() {
        fn my_time_fn() -> u64 {
            1483228800000 + 123
        }

        let g = GeneratorAsync::spawn(GeneratorOptions::default().time_fn(my_time_fn));
        assert_eq!(g.generate().await.unwrap(), (123 << 22));
        assert_eq!(g.generate().await.unwrap(), (123 << 22) + 1);
        assert_eq!(g.generate().await.unwrap(), (123 << 22) + 2);
    }

    #[tokio::test]
    async fn test_multiple_task() {
        let (tx, mut rx) = mpsc::channel(10);

        fn my_time_fn() -> u64 {
            1483228800000 + 123
        }

        let g = GeneratorAsync::spawn(GeneratorOptions::default().time_fn(my_time_fn));
        for _ in 0..10 {
            let g = g.clone();
            let tx = tx.clone();
            tokio::spawn(async move { tx.send(g.generate().await.unwrap()).await });
        }

        for i in 0..10 {
            let r = rx.recv().await.unwrap();
            assert_eq!(r, (123 << 22) + i);
        }
        assert!(rx.try_recv().is_err());
    }
}
