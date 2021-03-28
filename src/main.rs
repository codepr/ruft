mod raftmachine;
use std::io;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> io::Result<()> {
    let notify = Arc::new(Notify::new());
    let cluster = Arc::new(Mutex::new(raftmachine::RaftCluster::new(notify.clone())));
    tokio::spawn(raftmachine::run(cluster.clone(), notify, "127.0.0.1:9999"));
    let mut cluster = cluster.lock().unwrap();
    cluster.start(["127.0.0.1:9999".into()].to_vec()).await?;
    sleep(Duration::from_secs(10)).await;
    Ok(())
}
