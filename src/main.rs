mod raftmachine;
use std::io;
use std::sync::Arc;

#[tokio::main]
async fn main() -> io::Result<()> {
    let cluster = raftmachine::RaftCluster::new();
    raftmachine::run(Arc::new(cluster), "127.0.0.1:8989").await?;
    Ok(())
}
