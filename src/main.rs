use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> ruft::AsyncResult<()> {
    tokio::spawn(async {
        let listener = TcpListener::bind("127.0.0.1:4989".to_string()).await?;
        ruft::rpc::run(
            listener,
            [
                "127.0.0.1:4989".into(),
                "127.0.0.1:4990".into(),
                "127.0.0.1:4991".into(),
            ]
            .to_vec(),
        )
        .await
    });
    tokio::spawn(async {
        let listener = TcpListener::bind("127.0.0.1:4990".to_string()).await?;
        ruft::rpc::run(
            listener,
            [
                "127.0.0.1:4989".into(),
                "127.0.0.1:4990".into(),
                "127.0.0.1:4991".into(),
            ]
            .to_vec(),
        )
        .await
    });
    let listener = TcpListener::bind("127.0.0.1:4991".to_string())
        .await
        .unwrap();
    ruft::rpc::run(
        listener,
        [
            "127.0.0.1:4989".into(),
            "127.0.0.1:4990".into(),
            "127.0.0.1:4991".into(),
        ]
        .to_vec(),
    )
    .await
}
