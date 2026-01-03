use tokio::net::TcpListener;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, split};
use tokio::sync::broadcast;
use tokio::spawn;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("Chat server running on port 8080...");

    let (tx, _rx) = broadcast::channel::<String>(16);

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("New client: {}", addr);

        let tx = tx.clone();
        let mut rx = tx.subscribe();

        spawn(async move {
            let (reader, writer) = split(socket);
            let mut reader = BufReader::new(reader);
            let mut writer = writer;
            let mut line = String::new();

            let mut write_task = spawn(async move {
                while let Ok(msg) = rx.recv().await {
                    if writer.write_all(msg.as_bytes()).await.is_err() {
                        break;
                    }
                }
            });

            let mut read_task = spawn(async move {
                loop {
                    line.clear();
                    let bytes = reader.read_line(&mut line).await.unwrap();
                    if bytes == 0 {
                        break;
                    }

                    let msg = format!("{}: {}", addr, line);
                    let _ = tx.send(msg);
                }
            });

            tokio::select! {
                _ = &mut write_task => read_task.abort(),
                _ = &mut read_task => write_task.abort(),
            }

            println!("Client disconnected: {}", addr);
        });
    }
}


