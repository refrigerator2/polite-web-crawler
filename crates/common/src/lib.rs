pub mod error;
pub mod network;
pub mod parsers;
pub mod task_queue;
pub const STREAM_NAME: &str = "crawler_events";
pub const CRAWLER_TASK_QUEUE_NAME: &str = "crawler_queue";
pub const DEFAULT_AGENT_NAME: &str = "EDUCATIONAL_CRAWLER";
pub const ENV_REDIS_URL: &str = "REDIS_URL";
pub const ENV_STORAGE_LISTEN_ADDR: &str = "STORAGE_LISTEN_ADDR";
pub const ENV_STORAGE_GRPC_ADDR: &str = "STORAGE_GRPC_ADDR";
pub async fn wait_for_shutdown_signal() {
    use tokio::signal;

    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl + C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
