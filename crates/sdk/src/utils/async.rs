use std::future::Future;

#[allow(clippy::unwrap_used)]
pub fn build_tokio_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

pub fn await_fut_sync<T>(future: impl Future<Output = T>) -> T {
    build_tokio_runtime().block_on(future)
}

pub fn spawn_task_as_thread<T: Send + 'static>(
    fut: impl Future<Output = T> + Send + 'static,
) -> std::thread::JoinHandle<T> {
    std::thread::spawn(move || {
        let runtime = build_tokio_runtime();
        runtime.block_on(fut)
    })
}
