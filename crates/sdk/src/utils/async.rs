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
