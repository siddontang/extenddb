// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! Keep a shared SDK client's connection tasks alive across #[tokio::test]
//! runtimes. Both constructing and polling each connector call happen on a
//! process-lifetime runtime; request signing and assertions stay in the test.
//! Timeouts, TLS verification, and SDK retry policy remain unchanged.

use aws_smithy_runtime_api::client::{
    http::{
        http_client_fn, HttpClient, HttpConnector, HttpConnectorFuture, SharedHttpClient,
        SharedHttpConnector,
    },
    orchestrator::HttpRequest,
    result::ConnectorError,
};
use std::sync::OnceLock;

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("SDK transport runtime")
    })
}

#[derive(Debug)]
struct RuntimeConnector(SharedHttpConnector);

struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl HttpConnector for RuntimeConnector {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let inner = self.0.clone();
        let mut task = AbortOnDrop(runtime().spawn(async move { inner.call(request).await }));
        HttpConnectorFuture::new(async move {
            (&mut task.0)
                .await
                .map_err(|e| ConnectorError::other(Box::new(e), None))?
        })
    }
}

pub fn shared_runtime(client: SharedHttpClient) -> SharedHttpClient {
    http_client_fn(move |settings, components| {
        let _guard = runtime().enter();
        SharedHttpConnector::new(RuntimeConnector(
            client.http_connector(settings, components),
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_smithy_runtime_api::client::orchestrator::HttpResponse;
    use aws_smithy_types::body::SdkBody;

    #[derive(Debug)]
    struct Probe;
    impl HttpConnector for Probe {
        fn call(&self, _: HttpRequest) -> HttpConnectorFuture {
            assert_eq!(
                tokio::runtime::Handle::current().id(),
                runtime().handle().id()
            );
            HttpConnectorFuture::new(async {
                tokio::task::yield_now().await;
                assert_eq!(
                    tokio::runtime::Handle::current().id(),
                    runtime().handle().id()
                );
                Ok(HttpResponse::new(
                    aws_smithy_runtime_api::http::StatusCode::try_from(200).unwrap(),
                    SdkBody::empty(),
                ))
            })
        }
    }

    #[test]
    fn connector_survives_test_runtime_teardown() {
        let connector = RuntimeConnector(SharedHttpConnector::new(Probe));
        for _ in 0..4 {
            let test_runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            test_runtime.block_on(async {
                connector
                    .call(HttpRequest::new(SdkBody::empty()))
                    .await
                    .unwrap();
            });
            drop(test_runtime);
        }
    }

    #[test]
    fn dropping_request_cancels_transport_task() {
        #[derive(Debug)]
        struct Pending {
            started: std::sync::mpsc::Sender<()>,
            dropped: std::sync::mpsc::Sender<()>,
        }
        struct Notice(std::sync::mpsc::Sender<()>);
        impl Drop for Notice {
            fn drop(&mut self) {
                let _ = self.0.send(());
            }
        }
        impl HttpConnector for Pending {
            fn call(&self, _: HttpRequest) -> HttpConnectorFuture {
                let (started, dropped) = (self.started.clone(), self.dropped.clone());
                HttpConnectorFuture::new(async move {
                    let _notice = Notice(dropped);
                    started.send(()).unwrap();
                    std::future::pending().await
                })
            }
        }
        let (started, start_rx) = std::sync::mpsc::channel();
        let (dropped, drop_rx) = std::sync::mpsc::channel();
        let connector = RuntimeConnector(SharedHttpConnector::new(Pending { started, dropped }));
        let request = connector.call(HttpRequest::new(SdkBody::empty()));
        start_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        drop(request);
        drop_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
    }
}
