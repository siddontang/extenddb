// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

//! PD service safepoint leases for bulk snapshot readers. Never advances GC.
//!
//! Registration uses timestamp-1, matching TiDB BR's global GC manager. Every
//! page checks a monotonic local deadline; renewal failure aborts the snapshot.
//! A lost process leaves only a five-minute lease. A conforming external GC
//! controller must honor PD service safepoints, as TiDB's GC worker does.
mod protocol;
use super::Error;
use futures::future::BoxFuture;
use protocol::*;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tonic::{client::Grpc, transport::Channel};
const TTL: i64 = 300;
fn err(e: impl std::fmt::Display) -> Error {
    Error::Transport(format!("PD GC protection: {e}"))
}
/// Injectable barrier transport; tests can expire or reject leases without PD.
pub trait Barriers: Send + Sync {
    fn safe_point(&self) -> BoxFuture<'_, Result<u64, Error>>;
    fn pin(&self, service: String, timestamp: u64, ttl: i64) -> BoxFuture<'_, Result<u64, Error>>;
}
pub struct Pd {
    endpoints: Vec<String>,
    security: tikv_client::SecurityManager,
    timeout: Duration,
    cluster: u64,
}
impl Pd {
    pub async fn connect(
        endpoints: Vec<String>,
        config: &tikv_client::Config,
    ) -> Result<Arc<Self>, Error> {
        let security = match (&config.ca_path, &config.cert_path, &config.key_path) {
            (Some(ca), Some(cert), Some(key)) => {
                tikv_client::SecurityManager::load(ca, cert, key.clone()).map_err(err)?
            }
            _ => Default::default(),
        };
        let mut this = Self {
            endpoints,
            security,
            timeout: config.timeout,
            cluster: 0,
        };
        let (_, id) = this.leader().await?;
        this.cluster = id;
        Ok(Arc::new(this))
    }
    async fn channel(&self, url: &str) -> Result<Channel, Error> {
        tokio::time::timeout(self.timeout, self.security.connect(url, |c| c))
            .await
            .map_err(err)?
            .map_err(err)
    }
    async fn rpc<I: prost::Message + Default + 'static, O: prost::Message + Default + 'static>(
        &self,
        ch: Channel,
        path: &'static str,
        body: I,
    ) -> Result<O, Error> {
        let mut c = Grpc::new(ch);
        tokio::time::timeout(self.timeout, async {
            c.ready().await.map_err(err)?;
            c.unary(
                tonic::Request::new(body),
                tonic::codegen::http::uri::PathAndQuery::from_static(path),
                tonic::codec::ProstCodec::<I, O>::default(),
            )
            .await
            .map(|r| r.into_inner())
            .map_err(err)
        })
        .await
        .map_err(err)?
    }
    async fn leader(&self) -> Result<(Channel, u64), Error> {
        let mut failure = err("no reachable PD leader");
        for url in &self.endpoints {
            let attempt = async {
                let ch = self.channel(url).await?;
                let members: Members = self
                    .rpc(ch, "/pdpb.PD/GetMembers", Request { header: None })
                    .await?;
                let id = header(members.header, self.cluster)?;
                let leader = members
                    .leader
                    .and_then(|l| l.urls.into_iter().next())
                    .ok_or_else(|| err("missing leader"))?;
                Ok((self.channel(&leader).await?, id))
            }
            .await;
            match attempt {
                Ok(v) => return Ok(v),
                Err(e) => failure = e,
            }
        }
        Err(failure)
    }
}
fn header(h: Option<ResponseHeader>, expected: u64) -> Result<u64, Error> {
    let h = h.ok_or_else(|| err("missing response header"))?;
    if h.cluster_id == 0 || (expected != 0 && expected != h.cluster_id) {
        return Err(err("cluster identity mismatch"));
    }
    if let Some(e) = h.error
        && e.kind != 0
    {
        return Err(err(e.message));
    }
    Ok(h.cluster_id)
}
impl Barriers for Pd {
    fn safe_point(&self) -> BoxFuture<'_, Result<u64, Error>> {
        Box::pin(async move {
            let (ch, id) = self.leader().await?;
            let r: SafePoint = self
                .rpc(
                    ch,
                    "/pdpb.PD/GetGCSafePoint",
                    Request {
                        header: Some(Header { cluster_id: id }),
                    },
                )
                .await?;
            header(r.header, self.cluster)?;
            Ok(r.safe_point)
        })
    }
    fn pin(&self, service: String, timestamp: u64, ttl: i64) -> BoxFuture<'_, Result<u64, Error>> {
        Box::pin(async move {
            let (ch, id) = self.leader().await?;
            let r: Pinned = self
                .rpc(
                    ch,
                    "/pdpb.PD/UpdateServiceGCSafePoint",
                    Pin {
                        header: Some(Header { cluster_id: id }),
                        service: service.into_bytes(),
                        ttl,
                        safe_point: timestamp,
                    },
                )
                .await?;
            header(r.header, self.cluster)?;
            Ok(r.minimum)
        })
    }
}
pub struct Guard {
    api: Arc<dyn Barriers>,
    id: String,
    timestamp: u64,
    renewed: Instant,
}
impl Guard {
    pub async fn acquire(api: Arc<dyn Barriers>, timestamp: u64) -> Result<Self, Error> {
        if timestamp == 0 {
            return Err(err("zero snapshot timestamp"));
        }
        let mut g = Self {
            api,
            id: format!("extenddb-snapshot-{}", uuid::Uuid::new_v4()),
            timestamp,
            renewed: Instant::now(),
        };
        g.renew().await?;
        Ok(g)
    }
    async fn renew(&mut self) -> Result<(), Error> {
        let started = Instant::now();
        let floor = self.timestamp - 1;
        if self.api.pin(self.id.clone(), floor, TTL).await? > floor
            || self.api.safe_point().await? >= self.timestamp
        {
            return Err(err("snapshot timestamp is below the retained GC window"));
        }
        // Count connection/registration time against the lease conservatively.
        if started.elapsed() >= Duration::from_secs(120) {
            return Err(err("GC lease registration exceeded its deadline"));
        }
        self.renewed = started;
        Ok(())
    }
    pub async fn check(&mut self) -> Result<(), Error> {
        if self.renewed.elapsed() >= Duration::from_secs(240) {
            return Err(err("snapshot GC lease expired"));
        }
        if self.renewed.elapsed() >= Duration::from_secs(30) {
            self.renew().await?;
        }
        Ok(())
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        if let Ok(h) = tokio::runtime::Handle::try_current() {
            let api = self.api.clone();
            let id = self.id.clone();
            h.spawn(async move {
                let _ = api.pin(id, 0, 0).await;
            });
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    struct Fake {
        floor: u64,
        reject: bool,
    }
    impl Barriers for Fake {
        fn safe_point(&self) -> BoxFuture<'_, Result<u64, Error>> {
            Box::pin(async move { Ok(self.floor) })
        }
        fn pin(&self, _: String, ts: u64, _: i64) -> BoxFuture<'_, Result<u64, Error>> {
            Box::pin(async move {
                if self.reject {
                    Err(err("offline"))
                } else {
                    Ok(ts)
                }
            })
        }
    }
    #[tokio::test]
    async fn guard_fails_closed_on_gc_expiry_and_renewal_failure() {
        assert!(
            Guard::acquire(
                Arc::new(Fake {
                    floor: 10,
                    reject: false
                }),
                10
            )
            .await
            .is_err()
        );
        let mut g = Guard::acquire(
            Arc::new(Fake {
                floor: 0,
                reject: false,
            }),
            10,
        )
        .await
        .unwrap();
        g.renewed = Instant::now() - Duration::from_secs(240);
        assert!(g.check().await.is_err());
        g.renewed = Instant::now() - Duration::from_secs(31);
        g.api = Arc::new(Fake {
            floor: 0,
            reject: true,
        });
        assert!(g.check().await.is_err());
    }
    #[test]
    fn headers_require_correct_cluster_and_no_pd_error() {
        assert!(header(None, 1).is_err());
        assert!(
            header(
                Some(ResponseHeader {
                    cluster_id: 2,
                    error: None
                }),
                1
            )
            .is_err()
        );
        assert!(
            header(
                Some(ResponseHeader {
                    cluster_id: 1,
                    error: Some(PdError {
                        kind: 1,
                        message: "not leader".into()
                    })
                }),
                1
            )
            .is_err()
        );
    }
}
