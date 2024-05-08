#![feature(duration_constructors)]

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Duration;

use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, Response, Uri};
use hyper::body::{Bytes, Incoming};
use hyper::service::Service;
use moka::future::Cache;
use tokio::net::TcpStream;
use tracing::{error, info};

use cacheproxy::support::TokioIo;

#[tokio::main]
pub async fn main() {
    tracing_subscriber::fmt::init();

    let listen_addr: SocketAddr = ([127, 0, 0, 1], 5544).into();
    let proxy_addr: SocketAddr = ([127, 0, 0, 1], 8000).into();

    // Server
    let tcp = tokio::net::TcpListener::bind(listen_addr).await.unwrap();

    info!("listening on {:?}", listen_addr);
    info!("proxying to {:?}", proxy_addr);

    let handler = CachingProxy::new(proxy_addr);

    loop {
        let (stream, _) = tcp.accept().await.unwrap();

        let io = TokioIo::new(stream);
        hyper::server::conn::http1::Builder::new()
            .serve_connection(io, handler.clone())
            .await
            .unwrap();
    }
}

#[derive(Clone)]
pub struct CachingProxy {
    cache: Cache<Uri, Response<Full<Bytes>>>,
    upstream: SocketAddr,
}

impl CachingProxy {
    pub fn new(upstream: SocketAddr) -> Self {
        let cache: Cache<Uri, Response<Full<Bytes>>> = Cache::builder()
            .time_to_live(Duration::from_secs(10))
            .eviction_listener(|k, _, _| {
                info!("cache item for {:?} expired", k);
            })
            .build();

        Self { cache, upstream }
    }
}

impl Service<Request<Incoming>> for CachingProxy {
    type Response = Response<Full<Bytes>>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output=Result<Self::Response, Self::Error>>>>;

    fn call(&self, request: Request<Incoming>) -> Self::Future {
        let upstream = self.upstream;
        let cache = self.cache.clone();

        Box::pin(async move {
            info!("Processing request for {:?}", request.uri());

            // Only GET methods are eligible for caching.
            if request.method() == Method::GET {
                // Attempt to fill from cache
                if let Some(response) = cache.get(request.uri()).await {
                    info!("cache hit");
                    return Ok::<Response<Full<Bytes>>, hyper::Error>(response);
                }
            }

            info!("cache miss");

            let upstream_conn = TcpStream::connect(upstream).await.unwrap();
            let io = TokioIo::new(upstream_conn);

            let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;
            tokio::spawn(async move {
                if let Err(err) = conn.await {
                    error!("Connection failure: {:?}", err);
                }
            });

            let request_uri = request.uri().clone();

            let response = sender.send_request(request).await?;
            let (parts, body) = response.into_parts();
            let body = Full::new(body.collect().await?.to_bytes());

            let response = Response::from_parts(parts, body);
            cache.insert(request_uri, response.clone()).await;

            Ok::<Response<Full<Bytes>>, hyper::Error>(response)
        })
    }
}