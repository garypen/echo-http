use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::SinkExt as _;
use futures_util::StreamExt as _;
use http_body_util::BodyExt;
use http_body_util::Full;
use http_body_util::StreamBody;
use http_body_util::combinators::BoxBody;
use hyper::Method;
use hyper::Request;
use hyper::Response;
use hyper::StatusCode;
use hyper::body::Body;
use hyper::body::Bytes;
use hyper::body::Frame;
use hyper::body::Incoming;
use hyper_tungstenite::HyperWebsocket;
use hyper_tungstenite::tungstenite::Message;
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio_stream::wrappers::IntervalStream;

/// This is our service handler. It receives a Request, routes on its
/// path, and returns a Future of a Response.
async fn echo(
    mut req: Request<Incoming>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
    #[cfg(debug_assertions)]
    println!("req: {req:?}");
    match (req.method(), req.uri().path()) {
        // Serve some instructions at /
        (&Method::GET, "/") => {
            // tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            Ok(Response::new(
                Full::new(Bytes::from(
                    "Try GETting data to /echo such as: `curl localhost:1234/echo`\n\
                     Try streaming SSE from /sse such as: `curl -N localhost:1234/sse`\n\
                     Try a WebSocket at /ws such as: `websocat ws://localhost:1234/ws`",
                ))
                .map_err(|never| match never {})
                .boxed(),
            ))
        }

        // Serve some instructions at /
        (&Method::POST, "/") => {
            // tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            Ok(Response::new(Full::new(Bytes::from(
            "Try POSTing data to /echo such as: `curl localhost:1234/echo -XPOST -d 'hello world'`",
        )).map_err(|never| match never {})
        .boxed()))
        }

        // Simply echo the body back to the client.
        (&Method::GET, "/echo") => Ok(Response::new(req.into_body().boxed())),

        // Simply echo the body back to the client.
        (&Method::POST, "/echo") => Ok(Response::new(req.into_body().boxed())),

        // Convert to uppercase before sending back to client using a stream.
        (&Method::POST, "/echo/uppercase") => {
            // Map this body's frame to a different type
            let frame_stream = req.into_body().map_frame(|frame| {
                let frame = if let Ok(data) = frame.into_data() {
                    // Convert every byte in every Data frame to uppercase
                    data.iter()
                        .map(|byte| byte.to_ascii_uppercase())
                        .collect::<Bytes>()
                } else {
                    Bytes::new()
                };

                Frame::data(frame)
            });

            Ok(Response::new(frame_stream.boxed()))
        }

        // Stream Server-Sent Events indefinitely, one per second, so a caller
        // can exercise SSE proxying. Each event's `data` is a small JSON
        // object carrying a sequence number, e.g. `{"seq":0}`.
        (&Method::GET, "/sse") => {
            let ticks = IntervalStream::new(tokio::time::interval(Duration::from_secs(1)));
            let mut seq: u64 = 0;
            let events = ticks.map(move |_| {
                let event = format!("id: {seq}\nevent: message\ndata: {{\"seq\":{seq}}}\n\n");
                seq += 1;
                Ok::<_, Infallible>(Frame::data(Bytes::from(event)))
            });

            let mut resp = Response::new(
                StreamBody::new(events)
                    .map_err(|never| match never {})
                    .boxed(),
            );
            resp.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                "text/event-stream".parse().expect("valid header value"),
            );
            resp.headers_mut().insert(
                hyper::header::CACHE_CONTROL,
                "no-cache".parse().expect("valid header value"),
            );
            Ok(resp)
        }

        // Upgrade to a WebSocket connection. Echoes back whatever the client
        // sends, and separately pushes a server-initiated event (like /sse's)
        // once a second so a caller can exercise both directions.
        (&Method::GET, "/ws") => {
            if !hyper_tungstenite::is_upgrade_request(&req) {
                let mut resp = Response::new(
                    Full::new(Bytes::from("expected a websocket upgrade request"))
                        .map_err(|never| match never {})
                        .boxed(),
                );
                *resp.status_mut() = StatusCode::BAD_REQUEST;
                return Ok(resp);
            }

            let (response, websocket) = match hyper_tungstenite::upgrade(&mut req, None) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("websocket upgrade error: {e}");
                    let mut resp = Response::new(
                        Full::new(Bytes::from("bad websocket upgrade request"))
                            .map_err(|never| match never {})
                            .boxed(),
                    );
                    *resp.status_mut() = StatusCode::BAD_REQUEST;
                    return Ok(resp);
                }
            };

            tokio::spawn(async move {
                if let Err(e) = serve_websocket(websocket).await {
                    eprintln!("websocket error: {e}");
                }
            });

            Ok(response.map(|body| body.map_err(|never| match never {}).boxed()))
        }

        // Reverse the entire body before sending back to the client.
        //
        // Since we don't know the end yet, we can't simply stream
        // the chunks as they arrive as we did with the above uppercase endpoint.
        // So here we do `.await` on the future, waiting on concatenating the full body,
        // then afterwards the content can be reversed. Only then can we return a `Response`.
        (&Method::POST, "/echo/reversed") => {
            // Protect our server from massive bodies.
            let upper = req.body().size_hint().upper().unwrap_or(u64::MAX);
            if upper > 1024 * 64 {
                let mut resp = Response::new(
                    Full::new("Body too big".into())
                        .map_err(|never| match never {})
                        .boxed(),
                );
                *resp.status_mut() = hyper::StatusCode::PAYLOAD_TOO_LARGE;
                return Ok(resp);
            }

            // Await the whole body to be collected into a single `Bytes`...
            let whole_body = req.collect().await?.to_bytes();

            // Iterate the whole body in reverse order and collect into a new Vec.
            let reversed_body = whole_body.iter().rev().cloned().collect::<Vec<u8>>();

            Ok(Response::new(
                Full::new(reversed_body.into())
                    .map_err(|never| match never {})
                    .boxed(),
            ))
        }
        // Return the 404 Not Found for other routes.
        _ => {
            let mut not_found = Response::default();
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            Ok(not_found)
        }
    }
}

/// Drive one accepted WebSocket connection: echo back whatever the client
/// sends, and separately push a server-initiated event once a second, so a
/// caller can exercise both the client-to-server and server-to-client
/// directions.
async fn serve_websocket(
    websocket: HyperWebsocket,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut websocket = websocket.await?;
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    let mut seq: u64 = 0;

    loop {
        tokio::select! {
            message = websocket.next() => {
                match message {
                    Some(Ok(Message::Text(text))) => websocket.send(Message::Text(text)).await?,
                    Some(Ok(Message::Binary(data))) => websocket.send(Message::Binary(data)).await?,
                    Some(Ok(Message::Ping(_) | Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Frame(_))) => unreachable!("only received on the read side"),
                    Some(Err(e)) => return Err(e.into()),
                }
            }
            _ = ticker.tick() => {
                // A pseudo-random field, so the pushed event is visibly
                // different each time rather than just an incrementing count.
                let random = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.subsec_nanos())
                    .unwrap_or(0);
                let event = format!("{{\"seq\":{seq},\"random\":{random}}}");
                seq += 1;
                websocket.send(Message::text(event)).await?;
            }
        }
    }

    Ok(())
}

fn load_tls_acceptor(
    cert: &str,
    key: &str,
) -> Result<tokio_rustls::TlsAcceptor, Box<dyn std::error::Error + Send + Sync>> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .ok();

    let cert_file = std::fs::File::open(cert)?;
    let certs: Vec<_> =
        rustls_pemfile::certs(&mut std::io::BufReader::new(cert_file)).collect::<Result<_, _>>()?;

    let key_file = std::fs::File::open(key)?;
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(key_file))?
        .ok_or("no private key found")?;

    let mut server_cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    server_cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(server_cfg)))
}

async fn serve(listener: TcpListener, acceptor: Option<tokio_rustls::TlsAcceptor>) {
    loop {
        let (stream, _) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                eprintln!("accept error: {e}");
                continue;
            }
        };
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            let builder = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
            let svc = hyper::service::service_fn(echo);
            let result = match acceptor {
                Some(a) => match a.accept(stream).await {
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return,
                    Err(e) => {
                        eprintln!("TLS error: {e}");
                        return;
                    }
                    Ok(tls) => {
                        builder
                            .serve_connection_with_upgrades(TokioIo::new(tls), svc)
                            .await
                    }
                },
                None => {
                    builder
                        .serve_connection_with_upgrades(TokioIo::new(stream), svc)
                        .await
                }
            };
            if let Err(e) = result {
                let s = e.to_string();
                if !s.contains("close_notify") && !s.contains("Connection reset") {
                    eprintln!("server error: {e}");
                }
            }
        });
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let default_addr = "0.0.0.0:1234";
    let name = env!("CARGO_PKG_NAME");
    let args: Vec<String> = std::env::args().collect();

    let addr: SocketAddr = args
        .get(1)
        .cloned()
        .unwrap_or(default_addr.to_string())
        .parse()
        .unwrap_or_else(|_| panic!("usage: {name} [addr [cert key]]"));

    let acceptor = match (args.get(2), args.get(3)) {
        (Some(cert), Some(key)) => Some(load_tls_acceptor(cert, key)?),
        _ => None,
    };

    let listener = TcpListener::bind(addr).await?;
    match &acceptor {
        Some(_) => println!("Listening on https://{}", addr),
        None => println!("Listening on http://{}", addr),
    }

    serve(listener, acceptor).await;
    Ok(())
}
