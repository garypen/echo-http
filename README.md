# Rust High Performance Echo Server 

A high performance echo server that echoes back an HTTP response with the content received over the TCP connection. 


## Usage
To install dependencies
```
$ cargo install --path .
```

To check dependencies
```
$ cargo check
```

To build project
```
$ cargo build
```

To start the server run:
```
$ cargo run 
```

Simple CURL request
```
$ curl http://localhost:3000/
```

Post CURL request for echo 
```
$ curl --location --request POST 'localhost:3000/echo' \
  --header 'Content-Type: application/json' \
  --data-raw '{
  	"name": "Gary Pennington",
  	"email": "garypen@gmail.com"
  }'
```

## Endpoints

| Method | Path | Behaviour |
|---|---|---|
| GET/POST | `/` | Usage hints |
| GET/POST | `/echo` | Echo the request body back |
| POST | `/echo/uppercase` | Echo the body upper-cased, streamed as it arrives |
| POST | `/echo/reversed` | Echo the body reversed (bodies above 64 KiB are rejected) |
| GET | `/sse` | Server-Sent Events, `{"seq":N}` per event |
| POST | `/v1/chat/completions` | OpenAI-shaped `chat.completion.chunk` SSE stream |
| GET | `/ws` | WebSocket: echoes client messages and pushes a server event |

## Streaming endpoints

`/sse`, `/v1/chat/completions` and the server-push side of `/ws` share these
per-request query parameters:

| Parameter | Default | Meaning |
|---|---|---|
| `ttft_ms` | `0` | Delay before the first event (models prefill / time-to-first-token) |
| `interval_ms` | `1000` | Gap between events; `chunk_interval_ms` is accepted as an alias. Floored at 1ms |
| `chunks` | unbounded | Number of events to emit; `/v1/chat/completions` then ends with `data: [DONE]` |

The defaults reproduce the original `/sse` and `/ws` behaviour exactly, so
existing callers are unaffected. `/v1/chat/completions` reads `model` from the
JSON request body, so a gateway that routes on the body can be exercised.

```
# Three SSE events: first after 500ms, then every 100ms
$ curl -N 'localhost:1234/sse?ttft_ms=500&interval_ms=100&chunks=3'

# OpenAI-shaped stream, model taken from the body
$ curl -N localhost:1234/v1/chat/completions \
  -d '{"model":"gpt-4o"}'

# Two pushed WebSocket events (200ms, then every 100ms); echo keeps working
$ websocat 'ws://localhost:1234/ws?ttft_ms=200&interval_ms=100&chunks=2'
```

