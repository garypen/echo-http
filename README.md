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

