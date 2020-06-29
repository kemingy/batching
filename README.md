# Dynamic Batching for Deep Learning Serving

[![Language](https://img.shields.io/badge/Language-Go-blue.svg)](https://golang.org/)
[![Go Report Card](https://goreportcard.com/badge/github.com/kemingy/batching)](https://goreportcard.com/report/github.com/kemingy/batching)
[![GoDoc](https://img.shields.io/badge/Godoc-reference-blue.svg)](https://godoc.org/github.com/kemingy/batching)
![GitHub Actions](https://github.com/kemingy/batching/workflows/Go/badge.svg)
[![LICENSE](https://img.shields.io/github/license/kemingy/batching.svg)](https://github.com/kemingy/batching/blob/master/LICENSE)

[Ventu](https://github.com/kemingy/ventu) already implement this protocol, so it can be used as the worker for deep learning inference.

## Features

* dynamic batching with batch size and latency
* invalid request won't affects others in the same batch
* communicate with workers through Unix domain socket or TCP
* load balancing

If you are interested in the design, check my blog [Deep Learning Serving Framework](https://kemingy.github.io/blogs/deep-learning-serving/).

## Configs

```shell script
go run service/app.go --help
```

```
Usage app:
  -address string
        socket file or host:port (default "batching.socket")
  -batch int
        max batch size (default 32)
  -capacity int
        max jobs in the queue (default 1024)
  -host string
        host address (default "localhost")
  -latency int
        max latency (millisecond) (default 10)
  -port int
        service port (default 8080)
  -protocol string
        unix or tcp (default "unix")
  -timeout int
        timeout for a job (millisecond) (default 5000)
```

## Demo

```shell script
go run service/app.go
python examples/app.py
python examples/client.py
```
