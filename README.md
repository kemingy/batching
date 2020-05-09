# Dynamic Batching for Deep Learning Serving

[Ventu](https://github.com/kemingy/ventu) already implement this protocol, so it can be used as the worker for deep learning inference.

## Features

* dynamic batching with batch size and latency
* invalid request won't affects others in the same batch
* communicate with workers through Unix domain socket

## Configs

```shell script
go run service/app.go --help
```

```shell script
Usage of app:
  -batch int
        max batch size (default 32)
  -capacity int
        max jobs in the queue (default 1024)
  -latency int
        max latency (millisecond) (default 10)
  -name string
        socket name: '{name}.socket' (default "batching")
  -timeout int
        timeout for a job (millisecond) (default 5000)
```

## Demo

```shell script
go run service/app.go
python examples/app.py
python examples/client.py
```
