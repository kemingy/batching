# Dynamic Batching for Deep Learning Serving

[Ventu](https://github.com/kemingy/ventu) already implement this protocol, so it can be used as the worker for deep learning inference.

## Features

* dynamic batching with batch size and latency
* invalid request won't affects others in the same batch
* communicate with workers through Unix domain socket

## Demo

```shell script
go run service/app.go
python examples/app.py
python examples/client.py
```
