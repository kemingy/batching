package main

import (
	"fmt"
	"github.com/valyala/fasthttp"
	"log"
	"os"
	"os/signal"
)

type Batching struct {
	maxLatency float32
	batchSize  uint
	capacity   uint64
}

func (b *Batching) HandleHTTP(ctx *fasthttp.RequestCtx) {
	fmt.Fprintf(ctx, "Hello, world! Requested path is %q. Capacity is %v",
		ctx.Path(), b.capacity)
}

func main() {
	batching := Batching{
		maxLatency: 0.01,
		batchSize:  32,
		capacity:   1024,
	}

	s := &fasthttp.Server{
		Handler: batching.HandleHTTP,
	}

	go func() {
		if err := s.ListenAndServe("localhost:8080"); err != nil {
			log.Fatalf("error in ListenAndServe: %s", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit
	if err := s.Shutdown(); err != nil {
		log.Fatalf("error in shutdown: %s", err)
	}
}
