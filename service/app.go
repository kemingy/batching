package main

import (
	"flag"
	"fmt"
	"github.com/kemingy/batching"
	"github.com/valyala/fasthttp"
	"log"
	"os"
	"os/signal"
	"time"
)

func main() {
	address := flag.String("address", "batching.socket", "socket file or host:port")
	protocol := flag.String("protocol", "unix", "unix or tcp")
	batchSize := flag.Int("batch", 32, "max batch size")
	capacity := flag.Int("capacity", 1024, "max jobs in the queue")
	latency := flag.Int("latency", 10, "max latency (millisecond)")
	timeout := flag.Int("timeout", 5000, "timeout for a job (millisecond)")
	host := flag.String("host", "localhost", "host address")
	port := flag.Int("port", 8080, "service port")
	flag.Parse()
	batch := batching.NewBatching(
		*address,
		*protocol,
		*batchSize,
		*capacity,
		time.Millisecond*time.Duration(*latency),
		time.Millisecond*time.Duration(*timeout),
	)

	s := &fasthttp.Server{
		Handler: batch.HandleHTTP,
	}

	go batch.Run()
	go func() {
		if err := s.ListenAndServe(fmt.Sprintf("%s:%d", *host, *port)); err != nil {
			log.Fatalf("error in ListenAndServe: %s", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit
	if err := batch.Stop(); err != nil {
		log.Fatalf("socket %s cannot be stopped", batch.Address)
	}
	if err := s.Shutdown(); err != nil {
		log.Fatalf("error in shutdown: %s", err)
	}
}
