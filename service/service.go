package batching

import (
	"github.com/valyala/fasthttp"
	"log"
	"os"
	"os/signal"
	"time"
)

func main() {
	batching := NewBatching("batching", 32, 1024, 10*time.Millisecond, 5*time.Second)

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
	if err := batching.Stop(); err != nil {
		log.Fatalf("socket %s cannot be stopped", batching.name)
	}
	if err := s.Shutdown(); err != nil {
		log.Fatalf("error in shutdown: %s", err)
	}
}
