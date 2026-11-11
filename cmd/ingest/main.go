// Command ingest accepts agent events over HTTP and publishes them to Kafka.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aarondcarr/agent-event-bus/internal/bus"
	"github.com/aarondcarr/agent-event-bus/internal/config"
	"github.com/aarondcarr/agent-event-bus/internal/ingest"
	"github.com/aarondcarr/agent-event-bus/internal/tracing"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("exit", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdown, err := tracing.Setup(ctx, "ingest")
	if err != nil {
		return err
	}
	defer shutdown(context.Background())

	prod, err := bus.NewKafkaProducer(config.Brokers())
	if err != nil {
		return err
	}
	defer prod.Close()

	srv := &http.Server{
		Addr:              config.String("HTTP_ADDR", ":8080"),
		Handler:           ingest.Handler(tracing.TracedProducer{Producer: prod}, config.EventsTopic()),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(sctx) // drain in-flight requests before the producer closes
	}()
	log.Info("ingest listening", "addr", srv.Addr)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
