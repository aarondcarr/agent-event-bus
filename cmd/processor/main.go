// Command processor consumes agent events, runs the kill switch and the
// metrics aggregator, and serves their state over HTTP.
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

	"golang.org/x/sync/errgroup"

	"github.com/aarondcarr/agent-event-bus/internal/bus"
	"github.com/aarondcarr/agent-event-bus/internal/config"
	"github.com/aarondcarr/agent-event-bus/internal/metrics"
	"github.com/aarondcarr/agent-event-bus/internal/policy"
	"github.com/aarondcarr/agent-event-bus/internal/processor"
	"github.com/aarondcarr/agent-event-bus/internal/tracing"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("exit", "err", err)
		os.Exit(1) // the orchestrator restarts us; uncommitted records are redelivered
	}
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdown, err := tracing.Setup(ctx, "processor")
	if err != nil {
		return err
	}
	defer shutdown(context.Background())

	brokers := config.Brokers()
	cons, err := bus.NewKafkaConsumer(brokers, config.String("CONSUMER_GROUP", "agent-processor"), config.EventsTopic())
	if err != nil {
		return err
	}
	defer cons.Close()
	prod, err := bus.NewKafkaProducer(brokers)
	if err != nil {
		return err
	}
	defer prod.Close()

	ev := policy.New(policy.Config{
		Window:          config.Duration("POLICY_WINDOW", time.Minute),
		MaxErrors:       config.Int("POLICY_MAX_ERRORS", 5),
		MaxGuardDenials: config.Int("POLICY_MAX_GUARD_DENIALS", 3),
		MaxTokens:       int64(config.Int("POLICY_MAX_TOKENS", 200_000)),
	})
	agg := metrics.New()
	p := &processor.Processor{
		Consumer: cons,
		Handlers: []processor.Handler{
			processor.PolicyHandler(ev, tracing.TracedProducer{Producer: prod}, config.ControlTopic()),
			processor.MetricsHandler(agg),
		},
		Log: log,
	}
	srv := &http.Server{Addr: config.String("HTTP_ADDR", ":8081"), Handler: processor.API(ev, agg), ReadHeaderTimeout: 5 * time.Second}

	// Two long-lived goroutines; if either fails, cancel the other.
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return p.Run(gctx) })
	g.Go(func() error {
		log.Info("processor API listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(sctx)
	})
	return g.Wait()
}
