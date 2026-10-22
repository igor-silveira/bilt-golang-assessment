// Command pointsd consumes payment events and serves member points balances.
//
// Local mode replays newline-delimited events from a file instead of a
// Pub/Sub subscription:
//
//	go run ./cmd/pointsd -events testdata/events.jsonl
package main

import (
	"bufio"
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"strconv"

	"github.com/rentpoints/pointsd/internal/api"
	"github.com/rentpoints/pointsd/internal/ledger"
	"github.com/rentpoints/pointsd/internal/rewards"
)

type fileMessage struct {
	id   string
	data []byte
}

func (m fileMessage) ID() string           { return m.id }
func (m fileMessage) Data() []byte         { return m.data }
func (m fileMessage) DeliveryAttempt() int { return 1 }
func (m fileMessage) Ack()                 {}
func (m fileMessage) Nack()                {}

type logPublisher struct{ log *slog.Logger }

func (p logPublisher) Publish(_ context.Context, data []byte, attrs map[string]string) error {
	p.log.Warn("dead letter", "reason", attrs["reason"], "data", string(data))
	return nil
}

func main() {
	eventsPath := flag.String("events", "", "replay events from this JSONL file")
	addr := flag.String("addr", ":8080", "HTTP listen address")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	store := ledger.NewMemoryStore()
	handler := rewards.NewHandler(store, rewards.DefaultPolicy, logPublisher{log}, log)

	if *eventsPath != "" {
		f, err := os.Open(*eventsPath)
		if err != nil {
			log.Error("open events", "err", err)
			os.Exit(1)
		}
		sc := bufio.NewScanner(f)
		for n := 1; sc.Scan(); n++ {
			handler.Handle(context.Background(), fileMessage{id: strconv.Itoa(n), data: append([]byte(nil), sc.Bytes()...)})
		}
		f.Close()
	}

	log.Info("listening", "addr", *addr)
	if err := http.ListenAndServe(*addr, api.NewServer(store)); err != nil {
		log.Error("server stopped", "err", err)
		os.Exit(1)
	}
}
