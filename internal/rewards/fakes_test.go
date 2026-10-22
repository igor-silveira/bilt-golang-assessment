package rewards_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/rentpoints/pointsd/internal/ledger"
	"github.com/rentpoints/pointsd/internal/rewards"
)

type outcome int

const (
	pending outcome = iota
	acked
	nacked
)

type fakeMessage struct {
	id      string
	data    []byte
	attempt int

	mu      sync.Mutex
	result  outcome
	settles int
}

func (m *fakeMessage) ID() string           { return m.id }
func (m *fakeMessage) Data() []byte         { return m.data }
func (m *fakeMessage) DeliveryAttempt() int { return m.attempt }
func (m *fakeMessage) Ack()                 { m.settle(acked) }
func (m *fakeMessage) Nack()                { m.settle(nacked) }

func (m *fakeMessage) settle(o outcome) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.result = o
	m.settles++
}

func (m *fakeMessage) outcome(t *testing.T) outcome {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.settles != 1 {
		t.Fatalf("message %s settled %d times, want exactly once", m.id, m.settles)
	}
	return m.result
}

type fakeDLQ struct {
	mu       sync.Mutex
	messages [][]byte
}

func (d *fakeDLQ) Publish(_ context.Context, data []byte, _ map[string]string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.messages = append(d.messages, data)
	return nil
}

type event struct {
	Type        string    `json:"type"`
	EventID     string    `json:"event_id"`
	PaymentID   string    `json:"payment_id"`
	MemberID    string    `json:"member_id"`
	AmountCents int64     `json:"amount_cents"`
	OccurredAt  time.Time `json:"occurred_at"`
}

var oct = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

func settled(paymentID, memberID string, dollars int64) event {
	return event{"payment.settled", "evt_" + paymentID, paymentID, memberID, dollars * 100, oct}
}

func reversed(paymentID, memberID string, dollars int64) event {
	return event{"payment.reversed", "evt_rev_" + paymentID, paymentID, memberID, dollars * 100, oct.Add(72 * time.Hour)}
}

func newMessage(t *testing.T, ev event) *fakeMessage {
	t.Helper()
	data, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	return &fakeMessage{id: ev.EventID, data: data, attempt: 1}
}

func newHandler(store ledger.Store) *rewards.Handler {
	return rewards.NewHandler(store, rewards.DefaultPolicy, &fakeDLQ{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// deliver hands ev to h, redelivering while it is nacked, like the broker would.
func deliver(t *testing.T, h *rewards.Handler, ev event) *fakeMessage {
	t.Helper()
	for attempt := 1; attempt <= 10; attempt++ {
		msg := newMessage(t, ev)
		msg.attempt = attempt
		h.Handle(context.Background(), msg)
		if msg.outcome(t) == acked {
			return msg
		}
	}
	t.Fatalf("event %s never acked after 10 deliveries", ev.EventID)
	return nil
}

func balance(t *testing.T, s ledger.Store, member string) int64 {
	t.Helper()
	b, err := s.Balance(context.Background(), member)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func newHandlerLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
