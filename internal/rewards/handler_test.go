package rewards_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rentpoints/pointsd/internal/ledger"
	"github.com/rentpoints/pointsd/internal/rewards"
)

const dbLatency = 2 * time.Millisecond

// concurrently runs every fn at the same time and waits for all of them.
func concurrently(fns ...func()) {
	var start, done sync.WaitGroup
	start.Add(1)
	for _, fn := range fns {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			fn()
		}()
	}
	start.Done()
	done.Wait()
}

func TestAward_OnePointPerDollar(t *testing.T) {
	store := ledger.NewMemoryStore()
	deliver(t, newHandler(store), settled("pay_1", "mbr_1", 2450))
	if got := balance(t, store, "mbr_1"); got != 2450 {
		t.Fatalf("balance = %d, want 2450", got)
	}
}

func TestAward_MonthlyCapResetsEachCalendarMonthUTC(t *testing.T) {
	store := ledger.NewMemoryStore()
	h := newHandler(store)

	sep := settled("pay_1", "mbr_1", 9000)
	sep.OccurredAt = time.Date(2026, 9, 30, 23, 30, 0, 0, time.UTC)
	octFirst := settled("pay_2", "mbr_1", 9000)
	octFirst.OccurredAt = time.Date(2026, 10, 1, 0, 30, 0, 0, time.UTC)
	octSecond := settled("pay_3", "mbr_1", 3000)

	deliver(t, h, sep)       // 9,000 in September
	deliver(t, h, octFirst)  // 9,000 in October
	deliver(t, h, octSecond) // capped: only 1,000 left in October

	if got := balance(t, store, "mbr_1"); got != 19_000 {
		t.Fatalf("balance = %d, want 19000", got)
	}
}

func TestAward_RepublishedPaymentIsAwardedOnce(t *testing.T) {
	store := ledger.NewMemoryStore()
	h := newHandler(store)

	first := settled("pay_1", "mbr_1", 1800)
	again := first
	again.EventID = "evt_republished"
	deliver(t, h, first)
	deliver(t, h, again)

	if got := balance(t, store, "mbr_1"); got != 1800 {
		t.Fatalf("balance = %d, want 1800", got)
	}
}

func TestAward_ConcurrentRedeliveryAcrossReplicasIsAwardedOnce(t *testing.T) {
	store := ledger.NewMemoryStore(ledger.WithLatency(dbLatency))
	replicas := []*rewards.Handler{newHandler(store), newHandler(store)}
	ev := settled("pay_1", "mbr_1", 2000)

	var fns []func()
	for i := 0; i < 8; i++ {
		h := replicas[i%2]
		fns = append(fns, func() { deliver(t, h, ev) })
	}
	concurrently(fns...)

	if got := balance(t, store, "mbr_1"); got != 2000 {
		t.Fatalf("balance = %d, want 2000 (payment awarded more than once)", got)
	}
}

func TestAward_ConcurrentPaymentsRespectMonthlyCap(t *testing.T) {
	store := ledger.NewMemoryStore(ledger.WithLatency(dbLatency))
	replicas := []*rewards.Handler{newHandler(store), newHandler(store)}

	var fns []func()
	for i := 0; i < 15; i++ {
		h, ev := replicas[i%2], settled(fmt.Sprintf("pay_%d", i), "mbr_1", 1000)
		fns = append(fns, func() { deliver(t, h, ev) })
	}
	concurrently(fns...)

	if got := balance(t, store, "mbr_1"); got != rewards.DefaultPolicy.MonthlyCap {
		t.Fatalf("balance = %d, want %d (monthly cap)", got, rewards.DefaultPolicy.MonthlyCap)
	}
}

func TestReversal_RemovesAwardedPoints(t *testing.T) {
	store := ledger.NewMemoryStore()
	h := newHandler(store)

	deliver(t, h, settled("pay_1", "mbr_1", 2000))
	deliver(t, h, settled("pay_2", "mbr_1", 1500))
	deliver(t, h, reversed("pay_2", "mbr_1", 1500))

	if got := balance(t, store, "mbr_1"); got != 2000 {
		t.Fatalf("balance = %d, want 2000", got)
	}
}

func TestReversal_ClawsBackOnlyWhatWasAwarded(t *testing.T) {
	store := ledger.NewMemoryStore()
	h := newHandler(store)

	deliver(t, h, settled("pay_1", "mbr_1", 9500))
	deliver(t, h, settled("pay_2", "mbr_1", 1000)) // capped: 500 awarded
	deliver(t, h, reversed("pay_2", "mbr_1", 1000))

	if got := balance(t, store, "mbr_1"); got != 9500 {
		t.Fatalf("balance = %d, want 9500 (reversal must remove only the 500 points awarded)", got)
	}
}

func TestHandle_TransientLedgerErrorIsRetried(t *testing.T) {
	var failures atomic.Int32
	failures.Store(2)
	store := ledger.NewMemoryStore(ledger.WithFaults(func(string) error {
		if failures.Add(-1) >= 0 {
			return ledger.ErrUnavailable
		}
		return nil
	}))

	deliver(t, newHandler(store), settled("pay_1", "mbr_1", 1200))

	if got := balance(t, store, "mbr_1"); got != 1200 {
		t.Fatalf("balance = %d, want 1200 (event lost on a transient ledger error)", got)
	}
}

func TestHandle_InvalidEventIsDeadLetteredAndAcked(t *testing.T) {
	dlq := &fakeDLQ{}
	h := rewards.NewHandler(ledger.NewMemoryStore(), rewards.DefaultPolicy, dlq, newHandlerLogger())
	msg := &fakeMessage{id: "m1", data: []byte(`{"type":"payment.settled","payment_id":"pay_1"}`), attempt: 1}

	h.Handle(context.Background(), msg)

	if msg.outcome(t) != acked {
		t.Fatal("invalid event was not acked; it would be redelivered forever")
	}
	if len(dlq.messages) != 1 {
		t.Fatalf("dead-lettered %d messages, want 1", len(dlq.messages))
	}
}

func TestHandle_ReversalForUnknownPaymentIsAcked(t *testing.T) {
	store := ledger.NewMemoryStore()
	msg := newMessage(t, reversed("pay_never_settled", "mbr_1", 1000))

	newHandler(store).Handle(context.Background(), msg)

	if msg.outcome(t) != acked {
		t.Fatal("reversal of an unknown payment was nacked; retrying cannot succeed")
	}
	if got := balance(t, store, "mbr_1"); got != 0 {
		t.Fatalf("balance = %d, want 0", got)
	}
}

func TestHandle_DistinctMembersAreProcessedConcurrently(t *testing.T) {
	const members = 40
	store := ledger.NewMemoryStore(ledger.WithLatency(10 * time.Millisecond))
	h := newHandler(store)

	var fns []func()
	for i := 0; i < members; i++ {
		ev := settled(fmt.Sprintf("pay_%d", i), fmt.Sprintf("mbr_%d", i), 100)
		fns = append(fns, func() { deliver(t, h, ev) })
	}
	began := time.Now()
	concurrently(fns...)
	elapsed := time.Since(began)

	// Serialized processing would take members x (ledger calls) x 10ms.
	if elapsed > 300*time.Millisecond {
		t.Fatalf("processing %d members took %v; events for different members must not wait on each other", members, elapsed)
	}
	for i := 0; i < members; i++ {
		if got := balance(t, store, fmt.Sprintf("mbr_%d", i)); got != 100 {
			t.Fatalf("mbr_%d balance = %d, want 100", i, got)
		}
	}
}
