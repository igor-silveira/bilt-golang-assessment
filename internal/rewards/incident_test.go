package rewards_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/rentpoints/pointsd/internal/ledger"
	"github.com/rentpoints/pointsd/internal/rewards"
)

// lostReplyStore applies each write and then reports ErrUnavailable once, like a
// Postgres commit whose reply never reached the replica.
type lostReplyStore struct {
	ledger.Store
	lost atomic.Bool
}

func (s *lostReplyStore) Award(ctx context.Context, e ledger.Entry, monthlyCap int64) (ledger.Entry, bool, error) {
	stored, created, err := s.Store.Award(ctx, e, monthlyCap)
	if err == nil && s.lost.CompareAndSwap(false, true) {
		return ledger.Entry{}, false, ledger.ErrUnavailable
	}
	return stored, created, err
}

func (s *lostReplyStore) Reverse(ctx context.Context, paymentID string) (ledger.Entry, bool, error) {
	stored, created, err := s.Store.Reverse(ctx, paymentID)
	if err == nil && s.lost.CompareAndSwap(false, true) {
		return ledger.Entry{}, false, ledger.ErrUnavailable
	}
	return stored, created, err
}

func TestHandle_LedgerUnavailableIsNacked(t *testing.T) {
	store := ledger.NewMemoryStore(ledger.WithFaults(func(string) error { return ledger.ErrUnavailable }))
	msg := newMessage(t, settled("pay_1", "mbr_1", 1000))

	newHandler(store).Handle(context.Background(), msg)

	if msg.outcome(t) != nacked {
		t.Fatal("event was acked while the ledger was unavailable; it would be lost")
	}
}

func TestHandle_RetryAfterLostCommitReplyAwardsOnce(t *testing.T) {
	mem := ledger.NewMemoryStore()
	h := newHandler(&lostReplyStore{Store: mem})

	deliver(t, h, settled("pay_1", "mbr_1", 1200))

	if got := balance(t, mem, "mbr_1"); got != 1200 {
		t.Fatalf("balance = %d, want 1200", got)
	}
}

func TestReversal_TransientLedgerErrorIsRetried(t *testing.T) {
	var failing atomic.Bool
	store := ledger.NewMemoryStore(ledger.WithFaults(func(op string) error {
		if op == "reverse" && failing.CompareAndSwap(true, false) {
			return ledger.ErrUnavailable
		}
		return nil
	}))
	h := newHandler(store)
	deliver(t, h, settled("pay_1", "mbr_1", 1500))
	failing.Store(true)

	deliver(t, h, reversed("pay_1", "mbr_1", 1500))

	if got := balance(t, store, "mbr_1"); got != 0 {
		t.Fatalf("balance = %d, want 0 (reversal lost on a transient ledger error)", got)
	}
}

func TestReversal_RetryAfterLostCommitReplyReversesOnce(t *testing.T) {
	mem := ledger.NewMemoryStore()
	deliver(t, newHandler(mem), settled("pay_1", "mbr_1", 2000))
	deliver(t, newHandler(mem), settled("pay_2", "mbr_1", 700))

	deliver(t, newHandler(&lostReplyStore{Store: mem}), reversed("pay_2", "mbr_1", 700))

	if got := balance(t, mem, "mbr_1"); got != 2000 {
		t.Fatalf("balance = %d, want 2000", got)
	}
}

func TestReversal_ConcurrentRedeliveryAcrossReplicasReversesOnce(t *testing.T) {
	store := ledger.NewMemoryStore(ledger.WithLatency(dbLatency))
	replicas := []*rewards.Handler{newHandler(store), newHandler(store)}
	deliver(t, replicas[0], settled("pay_1", "mbr_1", 3000))
	deliver(t, replicas[0], settled("pay_2", "mbr_1", 1000))

	rev := reversed("pay_2", "mbr_1", 1000)
	var fns []func()
	for i := 0; i < 8; i++ {
		h := replicas[i%2]
		fns = append(fns, func() { deliver(t, h, rev) })
	}
	concurrently(fns...)

	if got := balance(t, store, "mbr_1"); got != 3000 {
		t.Fatalf("balance = %d, want 3000 (payment reversed more than once)", got)
	}
}

func TestReversal_UsesAwardedPointsNotEventAmount(t *testing.T) {
	store := ledger.NewMemoryStore()
	h := newHandler(store)
	deliver(t, h, settled("pay_1", "mbr_1", 800))

	// A malformed or republished reversal carrying a larger amount must not
	// remove more than the payment earned.
	deliver(t, h, reversed("pay_1", "mbr_1", 5000))

	if got := balance(t, store, "mbr_1"); got != 0 {
		t.Fatalf("balance = %d, want 0", got)
	}
}

func TestAward_RepublishedWithDifferentAmountIsAwardedOnce(t *testing.T) {
	store := ledger.NewMemoryStore()
	h := newHandler(store)
	first := settled("pay_1", "mbr_1", 1800)
	again := settled("pay_1", "mbr_1", 2500)
	again.EventID = "evt_republished"

	deliver(t, h, first)
	deliver(t, h, again)

	if got := balance(t, store, "mbr_1"); got != 1800 {
		t.Fatalf("balance = %d, want 1800", got)
	}
}

func TestAward_CapAndDedupeHoldUnderRedeliveryAndFaults(t *testing.T) {
	var calls atomic.Int64
	var faulty atomic.Bool
	faulty.Store(true)
	store := ledger.NewMemoryStore(ledger.WithLatency(dbLatency), ledger.WithFaults(func(string) error {
		if faulty.Load() && calls.Add(1)%3 == 0 {
			return ledger.ErrUnavailable
		}
		return nil
	}))
	replicas := []*rewards.Handler{newHandler(store), newHandler(store), newHandler(store)}

	var fns []func()
	for i := 0; i < 30; i++ {
		// 15 payments of 1,000, each delivered to two different replicas.
		h, ev := replicas[i%3], settled(fmt.Sprintf("pay_%d", i%15), "mbr_1", 1000)
		fns = append(fns, func() { deliver(t, h, ev) })
	}
	concurrently(fns...)
	faulty.Store(false)

	if got := balance(t, store, "mbr_1"); got != rewards.DefaultPolicy.MonthlyCap {
		t.Fatalf("balance = %d, want %d", got, rewards.DefaultPolicy.MonthlyCap)
	}
}
