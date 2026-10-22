// Package ledger stores points entries.
//
// In production the Store is backed by Postgres (table ledger_entries).
// MemoryStore is the stand-in used for local runs and tests.
package ledger

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Kind string

const (
	KindAward    Kind = "award"
	KindReversal Kind = "reversal"
)

// Entry is one line of a member's points ledger.
type Entry struct {
	MemberID  string
	PaymentID string
	Kind      Kind
	Points    int64  // positive for awards, negative for reversals
	Month     string // settlement month of the payment, "2006-01" in UTC
}

// ErrUnavailable is returned when the ledger cannot be reached. Retrying may succeed.
var ErrUnavailable = errors.New("ledger: unavailable")

// ErrDuplicate is returned by Append when the payment already has an entry of
// that kind. In Postgres this is the UNIQUE (payment_id, kind) constraint.
var ErrDuplicate = errors.New("ledger: duplicate entry")

// ErrNotAwarded is returned by Reverse when the payment has no award entry.
var ErrNotAwarded = errors.New("ledger: payment was never awarded")

// Store is the points ledger. Award and Reverse are each one atomic, idempotent
// operation, so they are safe under redelivery and across replicas sharing the ledger.
type Store interface {
	// Award writes the award entry for e.PaymentID, cutting e.Points to the room
	// left under monthlyCap for e.MemberID in e.Month. If the payment already has
	// an award, nothing is written and the stored entry is returned.
	Award(ctx context.Context, e Entry, monthlyCap int64) (stored Entry, created bool, err error)
	// Reverse writes a reversal that removes exactly the points on the payment's
	// award entry. If the payment is already reversed, nothing is written and the
	// stored reversal is returned. It returns ErrNotAwarded if there is no award.
	Reverse(ctx context.Context, paymentID string) (stored Entry, created bool, err error)
	Balance(ctx context.Context, memberID string) (int64, error)
}

type Option func(*MemoryStore)

// WithLatency delays every call, approximating a database round trip.
func WithLatency(d time.Duration) Option {
	return func(s *MemoryStore) { s.latency = d }
}

// WithFaults installs a hook run at the start of every call (after the
// round trip). A non-nil error is returned to the caller and the call has no effect.
func WithFaults(f func(op string) error) Option {
	return func(s *MemoryStore) { s.faults = f }
}

type MemoryStore struct {
	latency time.Duration
	faults  func(op string) error

	mu      sync.Mutex
	entries []Entry
	byKey   map[entryKey]int // index into entries
}

type entryKey struct {
	paymentID string
	kind      Kind
}

func NewMemoryStore(opts ...Option) *MemoryStore {
	s := &MemoryStore{byKey: make(map[entryKey]int)}
	for _, o := range opts {
		o(s)
	}
	return s
}

func (s *MemoryStore) roundTrip(ctx context.Context, op string) error {
	if s.latency > 0 {
		select {
		case <-time.After(s.latency):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.faults != nil {
		return s.faults(op)
	}
	return nil
}

// Award holds mu for the whole check-cap-insert, the stand-in for a Postgres
// transaction that locks the member's month (see NOTES.md).
func (s *MemoryStore) Award(ctx context.Context, e Entry, monthlyCap int64) (Entry, bool, error) {
	if err := s.roundTrip(ctx, "award"); err != nil {
		return Entry{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if i, ok := s.byKey[entryKey{e.PaymentID, KindAward}]; ok {
		return s.entries[i], false, nil
	}
	var used int64
	for _, x := range s.entries {
		if x.MemberID == e.MemberID && x.Month == e.Month && x.Kind == KindAward {
			used += x.Points
		}
	}
	e.Kind = KindAward
	e.Points = max(min(e.Points, monthlyCap-used), 0)
	s.insert(e)
	return e, true, nil
}

func (s *MemoryStore) Reverse(ctx context.Context, paymentID string) (Entry, bool, error) {
	if err := s.roundTrip(ctx, "reverse"); err != nil {
		return Entry{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if i, ok := s.byKey[entryKey{paymentID, KindReversal}]; ok {
		return s.entries[i], false, nil
	}
	i, ok := s.byKey[entryKey{paymentID, KindAward}]
	if !ok {
		return Entry{}, false, ErrNotAwarded
	}
	award := s.entries[i]
	e := Entry{
		MemberID:  award.MemberID,
		PaymentID: paymentID,
		Kind:      KindReversal,
		Points:    -award.Points,
		Month:     award.Month,
	}
	s.insert(e)
	return e, true, nil
}

// Append writes e as is. It enforces one entry per (payment, kind) like the
// production table; use Award and Reverse to apply the earning rules.
func (s *MemoryStore) Append(ctx context.Context, e Entry) error {
	if err := s.roundTrip(ctx, "append"); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.byKey[entryKey{e.PaymentID, e.Kind}]; ok {
		return ErrDuplicate
	}
	s.insert(e)
	return nil
}

// insert must be called with mu held.
func (s *MemoryStore) insert(e Entry) {
	s.byKey[entryKey{e.PaymentID, e.Kind}] = len(s.entries)
	s.entries = append(s.entries, e)
}

func (s *MemoryStore) Balance(ctx context.Context, memberID string) (int64, error) {
	if err := s.roundTrip(ctx, "balance"); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var total int64
	for _, e := range s.entries {
		if e.MemberID == memberID {
			total += e.Points
		}
	}
	return total, nil
}
