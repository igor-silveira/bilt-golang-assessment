package rewards_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rentpoints/pointsd/internal/ledger"
	"github.com/rentpoints/pointsd/internal/rewards"
)

const (
	replicaCount           = 3  // production replicas
	maxOutstandingMessages = 10 // per replica
)

// flakyReplyStore loses the reply of every nth successful write, like a
// Postgres commit whose result never reaches the replica.
type flakyReplyStore struct {
	ledger.Store
	n      int64
	writes atomic.Int64
	off    atomic.Bool
}

func (s *flakyReplyStore) lose() bool { return !s.off.Load() && s.writes.Add(1)%s.n == 0 }

func (s *flakyReplyStore) Award(ctx context.Context, e ledger.Entry, monthlyCap int64) (ledger.Entry, bool, error) {
	stored, created, err := s.Store.Award(ctx, e, monthlyCap)
	if err == nil && s.lose() {
		return ledger.Entry{}, false, ledger.ErrUnavailable
	}
	return stored, created, err
}

func (s *flakyReplyStore) Reverse(ctx context.Context, paymentID string) (ledger.Entry, bool, error) {
	stored, created, err := s.Store.Reverse(ctx, paymentID)
	if err == nil && s.lose() {
		return ledger.Entry{}, false, ledger.ErrUnavailable
	}
	return stored, created, err
}

// cluster simulates the subscription: replicaCount handlers sharing one store,
// each pulling from its own inbox with maxOutstandingMessages workers. An event
// is delivered once or to several replicas at once, and a nacked delivery is
// redelivered to a random replica, possibly while other copies are in flight.
type cluster struct {
	t        *testing.T
	inboxes  []chan *fakeMessage
	inflight sync.WaitGroup
	workers  sync.WaitGroup
}

func newCluster(t *testing.T, store ledger.Store) *cluster {
	c := &cluster{t: t}
	for r := 0; r < replicaCount; r++ {
		h, inbox := newHandler(store), make(chan *fakeMessage)
		c.inboxes = append(c.inboxes, inbox)
		for w := 0; w < maxOutstandingMessages; w++ {
			c.workers.Add(1)
			go func() {
				defer c.workers.Done()
				for msg := range inbox {
					c.handle(h, msg)
				}
			}()
		}
	}
	return c
}

func (c *cluster) handle(h *rewards.Handler, msg *fakeMessage) {
	defer c.inflight.Done()
	h.Handle(context.Background(), msg)
	if msg.outcome(c.t) == nacked {
		if msg.attempt >= 20 {
			c.t.Errorf("message %s still nacked after %d attempts", msg.id, msg.attempt)
			return
		}
		c.send(rand.IntN(replicaCount), &fakeMessage{id: msg.id, data: msg.data, attempt: msg.attempt + 1})
	}
}

func (c *cluster) send(replica int, msg *fakeMessage) {
	c.inflight.Add(1)
	go func() { c.inboxes[replica] <- msg }()
}

// publish delivers event i to 1 + i%replicaCount replicas at once, so some
// events arrive only once (an ack on error loses them) and others concurrently
// on several replicas. It waits until every delivery, including redeliveries,
// is acked.
func (c *cluster) publish(evs []event) {
	for i, ev := range evs {
		for r := 0; r <= i%replicaCount; r++ {
			c.send(r, newMessage(c.t, ev))
		}
	}
	c.inflight.Wait()
}

func (c *cluster) stop() {
	for _, inbox := range c.inboxes {
		close(inbox)
	}
	c.workers.Wait()
}

func TestReplicas_RulesHoldUnderConcurrentRedeliveryAndFaults(t *testing.T) {
	var calls atomic.Int64
	var faultsOff atomic.Bool
	mem := ledger.NewMemoryStore(ledger.WithLatency(dbLatency), ledger.WithFaults(func(string) error {
		if !faultsOff.Load() && calls.Add(1)%7 == 0 {
			return ledger.ErrUnavailable // fails before the write
		}
		return nil
	}))
	store := &flakyReplyStore{Store: mem, n: 5} // succeeds but the reply is lost
	c := newCluster(t, store)
	defer c.stop()

	const members = 10
	var settles, reversals []event
	want := map[string]int64{}
	for m := 0; m < members; m++ {
		// Over the cap and nothing reversed: exactly the cap.
		capped := fmt.Sprintf("mbr_cap_%d", m)
		for p := 0; p < 8; p++ {
			settles = append(settles, settled(fmt.Sprintf("pay_cap_%d_%d", m, p), capped, 2000))
		}
		want[capped] = rewards.DefaultPolicy.MonthlyCap

		// Under the cap, two of four payments reversed: the other two remain.
		partial := fmt.Sprintf("mbr_rev_%d", m)
		for p := 0; p < 4; p++ {
			dollars := int64(1000 + 100*p)
			id := fmt.Sprintf("pay_rev_%d_%d", m, p)
			settles = append(settles, settled(id, partial, dollars))
			if p < 2 {
				reversals = append(reversals, reversed(id, partial, dollars))
			} else {
				want[partial] += dollars
			}
		}

		// Over the cap, every payment reversed: each reversal must remove exactly
		// what the (possibly cut) award earned, so the balance returns to 0.
		clawback := fmt.Sprintf("mbr_clawback_%d", m)
		for p := 0; p < 8; p++ {
			id := fmt.Sprintf("pay_clawback_%d_%d", m, p)
			settles = append(settles, settled(id, clawback, 2000))
			reversals = append(reversals, reversed(id, clawback, 2000))
		}
		want[clawback] = 0
	}
	rand.Shuffle(len(settles), func(i, j int) { settles[i], settles[j] = settles[j], settles[i] })
	rand.Shuffle(len(reversals), func(i, j int) { reversals[i], reversals[j] = reversals[j], reversals[i] })

	c.publish(settles)
	c.publish(reversals)
	faultsOff.Store(true)
	store.off.Store(true)

	for member, w := range want {
		if got := balance(t, mem, member); got != w {
			t.Errorf("%s balance = %d, want %d", member, got, w)
		}
	}
}
