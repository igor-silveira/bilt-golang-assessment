package ledger_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rentpoints/pointsd/internal/ledger"
)

const capPoints = 10_000

var ctx = context.Background()

func award(paymentID, memberID string, points int64) ledger.Entry {
	return ledger.Entry{MemberID: memberID, PaymentID: paymentID, Points: points, Month: "2026-10"}
}

func balance(t *testing.T, s *ledger.MemoryStore, member string) int64 {
	t.Helper()
	b, err := s.Balance(ctx, member)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestAward_IsIdempotentPerPayment(t *testing.T) {
	s := ledger.NewMemoryStore()
	first, created, err := s.Award(ctx, award("pay_1", "mbr_1", 1800), capPoints)
	if err != nil || !created || first.Points != 1800 || first.Kind != ledger.KindAward {
		t.Fatalf("first award = %+v, %v, %v", first, created, err)
	}
	// A republish may carry a different amount; the stored award wins.
	again, created, err := s.Award(ctx, award("pay_1", "mbr_1", 5000), capPoints)
	if err != nil || created || again != first {
		t.Fatalf("second award = %+v, %v, %v; want stored %+v, not created", again, created, err, first)
	}
	if got := balance(t, s, "mbr_1"); got != 1800 {
		t.Fatalf("balance = %d, want 1800", got)
	}
}

func TestAward_CutsToMonthlyCapPerMemberAndMonth(t *testing.T) {
	s := ledger.NewMemoryStore()
	for _, tc := range []struct {
		e    ledger.Entry
		want int64
	}{
		{award("pay_1", "mbr_1", 9500), 9500},
		{award("pay_2", "mbr_1", 1000), 500},
		{award("pay_3", "mbr_1", 1000), 0},
		{award("pay_4", "mbr_2", 1000), 1000},                                                     // other member
		{ledger.Entry{MemberID: "mbr_1", PaymentID: "pay_5", Points: 700, Month: "2026-11"}, 700}, // other month
	} {
		got, _, err := s.Award(ctx, tc.e, capPoints)
		if err != nil {
			t.Fatal(err)
		}
		if got.Points != tc.want {
			t.Fatalf("%s awarded %d, want %d", tc.e.PaymentID, got.Points, tc.want)
		}
	}
}

func TestAward_ConcurrentAwardsRespectCapAndDeduplicate(t *testing.T) {
	s := ledger.NewMemoryStore(ledger.WithLatency(time.Millisecond))
	var wg sync.WaitGroup
	run := func(paymentID, memberID string) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, err := s.Award(ctx, award(paymentID, memberID, 1000), capPoints); err != nil {
				t.Error(err)
			}
		}()
	}
	for i := 0; i < 50; i++ {
		// mbr_cap: 25 payments of 1,000, each delivered twice; only the cap binds.
		run(fmt.Sprintf("pay_cap_%d", i%25), "mbr_cap")
	}
	for i := 0; i < 20; i++ {
		// mbr_dup: 5 payments of 1,000, each delivered 4 times; stays under the
		// cap, so any double award shows in the balance.
		run(fmt.Sprintf("pay_dup_%d", i%5), "mbr_dup")
	}
	wg.Wait()
	if got := balance(t, s, "mbr_cap"); got != capPoints {
		t.Fatalf("mbr_cap balance = %d, want %d", got, capPoints)
	}
	if got := balance(t, s, "mbr_dup"); got != 5000 {
		t.Fatalf("mbr_dup balance = %d, want 5000 (payment awarded more than once)", got)
	}
}

func TestReverse_RemovesExactlyWhatWasAwardedOnce(t *testing.T) {
	s := ledger.NewMemoryStore()
	if _, _, err := s.Award(ctx, award("pay_1", "mbr_1", 9500), capPoints); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Award(ctx, award("pay_2", "mbr_1", 1000), capPoints); err != nil { // 500 awarded
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e, _, err := s.Reverse(ctx, "pay_2")
			if err != nil || e.Points != -500 || e.MemberID != "mbr_1" || e.Kind != ledger.KindReversal {
				t.Errorf("reverse = %+v, %v", e, err)
			}
		}()
	}
	wg.Wait()
	if got := balance(t, s, "mbr_1"); got != 9500 {
		t.Fatalf("balance = %d, want 9500", got)
	}
}

func TestReverse_DoesNotGiveCapRoomBack(t *testing.T) {
	s := ledger.NewMemoryStore()
	if _, _, err := s.Award(ctx, award("pay_1", "mbr_1", capPoints), capPoints); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Reverse(ctx, "pay_1"); err != nil {
		t.Fatal(err)
	}
	got, _, err := s.Award(ctx, award("pay_2", "mbr_1", 1000), capPoints)
	if err != nil {
		t.Fatal(err)
	}
	if got.Points != 0 {
		t.Fatalf("awarded %d after a reversal in a capped month, want 0", got.Points)
	}
}

func TestReverse_UnknownPayment(t *testing.T) {
	s := ledger.NewMemoryStore()
	if _, _, err := s.Reverse(ctx, "pay_unknown"); !errors.Is(err, ledger.ErrNotAwarded) {
		t.Fatalf("err = %v, want ErrNotAwarded", err)
	}
	if got := balance(t, s, "mbr_1"); got != 0 {
		t.Fatalf("balance = %d, want 0", got)
	}
}

func TestFaults_LeaveNoEffect(t *testing.T) {
	failing := true
	s := ledger.NewMemoryStore(ledger.WithFaults(func(op string) error {
		if failing && op != "balance" {
			return ledger.ErrUnavailable
		}
		return nil
	}))
	if _, _, err := s.Award(ctx, award("pay_1", "mbr_1", 100), capPoints); !errors.Is(err, ledger.ErrUnavailable) {
		t.Fatalf("award err = %v, want ErrUnavailable", err)
	}
	if got := balance(t, s, "mbr_1"); got != 0 {
		t.Fatalf("balance after failed award = %d, want 0", got)
	}
	failing = false
	if _, created, err := s.Award(ctx, award("pay_1", "mbr_1", 100), capPoints); err != nil || !created {
		t.Fatalf("retry = %v, %v; want created", created, err)
	}
}

func TestAppend_WritesAsIsAndAwardSeesIt(t *testing.T) {
	s := ledger.NewMemoryStore()
	seed := ledger.Entry{MemberID: "mbr_1", Kind: ledger.KindAward, Points: 100, Month: "2026-10"}
	for i := 0; i < 2; i++ { // seeding entries without a payment is allowed
		if err := s.Append(ctx, seed); err != nil {
			t.Fatal(err)
		}
	}
	e := ledger.Entry{MemberID: "mbr_1", PaymentID: "pay_1", Kind: ledger.KindAward, Points: 9000, Month: "2026-10"}
	if err := s.Append(ctx, e); err != nil {
		t.Fatal(err)
	}
	if _, created, _ := s.Award(ctx, award("pay_1", "mbr_1", 99), capPoints); created {
		t.Fatal("Award created a second award for an appended payment")
	}
	got, _, err := s.Award(ctx, award("pay_2", "mbr_1", 1000), capPoints)
	if err != nil {
		t.Fatal(err)
	}
	if got.Points != 800 {
		t.Fatalf("awarded %d, want 800 (appended entries count toward the cap)", got.Points)
	}
	if got := balance(t, s, "mbr_1"); got != 10_000 {
		t.Fatalf("balance = %d, want 10000", got)
	}
}
