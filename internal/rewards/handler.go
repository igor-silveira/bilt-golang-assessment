// Package rewards turns payment events into points ledger entries.
package rewards

import (
	"context"
	"errors"
	"log/slog"

	"github.com/rentpoints/pointsd/internal/events"
	"github.com/rentpoints/pointsd/internal/ledger"
	"github.com/rentpoints/pointsd/internal/pubsub"
)

// ErrUnknownPayment is returned for a reversal of a payment that was never awarded.
var ErrUnknownPayment = errors.New("rewards: reversal for unknown payment")

type Handler struct {
	store      ledger.Store
	policy     Policy
	deadLetter pubsub.Publisher
	log        *slog.Logger
}

func NewHandler(store ledger.Store, policy Policy, deadLetter pubsub.Publisher, log *slog.Logger) *Handler {
	return &Handler{store: store, policy: policy, deadLetter: deadLetter, log: log}
}

// Handle processes one delivery. The subscriber calls it concurrently
// (MaxOutstandingMessages per replica), and pointsd runs several replicas.
func (h *Handler) Handle(ctx context.Context, msg pubsub.Message) {
	ev, err := events.Decode(msg.Data())
	if err != nil {
		h.log.Warn("dead-lettering invalid event", "msg_id", msg.ID(), "err", err)
		if err := h.deadLetter.Publish(ctx, msg.Data(), map[string]string{"reason": err.Error()}); err != nil {
			h.log.Error("dead-letter publish failed", "msg_id", msg.ID(), "err", err)
			msg.Nack()
			return
		}
		msg.Ack()
		return
	}

	if err := h.process(ctx, ev); err != nil {
		h.log.Error("processing failed", "payment_id", ev.PaymentID, "type", ev.Type, "err", err)
	}
	msg.Ack()
}

func (h *Handler) process(ctx context.Context, ev events.Payment) error {
	switch ev.Type {
	case events.TypeSettled:
		return h.award(ctx, ev)
	case events.TypeReversed:
		return h.reverse(ctx, ev)
	}
	return nil
}

func (h *Handler) award(ctx context.Context, ev events.Payment) error {
	seen, err := h.store.HasEntry(ctx, ev.PaymentID, ledger.KindAward)
	if err != nil {
		return err
	}
	if seen {
		return nil
	}

	month := Month(ev.OccurredAt)
	used, err := h.store.AwardedInMonth(ctx, ev.MemberID, month)
	if err != nil {
		return err
	}
	points := h.policy.Points(ev.AmountCents)
	if room := h.policy.MonthlyCap - used; points > room {
		points = max(room, 0)
	}

	return h.store.Append(ctx, ledger.Entry{
		MemberID:  ev.MemberID,
		PaymentID: ev.PaymentID,
		Kind:      ledger.KindAward,
		Points:    points,
		Month:     month,
	})
}

func (h *Handler) reverse(ctx context.Context, ev events.Payment) error {
	awarded, err := h.store.HasEntry(ctx, ev.PaymentID, ledger.KindAward)
	if err != nil {
		return err
	}
	if !awarded {
		return ErrUnknownPayment
	}
	done, err := h.store.HasEntry(ctx, ev.PaymentID, ledger.KindReversal)
	if err != nil {
		return err
	}
	if done {
		return nil
	}

	return h.store.Append(ctx, ledger.Entry{
		MemberID:  ev.MemberID,
		PaymentID: ev.PaymentID,
		Kind:      ledger.KindReversal,
		Points:    -h.policy.Points(ev.AmountCents),
		Month:     Month(ev.OccurredAt),
	})
}
