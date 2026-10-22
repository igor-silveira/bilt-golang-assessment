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

// award and reverse are each one atomic ledger call, so redeliveries and
// concurrent deliveries on other replicas cannot double-award or overrun the cap.
func (h *Handler) award(ctx context.Context, ev events.Payment) error {
	e, created, err := h.store.Award(ctx, ledger.Entry{
		MemberID:  ev.MemberID,
		PaymentID: ev.PaymentID,
		Kind:      ledger.KindAward,
		Points:    h.policy.Points(ev.AmountCents),
		Month:     Month(ev.OccurredAt),
	}, h.policy.MonthlyCap)
	if err != nil {
		return err
	}
	if !created {
		h.log.Info("payment already awarded", "payment_id", ev.PaymentID, "event_id", ev.EventID)
	} else if e.Points < h.policy.Points(ev.AmountCents) {
		h.log.Info("award cut by monthly cap", "payment_id", ev.PaymentID, "member_id", e.MemberID, "points", e.Points)
	}
	return nil
}

func (h *Handler) reverse(ctx context.Context, ev events.Payment) error {
	e, created, err := h.store.Reverse(ctx, ev.PaymentID)
	if errors.Is(err, ledger.ErrNotAwarded) {
		return ErrUnknownPayment
	}
	if err != nil {
		return err
	}
	if !created {
		h.log.Info("payment already reversed", "payment_id", ev.PaymentID, "event_id", ev.EventID)
	} else if e.MemberID != ev.MemberID {
		h.log.Warn("reversal member differs from award", "payment_id", ev.PaymentID, "event_member_id", ev.MemberID, "award_member_id", e.MemberID)
	}
	return nil
}
