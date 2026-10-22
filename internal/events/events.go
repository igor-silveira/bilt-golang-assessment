// Package events decodes the payment events published by the payments service.
package events

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const (
	TypeSettled  = "payment.settled"
	TypeReversed = "payment.reversed"
)

// ErrInvalid wraps every decoding or validation failure.
var ErrInvalid = errors.New("invalid event")

// Payment is a rent payment lifecycle event.
//
// The payments service publishes at-least-once and may republish the same
// payment with a new event_id, so payment_id is the identity of a payment.
type Payment struct {
	Type        string    `json:"type"`
	EventID     string    `json:"event_id"`
	PaymentID   string    `json:"payment_id"`
	MemberID    string    `json:"member_id"`
	AmountCents int64     `json:"amount_cents"`
	OccurredAt  time.Time `json:"occurred_at"`
}

// Decode parses and validates a raw event.
func Decode(data []byte) (Payment, error) {
	var p Payment
	if err := json.Unmarshal(data, &p); err != nil {
		return Payment{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	switch {
	case p.Type != TypeSettled && p.Type != TypeReversed:
		return Payment{}, fmt.Errorf("%w: unknown type %q", ErrInvalid, p.Type)
	case p.PaymentID == "" || p.MemberID == "":
		return Payment{}, fmt.Errorf("%w: missing payment_id or member_id", ErrInvalid)
	case p.AmountCents <= 0:
		return Payment{}, fmt.Errorf("%w: amount_cents must be positive", ErrInvalid)
	case p.OccurredAt.IsZero():
		return Payment{}, fmt.Errorf("%w: missing occurred_at", ErrInvalid)
	}
	return p, nil
}
