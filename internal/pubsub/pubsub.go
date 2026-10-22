// Package pubsub defines the subset of the broker client that pointsd depends on.
// Production wires these to Google Cloud Pub/Sub; tests use in-memory fakes.
package pubsub

import "context"

// Message is a single delivery from a subscription.
//
// Delivery is at-least-once: until a message is acked, the broker may deliver it
// again, including concurrently to another replica. Nack asks the broker to
// redeliver it after a backoff.
type Message interface {
	ID() string
	Data() []byte
	DeliveryAttempt() int
	Ack()
	Nack()
}

// Publisher publishes raw payloads to a topic.
type Publisher interface {
	Publish(ctx context.Context, data []byte, attrs map[string]string) error
}
