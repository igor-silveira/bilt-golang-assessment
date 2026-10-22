# pointsd

Awards rewards points for rent payments. `pointsd` consumes `payment.settled` and
`payment.reversed` events from the `payments-events` Pub/Sub subscription, writes
entries to the points ledger, and serves member balances.

## Earning rules

- 1 point per whole dollar of rent (cents are truncated).
- At most 10,000 points per member per calendar month, by the payment's settlement
  time in UTC. Reversals do not give cap room back.
- A payment earns once. The payments service may republish a payment with a new
  `event_id`; `payment_id` is the identity of a payment.
- A reversal (chargeback, ACH return) removes the points that payment earned.
- Reversals of payments that never earned points are acked and logged; finance
  reconciles them from the logs.

## Delivery

- The subscription is at-least-once. A message can be redelivered, including to a
  different replica while the first delivery is still being processed.
- Production runs 3 replicas, each with `MaxOutstandingMessages = 10`.
- `Ack` when a message is handled. `Nack` to have it redelivered after a backoff.
- Payloads that fail validation are published to the dead-letter topic and acked.

## Storage

Production uses Postgres (`ledger_entries`). `ledger.MemoryStore` is the local
stand-in; `WithLatency` and `WithFaults` approximate database round trips and outages.
`Balance` is the sum of the points on the stored entries. A duplicate, a cap cut,
or a reversal has to be right on the entry that was written.

## Layout

```
cmd/pointsd        entrypoint (local mode replays a JSONL file)
internal/events    event decoding and validation
internal/ledger    ledger store
internal/rewards   event handler and earning policy
internal/api       balance endpoint
internal/pubsub    broker interfaces
```

## Development

Requires Go 1.22+.

```
go test -race ./...
go vet ./...
go run ./cmd/pointsd -events testdata/events.jsonl
curl localhost:8080/v1/members/mbr_raj/points
```
