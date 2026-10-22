# Ledger in Postgres
Schema: `ledger_entries(member_id, payment_id, kind, points, month)` with `UNIQUE (payment_id, kind)`, plus
`member_month_awards(member_id, month, awarded, PRIMARY KEY (member_id, month))` as the cap counter.

**Award(e, cap)**, one transaction:
1. `INSERT INTO member_month_awards VALUES ($member, $month, 0) ON CONFLICT DO NOTHING;`
2. `SELECT awarded FROM member_month_awards WHERE member_id=$member AND month=$month FOR UPDATE;`
3. `SELECT * FROM ledger_entries WHERE payment_id=$payment AND kind='award';` if found: `COMMIT`, return it (duplicate).
4. `points := GREATEST(LEAST($points, $cap - awarded), 0)`; `INSERT INTO ledger_entries ... 'award', points ...;`
5. `UPDATE member_month_awards SET awarded = awarded + points WHERE ...; COMMIT;`

**Reverse(payment)**, one statement (then a `SELECT` to tell "already reversed" from "never awarded" if no row):
`INSERT INTO ledger_entries (member_id, payment_id, kind, points, month) SELECT member_id, payment_id, 'reversal', -points, month`
`FROM ledger_entries WHERE payment_id=$payment AND kind='award' ON CONFLICT (payment_id, kind) DO NOTHING RETURNING points;`
Why it holds with several replicas: the lock in step 2 lives in the database, so every Award for one member-month is serialized
across all replicas, and the check, cap and insert see a consistent total. Different members lock different rows and run in parallel.
`UNIQUE (payment_id, kind)` is the backstop: a racing duplicate (even one carrying a different member or month) fails with
unique_violation, rolls back, and on retry step 3 returns the stored award. Reversal takes the points from the stored award, so it
removes exactly what was earned, and the counter is never decremented, so reversals give no cap room back. Every operation is
idempotent, so the handler nacks on any ledger error and a redelivery after a commit whose reply was lost changes nothing.
