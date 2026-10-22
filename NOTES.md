# Ledger in Postgres
Schema: `ledger_entries(id, member_id, payment_id, kind, points, month)`, `UNIQUE (payment_id, kind)`, index on `(member_id, month)`;
`member_month_locks(member_id, month, PRIMARY KEY (member_id, month))` exists only to be locked. The ledger stays the one source of truth.

**Award(e, cap)**, one transaction:
1. `INSERT INTO member_month_locks VALUES ($member, $month) ON CONFLICT DO NOTHING;`
2. `SELECT 1 FROM member_month_locks WHERE member_id=$member AND month=$month FOR UPDATE;` serializes this member-month.
3. `SELECT * FROM ledger_entries WHERE payment_id=$payment AND kind='award';` if found: `COMMIT`, return it (duplicate).
4. `used := SELECT COALESCE(SUM(points), 0) FROM ledger_entries WHERE member_id=$member AND month=$month AND kind='award';`
5. `INSERT INTO ledger_entries ... 'award', GREATEST(LEAST($points, $cap - used), 0) ...; COMMIT;`

**Reverse(payment)**, one statement (then a `SELECT` to tell "already reversed" from "never awarded" if no row):
`INSERT INTO ledger_entries (member_id, payment_id, kind, points, month) SELECT member_id, payment_id, 'reversal', -points, month`
`FROM ledger_entries WHERE payment_id=$payment AND kind='award' ON CONFLICT (payment_id, kind) DO NOTHING RETURNING points;`
Why it holds with several replicas: the lock in step 2 lives in the database, so Awards for one member-month run one at a time on
every replica, and under READ COMMITTED steps 3-4 run after the lock and see every award committed before it. Other members lock
other rows and run in parallel. `UNIQUE (payment_id, kind)` is the backstop: a racing duplicate with a different member or month
fails with unique_violation, rolls back, and on redelivery step 3 returns the stored award. Reversal copies the stored award's points,
and the cap sums awards only, so reversals give no cap room back. Each op is idempotent, so nacking on any ledger error is safe.
Migration: with finance, delete the incident's duplicate award rows (keep the lowest `id` per payment), then add the constraint.
