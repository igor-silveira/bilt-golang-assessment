package rewards

import "time"

// Policy is the earning rule for rent payments.
type Policy struct {
	PointsPerDollar int64
	MonthlyCap      int64 // max points awarded per member per settlement month
}

var DefaultPolicy = Policy{PointsPerDollar: 1, MonthlyCap: 10_000}

// Points returns the uncapped points for a payment amount. Cents are truncated.
func (p Policy) Points(amountCents int64) int64 {
	return amountCents / 100 * p.PointsPerDollar
}

// Month is the cap bucket of a settlement time.
func Month(t time.Time) string {
	return t.UTC().Format("2006-01")
}
