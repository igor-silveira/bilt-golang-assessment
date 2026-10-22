package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rentpoints/pointsd/internal/api"
	"github.com/rentpoints/pointsd/internal/ledger"
)

func TestBalance(t *testing.T) {
	store := ledger.NewMemoryStore()
	if err := store.Append(context.Background(), ledger.Entry{MemberID: "mbr_1", PaymentID: "pay_1", Kind: ledger.KindAward, Points: 750, Month: "2026-10"}); err != nil {
		t.Fatal(err)
	}
	srv := api.NewServer(store)

	for _, tc := range []struct {
		member string
		want   int64
	}{{"mbr_1", 750}, {"mbr_unknown", 0}} {
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/members/"+tc.member+"/points", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: status %d", tc.member, rec.Code)
		}
		var body struct {
			MemberID string `json:"member_id"`
			Points   int64  `json:"points"`
		}
		if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.MemberID != tc.member || body.Points != tc.want {
			t.Fatalf("got %+v, want member %s with %d points", body, tc.member, tc.want)
		}
	}
}
