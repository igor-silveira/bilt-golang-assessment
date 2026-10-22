// Package api serves the read side of the points ledger.
package api

import (
	"encoding/json"
	"net/http"

	"github.com/rentpoints/pointsd/internal/ledger"
)

type balanceResponse struct {
	MemberID string `json:"member_id"`
	Points   int64  `json:"points"`
}

func NewServer(store ledger.Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/members/{id}/points", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		points, err := store.Balance(r.Context(), id)
		if err != nil {
			http.Error(w, "ledger unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(balanceResponse{MemberID: id, Points: points})
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
