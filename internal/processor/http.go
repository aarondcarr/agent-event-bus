package processor

import (
	"encoding/json"
	"net/http"
	"sort"

	"github.com/aarondcarr/agent-event-bus/internal/metrics"
	"github.com/aarondcarr/agent-event-bus/internal/policy"
)

// AgentView is what GET /v1/agents/{id} returns.
type AgentView struct {
	AgentID string             `json:"agent_id"`
	Killed  bool               `json:"killed"`
	Reason  string             `json:"reason,omitempty"`
	Stats   metrics.AgentStats `json:"stats"`
}

// API serves the read side: kill-switch status and aggregates.
func API(ev *policy.Evaluator, agg *metrics.Aggregator) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /v1/agents", func(w http.ResponseWriter, _ *http.Request) {
		ids := agg.Agents()
		sort.Strings(ids)
		writeJSON(w, http.StatusOK, ids)
	})
	mux.HandleFunc("GET /v1/agents/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		stats, ok := agg.Get(id)
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "unknown agent"})
			return
		}
		killed, reason := ev.Status(id)
		writeJSON(w, http.StatusOK, AgentView{AgentID: id, Killed: killed, Reason: reason, Stats: stats})
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
