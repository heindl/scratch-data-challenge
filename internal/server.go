package internal

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

type Server struct {
	store *Store
}

func NewServer(store *Store) *Server {
	return &Server{store: store}
}

func (s *Server) NewServeMux() *http.ServeMux {
	m := http.NewServeMux()
	m.HandleFunc("GET /query", s.HandleQuery)
	m.HandleFunc("POST /data", s.HandleData)
	return m
}

func (s *Server) writeError(w http.ResponseWriter, code int, msg string, err error) {
	w.WriteHeader(code)
	if _, err = w.Write([]byte(err.Error())); err != nil {
		slog.Error("%s: %w", msg, err)
	}
}

func (s *Server) HandleQuery(w http.ResponseWriter, r *http.Request) {
	res, err := s.store.Query(r.Context(), &QueryStatement{
		Query: r.URL.Query().Get("q"),
	})
	if err != nil {
		// TODO: Setting standard error for now but should increase the resolution of error response codes.
		s.writeError(w, http.StatusBadRequest, "handle Query: writing error response", err)
		return
	}
	out, err := json.Marshal(res)
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, "handle Query: writing json marshal error response", err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err = w.Write(out); err != nil {
		slog.Error("handle Query: writing response: %w", err)
	}
}

func (s *Server) HandleData(w http.ResponseWriter, r *http.Request) {
	var columns map[string]any
	if err := json.NewDecoder(r.Body).Decode(&columns); err != nil {
		s.writeError(w, http.StatusBadRequest, "handle data: decoding request body", err)
		return
	}
	stmt := &InsertStatement{
		Table:   r.URL.Query().Get("Table"),
		Columns: columns,
	}
	if err := stmt.Validate(); err != nil {
		s.writeError(w, http.StatusBadRequest, "handle data: validating insert statement", err)
	}
	if err := s.store.Insert(r.Context(), stmt); err != nil {
		// TODO: Setting standard error for now but should increase the resolution of error response codes.
		s.writeError(w, http.StatusInternalServerError, "writing error response", err)
		return
	}
	w.WriteHeader(http.StatusOK)
}
