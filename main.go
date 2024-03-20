package main

import (
	"log"
	"log/slog"
	"net/http"
	"scratch/internal"
	"time"
)

const requestTimeout = 3 * time.Second

func main() {
	store, err := internal.NewDuckDBStore()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if closeErr := store.Close(); closeErr != nil {
			slog.Error("closing store: %w", err)
		}
	}()
	mux := internal.NewServer(store).NewServeMux()
	server := &http.Server{
		Addr:              ":8000",
		ReadHeaderTimeout: requestTimeout,
		Handler:           mux,
	}

	if err = server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
