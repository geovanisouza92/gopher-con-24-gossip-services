package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"time"
)

var cache = make(map[string][]byte)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, nil)))

	hostname, err := os.Hostname()
	if err != nil {
		slog.Error("failed to get hostname", "err", err)
		os.Exit(1)
	}

	ctrl := &controller{
		onGet: func(key []byte) []byte {
			slog.Info("onGet", "key", key)
			return cache[string(key)]
		},
		onPurge: func(key []byte) {
			slog.Info("onPurge", "key", key)
			delete(cache, string(key))
		},
	}

	stopCh := make(chan struct{})
	connect(stopCh, "gopher-con-24", ctrl)

	mux := http.NewServeMux()

	mux.HandleFunc("GET /{key}", loggingMiddleware(func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")

		if value, ok := cache[key]; ok {
			w.Header().Add("X-Source", "local")
			w.Header().Add("X-Hostname", hostname)
			w.Write(value)
			return
		}

		if res := ctrl.get([]byte(key)); res != nil {
			w.Header().Add("X-Source", "cluster")
			w.Header().Add("X-Hostname", hostname)
			w.Write(res)
			return
		}

		http.Error(w, "key not found", http.StatusNotFound)
	}))

	mux.HandleFunc("POST /{key}", loggingMiddleware(func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")

		value, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read request body", http.StatusInternalServerError)
			return
		}

		cache[key] = value
		ctrl.purge([]byte(key))
	}))

	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}
	go func() {
		slog.Info("starting server", "addr", ":8080")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("failed to start server", "error", err)
			os.Exit(1)
		}
	}()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	<-signalCh

	slog.Info("stopping server")
	close(stopCh)

	if err := srv.Shutdown(context.Background()); err != nil {
		slog.Error("failed to stop server", "error", err)
		os.Exit(1)
	}
}

func loggingMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		slog.Info(fmt.Sprintf("%s - - [%s] \"%s %s %s\" %s\n",
			r.RemoteAddr,
			time.Now().Format("02/Jan/2006:15:04:05 -0700"),
			r.Method,
			r.URL.Path,
			r.Proto,
			r.UserAgent(),
		))
		next(w, r)
	}
}
