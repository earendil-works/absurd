package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"time"

	"habitat/internal/config"
	"habitat/internal/web"
)

// Server exposes the HTTP API and static UI.
type Server struct {
	cfg config.Config
	db  *sql.DB

	staticHandler http.Handler
	indexHTML     []byte

	mux http.Handler
}

// New constructs a Server instance.
func New(cfg config.Config, db *sql.DB) (*Server, error) {
	s := &Server{
		cfg: cfg,
		db:  db,
	}

	staticFS, err := web.StaticFS()
	switch {
	case err == nil:
		s.staticHandler = http.FileServer(http.FS(staticFS))
	case errors.Is(err, fs.ErrNotExist):
		log.Printf("warning: no static assets found; run `npm run build` before building binaries")
		s.staticHandler = http.NotFoundHandler()
	default:
		return nil, fmt.Errorf("load static assets: %w", err)
	}

	if index, err := web.IndexHTML(); err == nil {
		s.indexHTML = index
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("load index html: %w", err)
	}

	s.mux = s.routes()
	return s, nil
}

// Run starts the HTTP server until the context is cancelled.
func (s *Server) Run(ctx context.Context) error {
	httpServer := &http.Server{
		Addr:              s.cfg.ListenAddress,
		Handler:           s.withLogging(s.mux),
		ReadHeaderTimeout: 10 * time.Second,
	}

	listener, err := net.Listen("tcp", s.cfg.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.cfg.ListenAddress, err)
	}
	log.Printf("dashboard listening on %s", listener.Addr())

	errCh := make(chan error, 1)
	go func() {
		if err := httpServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown server: %w", err)
		}
		return nil
	case err := <-errCh:
		return err
	}
}

func (s *Server) withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := &responseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(ww, r)
		duration := time.Since(start)
		log.Printf("%s %s -> %d (%s)", r.Method, r.URL.Path, ww.status, duration)
	})
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/_healthz", s.handleHealthz)
	mux.Handle("/_static/", http.StripPrefix("/_static/", s.staticHandler))

	mux.Handle("/api/metrics", http.HandlerFunc(s.handleMetrics))
	mux.Handle("/api/tasks", http.HandlerFunc(s.handleTasks))
	mux.Handle("/api/tasks/", http.HandlerFunc(s.handleTaskDetail))
	mux.Handle("/api/queues", http.HandlerFunc(s.handleQueues))
	mux.Handle("/api/queues/", http.HandlerFunc(s.handleQueueResource))
	mux.Handle("/api/events", http.HandlerFunc(s.handleEvents))
	mux.HandleFunc("/api/config", s.handleConfig)

	mux.HandleFunc("/", s.handleIndex)
	return mux
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (w *responseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
