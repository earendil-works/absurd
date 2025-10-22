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

	"habitat/internal/auth"
	"habitat/internal/config"
	"habitat/internal/web"
)

const (
	sessionCookieName = "habitat_session"
	cookiePath        = "/"
)

// Server exposes the HTTP API and static UI.
type Server struct {
	cfg          config.Config
	db           *sql.DB
	auth         *auth.Authenticator
	sessions     *auth.SessionStore
	authRequired bool

	staticHandler http.Handler
	indexHTML     []byte

	mux http.Handler
}

// New constructs a Server instance.
func New(cfg config.Config, db *sql.DB, authenticator *auth.Authenticator) (*Server, error) {
	s := &Server{
		cfg:          cfg,
		db:           db,
		auth:         authenticator,
		sessions:     auth.NewSessionStore(12 * time.Hour),
		authRequired: authenticator != nil && authenticator.Enabled(),
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

	mux.Handle("/api/metrics", s.requireAuth(http.HandlerFunc(s.handleMetrics)))
	mux.Handle("/api/tasks", s.requireAuth(http.HandlerFunc(s.handleTasks)))
	mux.Handle("/api/tasks/", s.requireAuth(http.HandlerFunc(s.handleTaskDetail)))
	mux.Handle("/api/queues", s.requireAuth(http.HandlerFunc(s.handleQueues)))
	mux.Handle("/api/queues/", s.requireAuth(http.HandlerFunc(s.handleQueueTasks)))
	mux.HandleFunc("/api/config", s.handleConfig)
	mux.HandleFunc("/api/login", s.handleLogin)
	mux.HandleFunc("/api/logout", s.handleLogout)

	mux.HandleFunc("/", s.handleIndex)
	return mux
}

func (s *Server) requireAuth(next http.Handler) http.Handler {
	if !s.authRequired {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if username := s.sessionUsername(r); username != "" {
			ctx := context.WithValue(r.Context(), userContextKey{}, username)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	})
}

func (s *Server) sessionUsername(r *http.Request) string {
	cookie, err := r.Cookie(sessionCookieName)
	if err != nil {
		return ""
	}
	username, ok := s.sessions.Validate(cookie.Value)
	if !ok {
		return ""
	}
	return username
}

func (s *Server) setSessionCookie(w http.ResponseWriter, sessionID string) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    sessionID,
		Path:     cookiePath,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
}

func (s *Server) clearSessionCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Expires:  time.Unix(0, 0),
		MaxAge:   -1,
		Path:     cookiePath,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
	})
}

type userContextKey struct{}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (w *responseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
