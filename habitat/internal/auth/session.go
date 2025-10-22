package auth

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

const defaultSessionTTL = 24 * time.Hour

// SessionStore keeps dashboard sessions in memory.
type SessionStore struct {
	mu       sync.RWMutex
	sessions map[string]session
	ttl      time.Duration
}

type session struct {
	username string
	expires  time.Time
}

// NewSessionStore creates a new in-memory session store.
func NewSessionStore(ttl time.Duration) *SessionStore {
	if ttl <= 0 {
		ttl = defaultSessionTTL
	}
	return &SessionStore{
		sessions: make(map[string]session),
		ttl:      ttl,
	}
}

// Create issues a new session identifier for the username.
func (s *SessionStore) Create(username string) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cleanupLocked()

	id := uuid.New().String()
	s.sessions[id] = session{
		username: username,
		expires:  time.Now().Add(s.ttl),
	}
	return id
}

// Validate returns the username for a session identifier if it is still valid.
func (s *SessionStore) Validate(id string) (string, bool) {
	if id == "" {
		return "", false
	}

	s.mu.RLock()
	session, ok := s.sessions[id]
	s.mu.RUnlock()
	if !ok {
		return "", false
	}
	if time.Now().After(session.expires) {
		s.Delete(id)
		return "", false
	}
	return session.username, true
}

// Delete removes the session identifier.
func (s *SessionStore) Delete(id string) {
	if id == "" {
		return
	}
	s.mu.Lock()
	delete(s.sessions, id)
	s.mu.Unlock()
}

func (s *SessionStore) cleanupLocked() {
	now := time.Now()
	for id, sess := range s.sessions {
		if now.After(sess.expires) {
			delete(s.sessions, id)
		}
	}
}
