package auth

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/go-crypt/crypt"
)

// Authenticator wraps the passwd style user store.
type Authenticator struct {
	enabled bool
	entries map[string]string
}

// LoadFromFile constructs an Authenticator from a passwd style file where each line
// contains "username:hashed_password".
func LoadFromFile(path string) (*Authenticator, error) {
	if path == "" {
		return &Authenticator{enabled: false, entries: nil}, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open auth file: %w", err)
	}
	defer f.Close()

	entries := make(map[string]string)

	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid auth file format on line %d", lineNum)
		}

		username := strings.TrimSpace(parts[0])
		if username == "" {
			return nil, fmt.Errorf("empty username on line %d", lineNum)
		}

		passwordHash := strings.TrimSpace(parts[1])
		if passwordHash == "" {
			return nil, fmt.Errorf("empty password hash on line %d", lineNum)
		}

		entries[username] = passwordHash
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read auth file: %w", err)
	}

	if len(entries) == 0 {
		return nil, errors.New("auth file contains no valid entries")
	}

	return &Authenticator{
		enabled: true,
		entries: entries,
	}, nil
}

// Enabled reports whether authentication is enforced.
func (a *Authenticator) Enabled() bool {
	return a != nil && a.enabled
}

// Authenticate validates the password for the supplied username. When the
// authenticator is disabled the method always returns true.
func (a *Authenticator) Authenticate(username, password string) (bool, error) {
	if a == nil || !a.enabled {
		return true, nil
	}

	hash, ok := a.entries[username]
	if !ok {
		return false, nil
	}

	valid, err := crypt.CheckPassword(password, hash)
	if err != nil {
		return false, fmt.Errorf("check password: %w", err)
	}
	return valid, nil
}
