package config

import (
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strconv"
)

const (
	envPrefix            = "HABITAT_"
	defaultListenAddress = ":7890"
	defaultDBURL         = ""
	defaultDBHost        = "localhost"
	defaultDBName        = "absurd"
	defaultDBPort        = 5432
	defaultDBSSLMode     = "disable"
	defaultDBSSLCert     = ""
	defaultDBSSLRootCert = ""
	defaultDBSSLKey      = ""
)

// Config captures runtime configuration for the dashboard server.
type Config struct {
	ListenAddress string
	DB            DBConfig
}

// DBConfig describes how to connect to Postgres.
type DBConfig struct {
	URL         string
	Host        string
	Port        int
	User        string
	Password    string
	Name        string
	SSLMode     string
	SSLCert     string
	SSLKey      string
	SSLRootCert string
}

// FromArgs parses command-line arguments and environment variables to produce
// a Config. Flags take precedence over environment variables.
func FromArgs(args []string) (Config, error) {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)

	cfg := Config{
		ListenAddress: envDefault("LISTEN", defaultListenAddress),
		DB: DBConfig{
			URL:         envDefault("DB_URL", defaultDBURL),
			Host:        envDefault("DB_HOST", defaultDBHost),
			Port:        envDefaultInt("DB_PORT", defaultDBPort),
			User:        envDefault("DB_USER", ""),
			Password:    envDefault("DB_PASSWORD", ""),
			Name:        envDefault("DB_NAME", defaultDBName),
			SSLMode:     envDefault("DB_SSLMODE", defaultDBSSLMode),
			SSLCert:     envDefault("DB_SSLCERT", defaultDBSSLCert),
			SSLKey:      envDefault("DB_SSLKEY", defaultDBSSLKey),
			SSLRootCert: envDefault("DB_SSLROOTCERT", defaultDBSSLRootCert),
		},
	}

	fs.StringVar(&cfg.ListenAddress, "listen", cfg.ListenAddress, "address to listen on (e.g. :7890)")
	fs.StringVar(&cfg.DB.URL, "db-url", cfg.DB.URL, "Postgres connection URL")
	fs.StringVar(&cfg.DB.Host, "db-host", cfg.DB.Host, "Postgres host")
	fs.IntVar(&cfg.DB.Port, "db-port", cfg.DB.Port, "Postgres port")
	fs.StringVar(&cfg.DB.User, "db-user", cfg.DB.User, "Postgres user")
	fs.StringVar(&cfg.DB.Password, "db-password", cfg.DB.Password, "Postgres password")
	fs.StringVar(&cfg.DB.Name, "db-name", cfg.DB.Name, "Postgres database name")
	fs.StringVar(&cfg.DB.SSLMode, "db-sslmode", cfg.DB.SSLMode, "Postgres sslmode")
	fs.StringVar(&cfg.DB.SSLCert, "db-sslcert", cfg.DB.SSLCert, "Postgres sslcert")
	fs.StringVar(&cfg.DB.SSLKey, "db-sslkey", cfg.DB.SSLKey, "Postgres sslkey")
	fs.StringVar(&cfg.DB.SSLRootCert, "db-sslrootcert", cfg.DB.SSLRootCert, "Postgres sslrootcert")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	if cfg.DB.URL == "" {
		if cfg.DB.Host == "" {
			return Config{}, errors.New("database host is required when --db-url is not supplied (set HABITAT_DB_HOST)")
		}
		if cfg.DB.Name == "" {
			return Config{}, errors.New("database name is required when --db-url is not supplied (set HABITAT_DB_NAME)")
		}
	}

	return cfg, nil
}

// ConnectionString builds the effective connection string for the supplied DB settings.
func (c DBConfig) ConnectionString() (string, error) {
	if c.URL != "" {
		return c.URL, nil
	}

	host := c.Host
	if host == "" {
		return "", errors.New("database host is required")
	}
	if c.Name == "" {
		return "", errors.New("database name is required")
	}

	u := &url.URL{
		Scheme: "postgres",
		Host:   fmt.Sprintf("%s:%d", host, c.Port),
		Path:   "/" + c.Name,
	}

	if c.User != "" {
		if c.Password != "" {
			u.User = url.UserPassword(c.User, c.Password)
		} else {
			u.User = url.User(c.User)
		}
	}

	query := url.Values{}
	if c.SSLMode != "" {
		query.Set("sslmode", c.SSLMode)
	}
	if c.SSLCert != "" {
		query.Set("sslcert", c.SSLCert)
	}
	if c.SSLKey != "" {
		query.Set("sslkey", c.SSLKey)
	}
	if c.SSLRootCert != "" {
		query.Set("sslrootcert", c.SSLRootCert)
	}
	u.RawQuery = query.Encode()

	return u.String(), nil
}

func envDefault(name, fallback string) string {
	if v := os.Getenv(envPrefix + name); v != "" {
		return v
	}
	return fallback
}

func envDefaultInt(name string, fallback int) int {
	if v := os.Getenv(envPrefix + name); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			return parsed
		}
	}
	return fallback
}
