package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"

	"habitat/internal/config"
	"habitat/internal/server"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(2)
	}

	cmd := os.Args[1]
	switch cmd {
	case "run":
		if err := run(os.Args[2:]); err != nil {
			log.Fatalf("error: %v", err)
		}
	default:
		printUsage()
		os.Exit(2)
	}
}

func run(args []string) error {
	cfg, err := config.FromArgs(args)
	if err != nil {
		return err
	}

	connStr, err := cfg.DB.ConnectionString()
	if err != nil {
		return fmt.Errorf("build postgres connection: %w", err)
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	srv, err := server.New(cfg, db)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := srv.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	return nil
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "usage: habitat run [options]")
}
