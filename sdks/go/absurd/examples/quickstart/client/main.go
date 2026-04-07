package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/earendil-works/absurd/sdks/go/absurd"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type ProvisionUserParams struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
}

func main() {
	var shouldAwait bool
	flag.BoolVar(&shouldAwait, "await", false, "wait for task completion")
	flag.Parse()

	userID := "alice"
	if flag.NArg() > 0 {
		userID = flag.Arg(0)
	}

	email := fmt.Sprintf("%s@example.com", userID)
	if flag.NArg() > 1 {
		email = flag.Arg(1)
	}

	ctx := context.Background()

	app, err := absurd.New(absurd.Options{QueueName: "default", DriverName: "pgx"})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := app.Close(); err != nil {
			log.Printf("close failed: %v", err)
		}
	}()

	queue := app.QueueName()

	spawned, err := app.Spawn(
		ctx,
		"provision-user",
		ProvisionUserParams{
			UserID: userID,
			Email:  email,
		},
		absurd.SpawnOptions{QueueName: queue},
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("spawned: %+v\n", spawned)

	snapshot, err := app.FetchTaskResult(ctx, queue, spawned.TaskID)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("current snapshot: %+v\n", snapshot)

	if shouldAwait {
		fmt.Printf("waiting for completion; emit user-activated:%s on queue default\n", userID)
		final, err := app.AwaitTaskResult(ctx, queue, spawned.TaskID, absurd.AwaitTaskResultOptions{
			Timeout: 5 * time.Minute,
		})
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("final snapshot: %+v\n", final)
	}
}
