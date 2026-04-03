package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/earendil-works/absurd/sdks/go/absurd"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type ProvisionUserParams struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
}

type UserRecord struct {
	UserID    string    `json:"user_id"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"created_at"`
}

type OutageState struct {
	Simulated bool `json:"simulated"`
}

type DeliveryResult struct {
	Sent     bool   `json:"sent"`
	Provider string `json:"provider"`
	To       string `json:"to"`
}

type ActivationEvent struct {
	ActivatedAt time.Time `json:"activated_at"`
}

type ProvisionUserResult struct {
	UserID      string         `json:"user_id"`
	Email       string         `json:"email"`
	Delivery    DeliveryResult `json:"delivery"`
	Status      string         `json:"status"`
	ActivatedAt time.Time      `json:"activated_at"`
}

var provisionUserTask = absurd.Task(
	"provision-user",
	func(ctx context.Context, params ProvisionUserParams) (ProvisionUserResult, error) {
		task := absurd.MustTaskContext(ctx)

		user, err := absurd.Step(ctx, "create-user-record", func(ctx context.Context) (UserRecord, error) {
			log.Printf("[%s] creating user record for %s", task.TaskID(), params.UserID)
			return UserRecord{
				UserID:    params.UserID,
				Email:     params.Email,
				CreatedAt: time.Now().UTC(),
			}, nil
		})
		if err != nil {
			return ProvisionUserResult{}, err
		}

		// Demo only: fail once after the first checkpoint so the retry behavior is visible.
		outage, err := absurd.BeginStep[OutageState](ctx, "demo-transient-outage")
		if err != nil {
			return ProvisionUserResult{}, err
		}
		if !outage.Done {
			log.Printf("[%s] simulating a temporary email provider outage", task.TaskID())
			if _, err := outage.CompleteStep(ctx, OutageState{Simulated: true}); err != nil {
				return ProvisionUserResult{}, err
			}
			return ProvisionUserResult{}, errors.New("temporary email provider outage")
		}

		delivery, err := absurd.Step(ctx, "send-activation-email", func(ctx context.Context) (DeliveryResult, error) {
			log.Printf("[%s] sending activation email to %s", task.TaskID(), user.Email)
			return DeliveryResult{
				Sent:     true,
				Provider: "demo-mail",
				To:       user.Email,
			}, nil
		})
		if err != nil {
			return ProvisionUserResult{}, err
		}

		eventName := fmt.Sprintf("user-activated:%s", user.UserID)
		log.Printf("[%s] waiting for %s", task.TaskID(), eventName)

		activation, err := absurd.AwaitEvent[ActivationEvent](ctx, eventName, absurd.AwaitEventOptions{
			Timeout: time.Hour,
		})
		if err != nil {
			return ProvisionUserResult{}, err
		}

		return ProvisionUserResult{
			UserID:      user.UserID,
			Email:       user.Email,
			Delivery:    delivery,
			Status:      "active",
			ActivatedAt: activation.ActivatedAt,
		}, nil
	},
	absurd.TaskOptions{DefaultMaxAttempts: 5},
)

func main() {
	runCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	app, err := absurd.New(absurd.Options{QueueName: "default", DriverName: "pgx"})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := app.Close(); err != nil {
			log.Printf("close failed: %v", err)
		}
	}()

	app.MustRegister(provisionUserTask)

	log.Println("worker listening on queue default")

	if err := app.RunWorker(runCtx, absurd.WorkerOptions{Concurrency: 4}); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
