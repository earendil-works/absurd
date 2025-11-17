package main

import (
	"context"
	"fmt"
	"log"

	"github.com/earendil-works/absurd/sdks/go/absurd"
)

type OrderParams struct {
	OrderID string  `json:"order_id"`
	Amount  float64 `json:"amount"`
}

type PaymentResult struct {
	TransactionID string `json:"transaction_id"`
}

type OrderResult struct {
	OrderID       string `json:"order_id"`
	TransactionID string `json:"transaction_id"`
	Status        string `json:"status"`
}

func main() {
	ctx := context.Background()

	abs, err := absurd.New(
		absurd.WithURI("postgres://localhost/absurd"),
		absurd.WithDefaultQueue("orders"),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Register task handler.
	err = absurd.Handle(
		abs,
		"process-order",
		absurd.TaskHandlerFunc[*OrderParams, *OrderResult](processOrder),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Spawn a task.
	result, err := absurd.Spawn(ctx, abs, "process-order", OrderParams{
		OrderID: "ORD-123",
		Amount:  99.99,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Spawned task: %s\n", result.TaskID)

	// Run worker.
	if err := absurd.Run(ctx, abs); err != nil {
		log.Printf("Worker stopped: %v", err)
	}
}

func processOrder(ctx absurd.Context, params *OrderParams) (*OrderResult, error) {
	// Step 1: Process payment (idempotent - won't re-execute on retry).
	payment, err := absurd.Step(ctx, "process-payment", func() (PaymentResult, error) {
		fmt.Println("Processing payment...")
		return PaymentResult{TransactionID: "TXN-" + params.OrderID}, nil
	})
	if err != nil {
		return nil, err
	}

	// Step 2: Reserve inventory.
	inventory, err := absurd.Step(ctx, "reserve-inventory", func() (string, error) {
		fmt.Println("Reserving inventory...")
		return "RES-" + params.OrderID, nil
	})
	if err != nil {
		return nil, err
	}

	// Step 3: Send confirmation.
	_, err = absurd.Step(ctx, "send-email", func() (string, error) {
		fmt.Printf("Order %s completed: %s, %s\n", params.OrderID, payment.TransactionID, inventory)
		return "EMAIL-" + params.OrderID, nil
	})
	if err != nil {
		return nil, err
	}

	return &OrderResult{
		OrderID:       params.OrderID,
		TransactionID: payment.TransactionID,
		Status:        "completed",
	}, nil
}
