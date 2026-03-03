use absurd::{Absurd, TaskOptions, WorkerOptions, SpawnOptions};
use serde_json::json;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://localhost/absurd".to_string());

    println!("=== Basic Transfer Example ===\n");

    let absurd = Absurd::with_queue(&database_url, "transfers").await?;
    absurd.create_queue(None).await?;

    // Register transfer task
    absurd.register_task(
        TaskOptions::new("process-transfer").with_queue("transfers"),
        |params, mut ctx| Box::pin(async move {
            let from_account = params["from_account"].as_str().unwrap();
            let to_account = params["to_account"].as_str().unwrap();
            let amount = params["amount"].as_f64().unwrap();

            println!("\n Processing transfer:");
            println!("  From: {}", from_account);
            println!("  To: {}", to_account);
            println!("  Amount: ${:.2}", amount);

            // Step 1: Validate accounts
            let validation = ctx.step("validate", || Box::pin(async move {
                println!("✓ Validating accounts...");
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                
                // Simulate validation
                Ok(json!({
                    "from_valid": true,
                    "to_valid": true,
                    "balance_sufficient": true
                }))
            })).await?;

            println!("  Validation: {}", validation);

            // Step 2: Debit from account
            let debit = ctx.step("debit", || {
                let amount = amount;
                let from = from_account.to_string();
                Box::pin(async move {
                    println!("💳 Debiting ${:.2} from {}", amount, from);
                    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
                    
                    // Use task_id for idempotency
                    let idempotency_key = format!("debit-{}", from);
                    
                    Ok(json!({
                        "debited": amount,
                        "new_balance": 900.0,
                        "idempotency_key": idempotency_key
                    }))
                })
            }).await?;

            println!("  Debit result: {}", debit);

            // Step 3: Credit to account
            let credit = ctx.step("credit", || {
                let amount = amount;
                let to = to_account.to_string();
                Box::pin(async move {
                    println!("💰 Crediting ${:.2} to {}", amount, to);
                    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
                    
                    Ok(json!({
                        "credited": amount,
                        "new_balance": 1100.0
                    }))
                })
            }).await?;

            println!("  Credit result: {}", credit);

            // Step 4: Send notification
            ctx.step("notify", || {
                let from = from_account.to_string();
                let to = to_account.to_string();
                // let amount = amount;
                Box::pin(async move {
                    println!("📧 Sending notification...");
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    
                    Ok(json!({
                        "notification_sent": true,
                        "recipients": [from, to]
                    }))
                })
            }).await?;

            println!("✅ Transfer complete!\n");

            Ok(json!({
                "status": "completed",
                "from": from_account,
                "to": to_account,
                "amount": amount,
                "debit": debit,
                "credit": credit
            }))
        })
    );

    // Start worker
    let worker = absurd.start_worker(
        WorkerOptions::new()
            .with_concurrency(2)
            .with_claim_timeout(60)
    ).await?;

    println!("Worker started");

    // Spawn multiple transfers
    for i in 1..=3 {
        let spawn_result = absurd.spawn(
            "process-transfer",
            json!({
                "from_account": format!("alice-{}", i),
                "to_account": format!("bob-{}", i),
                "amount": 100.0 * i as f64
            }),
            SpawnOptions {
                idempotency_key: Some(format!("transfer-{}", i)),
                ..Default::default()
            }
        ).await?;

        println!("Spawned transfer {} - Task ID: {}", i, spawn_result.task_id);
    }

    // Wait for completion
    println!("\nProcessing transfers...");
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    // Shutdown
    println!("\nShutting down...");
    worker.close().await?;

    println!("Done!");

    Ok(())
}
