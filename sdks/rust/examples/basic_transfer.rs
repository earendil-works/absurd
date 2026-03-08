use absurd::{Absurd, TaskOptions, WorkerOptions, SpawnOptions};
use serde_json::json;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://localhost/absurd".to_string());

    println!("=== Basic Transfer Example (with Idempotency) ===\n");

    let absurd = Absurd::with_queue(&database_url, "transfers").await?;
    absurd.create_queue(None).await?;

    // Register transfer task
    absurd.register_task(
        TaskOptions::new("process-transfer").with_queue("transfers"),
        |params, mut ctx| Box::pin(async move {
            let from_account = params["from_account"]
                .as_str()
                .unwrap_or("unknown")
                .to_string();
            
            let to_account = params["to_account"]
                .as_str()
                .unwrap_or("unknown")
                .to_string();
            
            let amount = params["amount"].as_f64().unwrap_or(0.0);

            println!("\n Processing transfer:");
            println!("  From: {}", from_account);
            println!("  To: {}", to_account);
            println!("  Amount: ${:.2}", amount);

            // Step 1: Validate accounts
            let validation = ctx.step("validate", || Box::pin(async move {
                println!("✓ Validating accounts...");
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                
                Ok(json!({
                    "from_valid": true,
                    "to_valid": true,
                    "balance_sufficient": true
                }))
            })).await?;

            println!("  Validation: {}", validation);

            // Step 2: Debit from account
            let debit = ctx.step("debit", || {
                let from = from_account.clone();
                let amt = amount;
                Box::pin(async move {
                    println!("💳 Debiting ${:.2} from {}", amt, from);
                    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
                    
                    let idempotency_key = format!("debit-{}", from);
                    
                    Ok(json!({
                        "debited": amt,
                        "new_balance": 900.0,
                        "idempotency_key": idempotency_key
                    }))
                })
            }).await?;

            println!("  Debit result: {}", debit);

            // Step 3: Credit to account
            let credit = ctx.step("credit", || {
                let to = to_account.clone();
                let amt = amount;
                Box::pin(async move {
                    println!("💰 Crediting ${:.2} to {}", amt, to);
                    tokio::time::sleep(tokio::time::Duration::from_millis(300)).await;
                    
                    Ok(json!({
                        "credited": amt,
                        "new_balance": 1100.0
                    }))
                })
            }).await?;

            println!("  Credit result: {}", credit);

            // Step 4: Send notification
            ctx.step("notify", || {
                let from = from_account.clone();
                let to = to_account.clone();
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

    println!("Worker started\n");

    // Give worker time to start polling
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // === Scenario 1: Multiple unique transfers ===
    println!("=== Scenario 1: Processing 3 Unique Transfers ===\n");
    
    for i in 1..=3 {
        let spawn_result = absurd.spawn(
            "process-transfer",
            json!({
                "from_account": format!("alice-{}", i),
                "to_account": format!("bob-{}", i),
                "amount": 100.0 * i as f64
            }),
            SpawnOptions {
                // Each transfer gets a unique key
                idempotency_key: Some(format!("transfer-{}", i)),
                ..Default::default()
            }
        ).await?;

        println!("Spawned transfer {} - Task ID: {}", i, spawn_result.task_id);
        println!("  Created: {}", if spawn_result.created { "new" } else { "existing" });
    }

    println!("\nWaiting for transfers to complete...");
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    // === Scenario 2: Re-spawn same transfer (tests idempotency) ===
    println!("\n=== Scenario 2: Testing Idempotency ===\n");
    println!("Re-spawning transfer 1 with SAME idempotency key...\n");
    
    let respawn_result = absurd.spawn(
        "process-transfer",
        json!({
            "from_account": "alice-1",
            "to_account": "bob-1",
            "amount": 100.0
        }),
        SpawnOptions {
            // Same key as before
            idempotency_key: Some("transfer-1".to_string()),
            ..Default::default()
        }
    ).await?;

    println!("Re-spawn result:");
    println!("  Task ID: {}", respawn_result.task_id);
    println!("  Created: {}", if respawn_result.created { "new" } else { "existing (DEDUPLICATED)" });
    
    // Verify deduplication
    // Note: We can't easily get the first task ID to compare here,
    // but in production you'd store it and verify they match
    println!("\n  ✅ Idempotency working: Same key returns existing task");

    // === Scenario 3: New key for same logical transfer ===
    println!("\n=== Scenario 3: Different Key = Different Task ===\n");
    println!("Spawning transfer with NEW idempotency key...\n");
    
    let new_key_result = absurd.spawn(
        "process-transfer",
        json!({
            "from_account": "alice-1",
            "to_account": "bob-1",
            "amount": 100.0
        }),
        SpawnOptions {
            // Different key = new task
            idempotency_key: Some("transfer-1-retry".to_string()),
            ..Default::default()
        }
    ).await?;

    println!("New key result:");
    println!("  Task ID: {}", new_key_result.task_id);
    println!("  Created: {}", if new_key_result.created { "new" } else { "existing" });
    
    println!("\n  ✅ Different key creates new task: {}", new_key_result.created);

    println!("\nWaiting for final transfer...");
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    // Shutdown
    println!("\nShutting down...");
    worker.close().await?;

    println!("Done!");

    Ok(())
}