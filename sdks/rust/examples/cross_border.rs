use absurd::{Absurd, AwaitEventOptions, TaskOptions, WorkerOptions};
use serde_json::json;
use std::env;
use tokio::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://localhost/absurd".to_string());

    println!("=== Cross-Border Transfer Example ===\n");

    let absurd = Absurd::with_queue(&database_url, "international").await?;
    absurd.create_queue(None).await?;

    // Register cross-border transfer task
    absurd.register_task(
        TaskOptions::new("cross-border-transfer")
            .with_queue("international"),
        |params, mut ctx| Box::pin(async move {
            let transfer_id = params["transfer_id"].as_str().unwrap();
            let from_country = params["from_country"].as_str().unwrap();
            let to_country = params["to_country"].as_str().unwrap();
            let amount = params["amount"].as_f64().unwrap();

            println!("\n🌍 Cross-border transfer initiated:");
            println!("  Transfer ID: {}", transfer_id);
            println!("  {} → {}", from_country, to_country);
            println!("  Amount: ${:.2}", amount);

            // Step 1: KYC verification
            ctx.step("kyc-check", || Box::pin(async {
                println!("🔍 Running KYC checks...");
                tokio::time::sleep(Duration::from_millis(500)).await;
                println!("  ✓ Sender verified");
                println!("  ✓ Beneficiary verified");
                Ok(json!({ "kyc_passed": true }))
            })).await?;

            // Step 2: Sanctions screening
            ctx.step("sanctions", || Box::pin(async {
                println!("⚖️  Sanctions screening...");
                tokio::time::sleep(Duration::from_millis(300)).await;
                println!("  ✓ No sanctions matches");
                Ok(json!({ "sanctions_clear": true }))
            })).await?;

            // Step 3: AML check
            ctx.step("aml-check", || Box::pin(async {
                println!("🛡️  AML threshold check...");
                tokio::time::sleep(Duration::from_millis(200)).await;
                println!("  ✓ Within limits");
                Ok(json!({ "aml_passed": true }))
            })).await?;

            // Step 4: Debit sender
            ctx.step("debit-sender", || {
                let amount = amount;
                Box::pin(async move {
                    println!("💳 Debiting sender...");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    println!("  ✓ ${:.2} debited", amount);
                    Ok(json!({ "debited": amount }))
                })
            }).await?;

            // Step 5: Create SWIFT message
            let swift_msg = ctx.step("create-swift", || {
                let tid = transfer_id.to_string();
                Box::pin(async move {
                    println!("📝 Creating SWIFT message...");
                    tokio::time::sleep(Duration::from_millis(300)).await;
                    let msg_id = format!("SWIFT-{}", tid);
                    println!("  ✓ SWIFT message: {}", msg_id);
                    Ok(json!({ "swift_id": msg_id }))
                })
            }).await?;

            println!("\n⏳ Waiting for settlement confirmation...");
            println!("   (In real world, this could take 24-48 hours)");
            println!("   Simulating 3 second wait...");

            // Step 6: Wait for settlement event
            // In production, this would be triggered by external system
            let settlement_event = format!("settlement.confirmed:{}", transfer_id);
            
            // Note: In production, this event would be emitted by an external system
            // For demo purposes, we'll emit it after task is waiting

            // Wait for the event
            let settlement = ctx.await_event(
                &settlement_event,
                AwaitEventOptions::new()
                    .with_timeout(172800) // 48 hours in production
            ).await?;

            println!("✅ Settlement confirmed!");
            println!("   Details: {}", settlement);

            // Step 7: Credit beneficiary
            ctx.step("credit-beneficiary", || {
                let amount = amount;
                Box::pin(async move {
                    println!("💰 Crediting beneficiary...");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    println!("  ✓ ${:.2} credited", amount);
                    Ok(json!({ "credited": amount }))
                })
            }).await?;

            // Step 8: Send notifications
            ctx.step("notify", || Box::pin(async {
                println!("📧 Sending notifications...");
                tokio::time::sleep(Duration::from_millis(100)).await;
                println!("  ✓ Sender notified");
                println!("  ✓ Beneficiary notified");
                Ok(json!({ "notifications_sent": true }))
            })).await?;

            // Emit completion event
            ctx.emit_event(
                &format!("transfer.completed:{}", transfer_id),
                Some(json!({
                    "transfer_id": transfer_id,
                    "amount": amount,
                    "swift": swift_msg,
                    "settlement": settlement
                }))
            ).await?;

            println!("\n✨ Cross-border transfer complete!\n");

            Ok(json!({
                "status": "completed",
                "transfer_id": transfer_id,
                "amount": amount,
                "swift_message": swift_msg,
                "settlement": settlement
            }))
        })
    );

    // Start worker
    let worker = absurd.start_worker(
        WorkerOptions::new()
            .with_concurrency(1)
            .with_claim_timeout(120)
    ).await?;

    println!("Worker started\n");

    // Spawn transfer
    let spawn_result = absurd.spawn(
        "cross-border-transfer",
        json!({
            "transfer_id": "XB-2024-001",
            "from_country": "US",
            "to_country": "UK",
            "amount": 5000.0,
            "sender": {
                "name": "Alice Smith",
                "account": "US1234567890"
            },
            "beneficiary": {
                "name": "Bob Johnson",
                "account": "GB9876543210"
            }
        }),
        Default::default()
    ).await?;

    println!("Transfer spawned - Task ID: {}\n", spawn_result.task_id);

    // Simulate external system emitting settlement event after delay
    let absurd_clone = absurd.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(3)).await;
        println!("\n🔔 External system confirming settlement...");
        let _ = absurd_clone.emit_event(
            "settlement.confirmed:XB-2024-001",
            Some(json!({
                "confirmation_code": "ABC123XYZ",
                "settled_at": "2024-01-15T10:30:00Z"
            })),
            Some("international")
        ).await;
    });

    // Wait for completion
    println!("Processing transfer...");
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Shutdown
    println!("\nShutting down...");
    worker.close().await?;

    println!("Done!");

    Ok(())
}
