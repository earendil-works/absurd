<?php declare(strict_types=1);

/**
 * Simple worker example - processes tasks from the queue.
 *
 * Usage: php examples/simple-worker.php
 *
 * Environment:
 *   ABSURD_DATABASE_URL - PostgreSQL connection string (default: pgsql:host=localhost;dbname=absurd)
 */

require __DIR__ . '/../vendor/autoload.php';

use Absurd\Absurd;
use Symfony\Component\Dotenv\Dotenv;
use function Absurd\{step, sleepFor};

new Dotenv()->bootEnv(dirname(__DIR__) . '/.env');

// Create PDO connection
$pdo = new PDO($_ENV['ABSURD_DATABASE_URL']);
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

// Create Absurd instance
$absurd = new Absurd($pdo);

// Register the greet-user task
$absurd->registerTask('greet-user', function ($params, $ctx) {
    echo "[" . date('c') . "] Task started for user: {$params->userId}\n";

    // Step: Create greeting (cached on retry)
    $greeting = yield step('create-greeting', function () use ($params) {
        $message = "Hello, {$params->name}!";
        echo "[" . date('c') . "] Generated greeting: {$message}\n";
        return $message;
    });

    // Sleep for 10 seconds
    echo "[" . date('c') . "] Sleeping for 10 seconds...\n";
    yield sleepFor('wait-10-seconds', 10);

    // Step: Add timestamp
    $timestamped = yield step('add-timestamp', function () use ($greeting) {
        $result = $greeting . " (at " . date('H:i:s') . ")";
        echo "[" . date('c') . "] Added timestamp: {$result}\n";
        return $result;
    });

    echo "[" . date('c') . "] Task completed\n";
    return ['message' => $timestamped];
});

// Set up signal handling for graceful shutdown
$running = true;
pcntl_async_signals(true);
pcntl_signal(SIGTERM, function() use (&$running) {
    echo "[" . date('c') . "] Received SIGTERM, shutting down gracefully...\n";
    $running = false;
});
pcntl_signal(SIGINT, function() use (&$running) {
    echo "[" . date('c') . "] Received SIGINT (Ctrl+C), shutting down gracefully...\n";
    $running = false;
});

// Process tasks indefinitely until signaled to stop
echo "[" . date('c') . "] Starting worker (press Ctrl+C to stop)...\n";

while ($running) {
    try {
        // Claim and process tasks
        $tasks = $absurd->claimTasks([
            'batchSize' => 1,
            'claimTimeout' => 120,
            'workerId' => 'simple-worker'
        ]);

        if (!empty($tasks)) {
            foreach ($tasks as $task) {
                $absurd->executeTask($task, 120, true);
            }
        }
    } catch (\Throwable $e) {
        echo "[ERROR] {$e->getMessage()}\n";
    }
    usleep(100_000); // 100ms between polls
}

echo "[" . date('c') . "] Worker stopped!\n";
