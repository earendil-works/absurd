<?php declare(strict_types=1);

/**
 * Simple example demonstrating basic Absurd usage.
 *
 * Usage: php examples/simple.php
 *
 * Environment:
 *   ABSURD_DATABASE_URL - PostgreSQL connection string (default: pgsql:host=localhost;dbname=absurd)
 */

require __DIR__ . '/../vendor/autoload.php';

use Absurd\Absurd;
use Symfony\Component\Dotenv\Dotenv;
use function Absurd\{step, awaitEvent, sleepFor, emitEvent};

// Load environment variables
(new Dotenv())->bootEnv(dirname(__DIR__) . '/.env');

// Create PDO connection
$pdo = new PDO($_ENV['ABSURD_DATABASE_URL']);
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

// Create Absurd instance
$absurd = new Absurd($pdo);

// Ensure queue exists
echo "[" . date('c') . "] Creating queue...\n";
$absurd->createQueue();

// Register a simple task
$absurd->registerTask('greet-user', function ($params, $ctx) {
    echo "[" . date('c') . "] Task started for user: {$params->userId}\n";

    // Step: Create greeting (cached on retry)
    $greeting = yield step('create-greeting', function () use ($params) {
        $message = "Hello, {$params->name}!";
        echo "[" . date('c') . "] Generated greeting: {$message}\n";
        return $message;
    });

    // Sleep for 2 seconds
    echo "[" . date('c') . "] Sleeping for 2 seconds...\n";
    yield sleepFor('wait-2-seconds', 2);

    // Step: Add timestamp
    $timestamped = yield step('add-timestamp', function () use ($greeting) {
        $result = $greeting . " (at " . date('H:i:s') . ")";
        echo "[" . date('c') . "] Added timestamp: {$result}\n";
        return $result;
    });

    echo "[" . date('c') . "] Task completed\n";
    return ['message' => $timestamped];
});

// Spawn the task
echo "[" . date('c') . "] Spawning task...\n";
$result = $absurd->spawn('greet-user', [
    'userId' => '123',
    'name' => 'Alice'
]);

echo "[" . date('c') . "] Task spawned: {$result['taskID']}\n";

// Process tasks with a polling loop
echo "[" . date('c') . "] Starting worker...\n";
$startTime = time();
$emptyPollCount = 0;
$maxEmptyPolls = 50; // Exit after 50 consecutive empty polls (5 seconds)

while (time() - $startTime < 10) {
    try {
        // Claim and process tasks
        $tasks = $absurd->claimTasks([
            'batchSize' => 1,
            'claimTimeout' => 120,
            'workerId' => 'simple-worker'
        ]);

        if (empty($tasks)) {
            $emptyPollCount++;
            if ($emptyPollCount >= $maxEmptyPolls) {
                echo "[" . date('c') . "] No more tasks to process\n";
                break;
            }
        } else {
            $emptyPollCount = 0;
            foreach ($tasks as $task) {
                $absurd->executeTask($task, 120, true);
            }
        }
    } catch (\Throwable $e) {
        echo "[ERROR] {$e->getMessage()}\n";
    }
    usleep(100_000); // 100ms between polls
}

echo "[" . date('c') . "] Example complete!\n";
