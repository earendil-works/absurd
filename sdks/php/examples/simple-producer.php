<?php declare(strict_types=1);

/**
 * Simple producer example - spawns tasks for workers to process.
 *
 * Usage: php examples/simple-producer.php
 *
 * Environment:
 *   ABSURD_DATABASE_URL - PostgreSQL connection string (default: pgsql:host=localhost;dbname=absurd)
 */

require __DIR__ . '/../vendor/autoload.php';

use Absurd\Absurd;
use Symfony\Component\Dotenv\Dotenv;

new Dotenv()->bootEnv(dirname(__DIR__) . '/.env');

// Create PDO connection
$pdo = new PDO($_ENV['ABSURD_DATABASE_URL']);
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

// Create Absurd instance
$absurd = new Absurd($pdo);

// Ensure queue exists
echo "[" . date('c') . "] Creating queue...\n";
$absurd->createQueue();

// Spawn the task (must specify queue since task is not registered here)
echo "[" . date('c') . "] Spawning task...\n";
$result = $absurd->spawn('greet-user', [
    'userId' => '123',
    'name' => 'Alice'
], [
    'queue' => 'default'
]);

echo "[" . date('c') . "] Task spawned: {$result['taskID']}\n";
echo "[" . date('c') . "] Run a worker with: php examples/simple-worker.php\n";
