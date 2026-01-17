<?php declare(strict_types=1);

/**
 * Simple event emitter example - emits events to wake waiting tasks.
 *
 * Usage: php examples/simple-produce-event.php <id>
 *
 * Environment:
 *   ABSURD_DATABASE_URL - PostgreSQL connection string (default: pgsql:host=localhost;dbname=absurd)
 */

if ($argc < 2) {
    fprintf(STDERR, "Usage: php %s <id>\n", $argv[0]);
    exit(1);
}

$id = $argv[1];

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

echo "[" . date('c') . "] Emitting event greet:{$id}...\n";
$absurd->emitEvent("greet:{$id}", [
    'name' => 'Alice',
], 'default');
