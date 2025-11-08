<?php declare(strict_types=1);

/**
 * Customer provisioning workflow example using the Absurd PHP SDK.
 *
 * This demonstrates a multi-step workflow with:
 * - Automatic checkpointing
 * - Child task spawning
 * - Event-driven coordination
 * - Timeout handling
 * - Simulated failures with retries
 *
 * Usage: php examples/provisioning.php
 *
 * Environment:
 *   ABSURD_DATABASE_URL - PostgreSQL connection string (default: pgsql:host=localhost;dbname=absurd)
 *   ABSURD_WORKERS      - Number of worker concurrency (default: 6)
 *   ABSURD_RUNTIME      - Worker lifetime in seconds (default: 15)
 */

require __DIR__ . '/../vendor/autoload.php';

use Absurd\{Absurd, TimeoutError};
use Symfony\Component\Dotenv\Dotenv;
use function Absurd\{step, awaitEvent, sleepFor, emitEvent, spawn};

new Dotenv()->bootEnv(dirname(__DIR__) . '/.env');

function logMessage(string $scope, string $message, array $context = []): void
{
    $contextStr = $context ? ' ' . json_encode($context) : '';
    echo "[" . date('c') . "] [{$scope}] {$message}{$contextStr}\n";
}

const DEFAULT_CANCELLATION = [
    'maxDelay' => 60,
    'maxDuration' => 120,
];
const ACTIVATION_TIMEOUT = 5;
const ON_TIME_DELAY_RANGE = ['min' => 1, 'max' => 4];
const LATE_DELAY_RANGE = ['min' => ACTIVATION_TIMEOUT + 2, 'max' => ACTIVATION_TIMEOUT + 3];

function registerTasks(Absurd $absurd): void
{
    // Main provisioning workflow
    $absurd->registerTask('provision-customer', function ($params, $ctx) use ($absurd) {
        // Create customer record
        $customer = yield step('create-customer-record', function () use ($params) {
            $createdAt = date('c');
            logMessage('provision', 'customer record created', [
                'customerId' => $params->customerId,
                'email' => $params->email,
                'plan' => $params->plan,
                'createdAt' => $createdAt,
            ]);
            return (object)['id' => $params->customerId, 'createdAt' => $createdAt];
        });

        // Queue activation email
        yield step('queue-activation-email', function () use ($absurd, $customer, $params) {
            logMessage('provision', 'queueing activation email task', [
                'customerId' => $customer->id,
            ]);
            $absurd->spawn('send-activation-email', [
                'customerId' => $customer->id,
                'email' => $params->email,
            ], ['maxAttempts' => 4]);
            return true;
        });

        // Start activation simulator
        $shouldTimeout = mt_rand(0, 1) === 1;
        $delayRange = $shouldTimeout ? LATE_DELAY_RANGE : ON_TIME_DELAY_RANGE;

        yield step('start-activation-simulator', function () use ($absurd, $customer, $shouldTimeout, $delayRange) {
            logMessage('provision', 'starting activation simulator', [
                'customerId' => $customer->id,
                'shouldTimeout' => $shouldTimeout,
                'delayRange' => $delayRange,
                'timeout' => ACTIVATION_TIMEOUT,
            ]);

            $absurd->spawn('activation-simulator', [
                'customerId' => $customer->id,
                'minDelay' => $delayRange['min'],
                'maxDelay' => $delayRange['max'],
                'timeout' => ACTIVATION_TIMEOUT,
                'shouldTimeout' => $shouldTimeout,
            ], ['maxAttempts' => 1]);

            return true;
        });

        // Wait for activation event
        $eventName = "customer:{$customer->id}:activated";
        logMessage('provision', 'waiting for activation event', [
            'customerId' => $customer->id,
            'eventName' => $eventName,
            'timeout' => ACTIVATION_TIMEOUT,
        ]);

        try {
            $activationPayload = yield awaitEvent($eventName, ['timeout' => ACTIVATION_TIMEOUT]);
        } catch (TimeoutError) {
            $timedOutAt = date('c');
            logMessage('provision', 'activation wait timed out', [
                'customerId' => $customer->id,
                'timedOutAt' => $timedOutAt,
                'timeout' => ACTIVATION_TIMEOUT,
                'expectedTimeout' => $shouldTimeout,
            ]);

            logMessage('provision', 'customer provisioning stalled', [
                'customerId' => $customer->id,
                'status' => 'activation-timeout',
                'timedOutAt' => $timedOutAt,
            ]);

            return (object)[
                'customerId' => $customer->id,
                'status' => 'activation-timeout',
                'timedOutAt' => $timedOutAt,
            ];
        }

        // Finalize activation
        $activatedAt = yield step('finalize-activation', function () use ($customer, $activationPayload) {
            $activationTime = $activationPayload['activatedAt'] ?? null;
            logMessage('provision', 'activation confirmed', [
                'customerId' => $customer->id,
                'activatedAt' => $activationTime,
            ]);
            return $activationTime;
        });

        // Queue audit log
        yield step('queue-audit-trail', function () use ($absurd, $customer, $activatedAt) {
            logMessage('provision', 'queueing audit trail task', [
                'customerId' => $customer->id,
            ]);
            $absurd->spawn('post-activation-audit', [
                'customerId' => $customer->id,
                'activatedAt' => $activatedAt,
            ], ['maxAttempts' => 1]);
            return true;
        });

        logMessage('provision', 'customer provisioning complete', [
            'customerId' => $customer->id,
            'status' => 'activated',
        ]);

        return [
            'customerId' => $customer->id,
            'status' => 'activated',
            'activatedAt' => $activatedAt,
        ];
    }, ['defaultCancellation' => DEFAULT_CANCELLATION]);

    // Send activation email task (with simulated failures)
    $simulatedFailureTracker = [];
    $absurd->registerTask('send-activation-email', function ($params, $ctx) use (&$simulatedFailureTracker) {
        $attempt = ($simulatedFailureTracker[$params->customerId] ?? 0) + 1;
        $simulatedFailureTracker[$params->customerId] = $attempt;

        logMessage('send-email', 'attempting to send activation email', [
            'customerId' => $params->customerId,
            'email' => $params->email,
            'attempt' => $attempt,
        ]);

        // Roll for failure
        $failChance = yield step('roll-for-failure', fn() => mt_rand(1, 100) / 100);

        if ($failChance < 0.45 && $attempt <= 2) {
            logMessage('send-email', 'simulated transient failure', [
                'customerId' => $params->customerId,
                'attempt' => $attempt,
                'failChance' => $failChance,
            ]);
            throw new \RuntimeException('Simulated email provider outage');
        }

        // Generate message ID
        $messageId = yield step('generate-message-id', function () use ($params) {
            $id = 'msg_' . bin2hex(random_bytes(3));
            logMessage('send-email', 'rendered activation email', [
                'customerId' => $params->customerId,
                'messageId' => $id,
            ]);
            return $id;
        });

        logMessage('send-email', 'activation email delivered', [
            'customerId' => $params->customerId,
            'messageId' => $messageId,
        ]);

        return ['messageId' => $messageId];
    }, ['defaultCancellation' => DEFAULT_CANCELLATION]);

    // Activation simulator task
    $absurd->registerTask('activation-simulator', function ($params, $ctx) {
        // Calculate delay
        $delay = yield step('calculate-activation-delay', function () use ($params) {
            $range = max($params->maxDelay - $params->minDelay, 0);
            $delay = $params->minDelay + ($range > 0 ? mt_rand(0, $range) : 0);
            logMessage('activation', 'user will eventually activate', [
                'customerId' => $params->customerId,
                'delay' => $delay,
                'shouldTimeout' => $params->shouldTimeout,
                'timeout' => $params->timeout,
            ]);
            return $delay;
        });

        // Calculate wake time
        $wakeAtIso = yield step('calculate-wake-time', function () use ($delay, $params) {
            $wake = date('c', time() + $delay);
            logMessage('activation', 'scheduled wake for activation', [
                'customerId' => $params->customerId,
                'wakeAt' => $wake,
            ]);
            return $wake;
        });

        $wakeAt = strtotime($wakeAtIso);
        $remaining = $wakeAt - time();

        if ($remaining > 0) {
            logMessage('activation', 'waiting for simulated user activation', [
                'customerId' => $params->customerId,
                'remaining' => $remaining,
            ]);
            yield sleepFor('wait-for-activation', $remaining);
        }

        // Emit activation event
        yield step('prepare-activation-event', function () use ($params) {
            logMessage('activation', 'emitting activation event', [
                'customerId' => $params->customerId,
                'shouldTimeout' => $params->shouldTimeout,
            ]);
            return true;
        });

        yield emitEvent("customer:{$params->customerId}:activated", [
            'customerId' => $params->customerId,
            'activatedAt' => date('c'),
        ]);

        logMessage('activation', 'activation event dispatched', [
            'customerId' => $params->customerId,
        ]);
    }, ['defaultCancellation' => DEFAULT_CANCELLATION]);

    // Post-activation audit task
    $absurd->registerTask('post-activation-audit', function ($params, $ctx) {
        $auditEntry = yield step('record-audit-trail', function () use ($params) {
            $recordedAt = date('c');
            logMessage('audit', 'recorded activation trail', [
                'customerId' => $params->customerId,
                'activatedAt' => $params->activatedAt,
                'recordedAt' => $recordedAt,
            ]);
            return ['recordedAt' => $recordedAt];
        });

        logMessage('audit', 'audit complete', [
            'customerId' => $params->customerId,
            'recordedAt' => $auditEntry['recordedAt'],
        ]);

        return $auditEntry;
    }, ['defaultCancellation' => DEFAULT_CANCELLATION]);
}

function main(): void
{
    $workerCount = (int)($_ENV['ABSURD_WORKERS'] ?? 6);
    $runtime = (int)($_ENV['ABSURD_RUNTIME'] ?? 15);

    logMessage('main', 'connecting to absurd queue', ['workers' => $workerCount]);

    // Create PDO connection
    $pdo = new PDO($_ENV['ABSURD_DATABASE_URL']);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    $absurd = new Absurd($pdo);

    logMessage('main', 'creating queue if not exists');
    $absurd->createQueue();

    registerTasks($absurd);

    // Generate and spawn tasks
    $taskCount = 12;
    for ($i = 0; $i < $taskCount; $i++) {
        $params = [
            'customerId' => sprintf('cust-%04d', $i),
            'email' => sprintf('customer%d@example.com', $i),
            'plan' => mt_rand(0, 1) ? 'pro' : 'basic',
        ];

        $result = $absurd->spawn('provision-customer', $params, ['maxAttempts' => 3]);
        logMessage('main', 'spawned provisioning workflow', [
            'taskId' => $result['taskID'],
            'runId' => $result['runID'],
            'customerId' => $params['customerId'],
        ]);
    }

    logMessage('main', 'worker running', ['runtime' => $runtime, 'concurrency' => $workerCount]);

    // Run the worker loop with timeout
    $startTime = time();
    $emptyPollCount = 0;
    $maxEmptyPolls = 10; // Exit after 10 consecutive empty polls (1 second)

    while (time() - $startTime < $runtime) {
        try {
            // Process a batch of tasks
            $tasks = $absurd->claimTasks([
                'batchSize' => $workerCount,
                'claimTimeout' => 120,
                'workerId' => 'demo-worker'
            ]);

            if (empty($tasks)) {
                $emptyPollCount++;
                if ($emptyPollCount >= $maxEmptyPolls) {
                    logMessage('main', 'no more tasks to process, shutting down early');
                    break;
                }
            } else {
                $emptyPollCount = 0;
                foreach ($tasks as $task) {
                    $absurd->executeTask($task, 120, true);
                }
            }
        } catch (\Throwable $e) {
            logMessage('worker', 'worker error', ['error' => $e->getMessage()]);
        }
        usleep(100_000); // 100ms between batches
    }

    logMessage('main', 'shutting down worker');

    $absurd->close();
    logMessage('main', 'example finished');
}

try {
    main();
} catch (\Throwable $e) {
    logMessage('main', 'example failed', ['error' => $e->getMessage()]);
    echo $e . "\n";
    exit(1);
}
