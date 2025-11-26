<?php declare(strict_types=1);

namespace Absurd;

use Generator;
use PDO;
use Closure;

final class Absurd
{
    private string $queueName;

    /** @var array<string, array{name: string, queue: string, handler: Closure, defaultMaxAttempts?: int, defaultCancellation?: array}> */
    private array $registry = [];

    public function __construct(
        private readonly PDO $pdo,
        private readonly string $defaultQueueName = 'default',
        private readonly int $defaultMaxAttempts = 5
    ) {
        $this->queueName = $this->defaultQueueName;
    }

    /** @param array{queue?: string, defaultMaxAttempts?: int, defaultCancellation?: array} $options */
    public function registerTask(string $name, Closure $handler, array $options = []): void
    {
        if (empty($name)) {
            throw new TaskExecutionError('Task registration requires a non-empty name');
        }

        $queue = $options['queue'] ?? $this->defaultQueueName;

        if (empty($queue)) {
            throw new TaskExecutionError(
                "Task \"{$name}\" must specify a queue or use a client with a default queue"
            );
        }

        if (isset($options['defaultMaxAttempts']) && $options['defaultMaxAttempts'] < 1) {
            throw new TaskExecutionError('defaultMaxAttempts must be at least 1');
        }

        $this->registry[$name] = [
            'name' => $name,
            'queue' => $queue,
            'handler' => $handler,
            'defaultMaxAttempts' => $options['defaultMaxAttempts'] ?? null,
            'defaultCancellation' => $options['defaultCancellation'] ?? null,
        ];
    }

    /**
     * @param array{maxAttempts?: int, retryStrategy?: array, headers?: array, queue?: string, cancellation?: array} $options
     * @return array{taskID: string, runID: string, attempt: int}
     */
    public function spawn(string $taskName, mixed $params, array $options = []): array
    {
        $registration = $this->registry[$taskName] ?? null;
        $queue = $options['queue'] ?? $registration['queue'] ?? null;

        if ($queue === null) {
            throw new TaskExecutionError(
                "Task \"{$taskName}\" is not registered. Provide options['queue'] when spawning unregistered tasks."
            );
        }

        if ($registration && isset($options['queue']) && $options['queue'] !== $registration['queue']) {
            throw new TaskExecutionError(
                "Task \"{$taskName}\" is registered for queue \"{$registration['queue']}\" but spawn requested queue \"{$options['queue']}\"."
            );
        }

        $effectiveMaxAttempts = $options['maxAttempts']
            ?? $registration['defaultMaxAttempts']
            ?? $this->defaultMaxAttempts;

        $effectiveCancellation = $options['cancellation']
            ?? $registration['defaultCancellation']
            ?? null;

        $normalizedOptions = $this->normalizeSpawnOptions([
            ...$options,
            'maxAttempts' => $effectiveMaxAttempts,
            'cancellation' => $effectiveCancellation,
        ]);

        $stmt = $this->pdo->prepare(
            'SELECT task_id, run_id, attempt FROM absurd.spawn_task(?, ?, ?, ?)'
        );
        $stmt->execute([
            $queue,
            $taskName,
            json_encode($params, JSON_THROW_ON_ERROR),
            json_encode($normalizedOptions, JSON_THROW_ON_ERROR)
        ]);

        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        if (!$row) {
            throw new TaskExecutionError('Failed to spawn task');
        }

        return [
            'taskID' => $row['task_id'],
            'runID' => $row['run_id'],
            'attempt' => (int)$row['attempt']
        ];
    }

    public function emitEvent(string $eventName, mixed $payload = null, ?string $queueName = null): void
    {
        if (empty($eventName)) {
            throw new TaskExecutionError('eventName must be a non-empty string');
        }

        $stmt = $this->pdo->prepare('SELECT absurd.emit_event(?, ?, ?)');
        $stmt->execute([
            $queueName ?? $this->queueName,
            $eventName,
            json_encode($payload, JSON_THROW_ON_ERROR)
        ]);
    }

    /**
     * @param array{batchSize?: int, claimTimeout?: int, workerId?: string} $options
     * @return array<object>
     */
    public function claimTasks(array $options = []): array
    {
        $batchSize = $options['batchSize'] ?? 1;
        $claimTimeout = $options['claimTimeout'] ?? 120;
        $workerId = $options['workerId'] ?? 'worker';

        $stmt = $this->pdo->prepare(
            'SELECT run_id, task_id, attempt, task_name, params, retry_strategy, max_attempts,
                    headers, wake_event, event_payload
             FROM absurd.claim_task(?, ?, ?, ?)'
        );
        $stmt->execute([$this->queueName, $workerId, $claimTimeout, $batchSize]);

        $tasks = [];
        while ($row = $stmt->fetch(PDO::FETCH_OBJ)) {
            $row->params = json_decode($row->params, false, 512, JSON_THROW_ON_ERROR);
            if ($row->headers !== null) {
                $row->headers = json_decode($row->headers, false, 512, JSON_THROW_ON_ERROR);
            }
            if ($row->event_payload !== null) {
                $row->event_payload = json_decode($row->event_payload, false, 512, JSON_THROW_ON_ERROR);
            }
            $tasks[] = $row;
        }

        return $tasks;
    }

    public function workBatch(string $workerId = 'worker', int $claimTimeout = 120, int $batchSize = 1): void
    {
        $tasks = $this->claimTasks([
            'batchSize' => $batchSize,
            'claimTimeout' => $claimTimeout,
            'workerId' => $workerId
        ]);

        foreach ($tasks as $task) {
            $this->executeTask($task, $claimTimeout);
        }
    }

    /** @param array{workerId?: string, claimTimeout?: int, batchSize?: int, concurrency?: int, pollInterval?: float, onError?: callable, fatalOnLeaseTimeout?: bool} $options */
    public function startWorker(array $options = []): Worker
    {
        $workerId = $options['workerId'] ?? gethostname() . ':' . getmypid();
        $claimTimeout = $options['claimTimeout'] ?? 120;
        $concurrency = $options['concurrency'] ?? 1;
        $batchSize = $options['batchSize'] ?? $concurrency;
        $pollInterval = $options['pollInterval'] ?? 0.25;
        $onError = $options['onError'] ?? fn(\Throwable $e) => error_log('Worker error: ' . $e->getMessage());
        $fatalOnLeaseTimeout = $options['fatalOnLeaseTimeout'] ?? true;

        return new Worker(
            $this,
            $workerId,
            $claimTimeout,
            $batchSize,
            $concurrency,
            $pollInterval,
            $onError,
            $fatalOnLeaseTimeout
        );
    }

    public function createQueue(?string $queueName = null): void
    {
        $queue = $queueName ?? $this->queueName;
        $stmt = $this->pdo->prepare('SELECT absurd.create_queue(?)');
        $stmt->execute([$queue]);
    }

    public function dropQueue(?string $queueName = null): void
    {
        $queue = $queueName ?? $this->queueName;
        $stmt = $this->pdo->prepare('SELECT absurd.drop_queue(?)');
        $stmt->execute([$queue]);
    }

    /** @return array<string> */
    public function listQueues(): array
    {
        $stmt = $this->pdo->query('SELECT * FROM absurd.list_queues()');
        $queues = [];
        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            $queues[] = $row['queue_name'];
        }
        return $queues;
    }

    public function close(): void
    {
        // PDO connections are closed automatically when no references remain
        // No explicit cleanup needed for unmanaged PDO instances
    }

    public function executeTask(object $task, int $claimTimeout, bool $fatalOnLeaseTimeout = true): void
    {
        $registration = $this->registry[$task->task_name] ?? null;

        if (!$registration) {
            throw new TaskExecutionError("Unknown task: {$task->task_name}");
        }

        if ($registration['queue'] !== $this->queueName) {
            throw new TaskExecutionError("Misconfigured task (queue mismatch)");
        }

        $ctx = TaskContext::create(
            $this->pdo,
            $task->task_id,
            $registration['queue'],
            $task,
            $claimTimeout
        );

        // Set up lease timeout warnings
        $warnTime = null;
        $fatalTime = null;
        if ($claimTimeout > 0) {
            $warnTime = microtime(true) + $claimTimeout;
            if ($fatalOnLeaseTimeout) {
                $fatalTime = microtime(true) + ($claimTimeout * 2);
            }
        }

        try {
            // Call the handler - it must return a Generator
            $generator = ($registration['handler'])($task->params, $ctx);

            if (!$generator instanceof Generator) {
                throw new TaskExecutionError(
                    "Task handler for \"{$task->task_name}\" must return a Generator"
                );
            }

            // Execute the generator
            $result = $this->executeGenerator($generator, $ctx, $warnTime, $fatalTime, $task, $claimTimeout);

            // Complete the run
            $ctx->complete($result);

        } catch (SuspendTask) {
            // Task suspended (sleep or await), don't complete or fail
            return;
        } catch (\Throwable $e) {
            error_log("Task {$task->task_name} ({$task->task_id}) failed: {$e->getMessage()}");
            $ctx->fail($e);
        }
    }

    private function executeGenerator(
        Generator $gen,
        TaskContext $ctx,
        ?float $warnTime,
        ?float $fatalTime,
        object $task,
        int $claimTimeout
    ): mixed {
        // Start the generator
        $gen->current();

        while ($gen->valid()) {
            // Check lease timeout warnings
            if ($warnTime !== null && microtime(true) > $warnTime) {
                error_log("Task {$task->task_name} ({$task->task_id}) exceeded claim timeout of {$claimTimeout}s");
                $warnTime = null; // Only warn once
            }
            if ($fatalTime !== null && microtime(true) > $fatalTime) {
                error_log("Task {$task->task_name} ({$task->task_id}) exceeded claim timeout by 100%; terminating process");
                exit(1);
            }

            $command = $gen->current();

            // Process the command
            $value = null;
            match (true) {
                $command instanceof Checkpoint => $value = $ctx->executeCheckpoint($command->name, $command->value),
                $command instanceof AwaitEvent => $value = $ctx->executeAwaitEvent($command->eventName, $command->options),
                $command instanceof SleepFor => $ctx->executeSleepFor($command->stepName, $command->duration),
                $command instanceof SleepUntil => $ctx->executeSleepUntil($command->stepName, $command->wakeAt),
                $command instanceof EmitEvent => $ctx->executeEmitEvent($command->eventName, $command->payload),
                $command instanceof SpawnTask => $value = $this->executeSpawnTask($command),
                default => throw new TaskExecutionError('Unknown command type: ' . get_class($command))
            };

            // Send the value back to the generator and advance
            $gen->send($value);
        }

        return $gen->getReturn();
    }

    private function executeSpawnTask(SpawnTask $command): mixed
    {
        $result = $this->spawn($command->taskName, $command->params, $command->options);

        if ($command->await) {
            // TODO: Implement await logic - would need to poll or wait for task completion
            // For now, just return the spawn result
            return $result;
        }

        return $result;
    }

    private function normalizeSpawnOptions(array $options): array
    {
        $normalized = [];

        if (isset($options['headers'])) {
            $normalized['headers'] = $options['headers'];
        }

        if (isset($options['maxAttempts'])) {
            $normalized['max_attempts'] = $options['maxAttempts'];
        }

        if (isset($options['retryStrategy'])) {
            $normalized['retry_strategy'] = $this->serializeRetryStrategy($options['retryStrategy']);
        }

        if (isset($options['cancellation']) && $options['cancellation'] !== null) {
            $cancellation = [];
            if (isset($options['cancellation']['maxDuration'])) {
                $cancellation['max_duration'] = $options['cancellation']['maxDuration'];
            }
            if (isset($options['cancellation']['maxDelay'])) {
                $cancellation['max_delay'] = $options['cancellation']['maxDelay'];
            }
            if (!empty($cancellation)) {
                $normalized['cancellation'] = $cancellation;
            }
        }

        return $normalized;
    }

    /** @param array{kind: string, baseSeconds?: int, factor?: float, maxSeconds?: int} $strategy */
    private function serializeRetryStrategy(array $strategy): array
    {
        $serialized = ['kind' => $strategy['kind']];

        if (isset($strategy['baseSeconds'])) {
            $serialized['base_seconds'] = $strategy['baseSeconds'];
        }
        if (isset($strategy['factor'])) {
            $serialized['factor'] = $strategy['factor'];
        }
        if (isset($strategy['maxSeconds'])) {
            $serialized['max_seconds'] = $strategy['maxSeconds'];
        }

        return $serialized;
    }
}
