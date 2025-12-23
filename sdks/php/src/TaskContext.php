<?php declare(strict_types=1);

namespace Absurd;

use DateTimeInterface;
use DateTime;
use PDO;

final class TaskContext
{
    /** @var array<string, int> */
    private array $stepNameCounter = [];

    /** @var array<string, mixed> */
    private array $checkpointCache;

    private function __construct(
        private readonly PDO $pdo,
        public readonly string $taskId,
        private readonly string $queueName,
        private readonly object $task,
        private readonly int $claimTimeout,
    ) {}

    /**
     * Create a TaskContext by loading checkpoints from the database.
     */
    public static function create(
        PDO $pdo,
        string $taskId,
        string $queueName,
        object $task,
        int $claimTimeout
    ): self {
        $stmt = $pdo->prepare(
            'SELECT checkpoint_name, state FROM absurd.get_task_checkpoint_states(?, ?, ?)'
        );
        $stmt->execute([$queueName, $task->task_id, $task->run_id]);

        $cache = [];
        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            $cache[$row['checkpoint_name']] = json_decode($row['state'], true, 512, JSON_THROW_ON_ERROR);
        }

        $ctx = new self($pdo, $taskId, $queueName, $task, $claimTimeout);
        $ctx->checkpointCache = $cache;
        return $ctx;
    }

    /**
     * Execute a named step with checkpointing.
     *
     */
    public function executeCheckpoint(string $name, mixed $value): mixed
    {
        $checkpointName = $this->getCheckpointName($name);

        // Return cached result if exists
        if (array_key_exists($checkpointName, $this->checkpointCache)) {
            return $this->checkpointCache[$checkpointName];
        }

        // For callables, execute them; otherwise use the value directly
        $result = is_callable($value) ? $value() : $value;

        $this->persistCheckpoint($checkpointName, $result);
        return $result;
    }

    /**
     * Sleep for a duration (in seconds).
     *
     * @throws SuspendTask
     */
    public function executeSleepFor(string $stepName, float $duration): void
    {
        $wakeAt = new DateTime();
        $wakeAt->modify("+{$duration} seconds");
        $this->executeSleepUntil($stepName, $wakeAt);
    }

    /**
     * Sleep until a specific timestamp.
     *
     * @throws SuspendTask
     */
    public function executeSleepUntil(string $stepName, DateTimeInterface $wakeAt): void
    {
        $checkpointName = $this->getCheckpointName($stepName);
        $state = $this->checkpointCache[$checkpointName] ?? null;

        if ($state !== null) {
            $actualWakeAt = new DateTime($state);
        } else {
            $actualWakeAt = $wakeAt;
            $this->persistCheckpoint($checkpointName, $wakeAt->format(DateTime::ATOM));
        }

        if (time() < $actualWakeAt->getTimestamp()) {
            $this->scheduleRun($actualWakeAt);
            throw new SuspendTask();
        }
    }

    /**
     * Await an event with optional timeout.
     *
     * @param array{stepName?: string, timeout?: int} $options
     * @throws SuspendTask|TimeoutError
     */
    public function executeAwaitEvent(string $eventName, array $options = []): mixed
    {
        $timeout = $options['timeout'] ?? null;

        if ($timeout !== null && (!is_int($timeout) || $timeout < 0)) {
            $timeout = null;
        }

        // Use stepName if provided, otherwise default to $awaitEvent:{eventName}
        $stepName = $options['stepName'] ?? "\$awaitEvent:{$eventName}";
        $checkpointName = $this->getCheckpointName($stepName);

        // Return cached result if exists
        if (array_key_exists($checkpointName, $this->checkpointCache)) {
            return $this->checkpointCache[$checkpointName];
        }

        // Check if woke due to timeout
        if ($this->task->wake_event === $eventName && $this->task->event_payload === null) {
            $this->task->wake_event = null;
            $this->task->event_payload = null;
            throw new TimeoutError($eventName);
        }

        $stmt = $this->pdo->prepare(
            'SELECT should_suspend, payload FROM absurd.await_event(?, ?, ?, ?, ?, ?)'
        );
        $stmt->execute([
            $this->queueName,
            $this->task->task_id,
            $this->task->run_id,
            $checkpointName,
            $eventName,
            $timeout
        ]);

        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        if (!$row) {
            throw new TaskExecutionError('Failed to await event');
        }

        if (!$row['should_suspend']) {
            $payload = json_decode($row['payload'], true, 512, JSON_THROW_ON_ERROR);
            $this->checkpointCache[$checkpointName] = $payload;
            $this->task->event_payload = null;
            return $payload;
        }

        throw new SuspendTask();
    }

    /**
     * Emit an event.
     *
     */
    public function executeEmitEvent(string $eventName, mixed $payload = null): void
    {
        if (empty($eventName)) {
            throw new TaskExecutionError('eventName must be a non-empty string');
        }

        $stmt = $this->pdo->prepare('SELECT absurd.emit_event(?, ?, ?)');
        $stmt->execute([
            $this->queueName,
            $eventName,
            json_encode($payload, JSON_THROW_ON_ERROR)
        ]);
    }

    /**
     * Complete the current run with a result.
     *
     */
    public function complete(mixed $result): void
    {
        $stmt = $this->pdo->prepare('SELECT absurd.complete_run(?, ?, ?)');
        $stmt->execute([
            $this->queueName,
            $this->task->run_id,
            json_encode($result, JSON_THROW_ON_ERROR)
        ]);
    }

    /**
     * Fail the current run with an error.
     *
     */
    public function fail(\Throwable $error): void
    {
        $stmt = $this->pdo->prepare('SELECT absurd.fail_run(?, ?, ?, ?)');
        $stmt->execute([
            $this->queueName,
            $this->task->run_id,
            json_encode($this->serializeError($error), JSON_THROW_ON_ERROR),
            null
        ]);
    }

    /**
     * Generate a unique checkpoint name with automatic deduplication.
     */
    private function getCheckpointName(string $name): string
    {
        $count = ($this->stepNameCounter[$name] ?? 0) + 1;
        $this->stepNameCounter[$name] = $count;
        return $count === 1 ? $name : "{$name}#{$count}";
    }

    /**
     * Persist a checkpoint value to the database.
     */
    private function persistCheckpoint(string $checkpointName, mixed $value): void
    {
        $stmt = $this->pdo->prepare(
            'SELECT absurd.set_task_checkpoint_state(?, ?, ?, ?, ?, ?)'
        );
        $stmt->execute([
            $this->queueName,
            $this->task->task_id,
            $checkpointName,
            json_encode($value, JSON_THROW_ON_ERROR),
            $this->task->run_id,
            $this->claimTimeout
        ]);
        $this->checkpointCache[$checkpointName] = $value;
    }

    /**
     * Schedule the next run to wake at a specific time.
     */
    private function scheduleRun(DateTimeInterface $wakeAt): void
    {
        $stmt = $this->pdo->prepare('SELECT absurd.schedule_run(?, ?, ?)');
        $stmt->execute([
            $this->queueName,
            $this->task->run_id,
            $wakeAt->format('Y-m-d H:i:s')
        ]);
    }

    /**
     * Serialize an exception for storage.
     *
     * @return array{name: string, message: string, stack: string}
     */
    private function serializeError(\Throwable $error): array
    {
        return [
            'name' => get_class($error),
            'message' => $error->getMessage(),
            'stack' => $error->getTraceAsString()
        ];
    }
}
