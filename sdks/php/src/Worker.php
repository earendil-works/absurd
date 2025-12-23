<?php declare(strict_types=1);

namespace Absurd;

use Closure;

/**
 * Worker that continuously polls for and processes tasks.
 */
final class Worker
{
    private bool $running = false;

    /**
     * @internal
     */
    public function __construct(
        private readonly Absurd $absurd,
        private readonly string $workerId,
        private readonly int $claimTimeout,
        private readonly int $batchSize,
        private readonly int $concurrency,
        private readonly float $pollInterval,
        private readonly Closure $onError,
        private readonly bool $fatalOnLeaseTimeout
    ) {}

    /**
     * Start the worker loop.
     */
    public function start(): void
    {
        $this->running = true;
        $executing = 0;
        $lastPoll = 0.0;

        while ($this->running) {
            try {
                // Wait for available capacity or poll interval
                if ($executing >= $this->concurrency) {
                    $this->wait($this->pollInterval);
                    continue;
                }

                // Respect poll interval
                $now = microtime(true);
                $timeSinceLastPoll = $now - $lastPoll;
                if ($timeSinceLastPoll < $this->pollInterval) {
                    $this->wait($this->pollInterval - $timeSinceLastPoll);
                    continue;
                }

                $lastPoll = $now;

                // Claim tasks
                $availableCapacity = $this->concurrency - $executing;
                $toClaim = min($this->batchSize, $availableCapacity);

                if ($toClaim <= 0) {
                    $this->wait($this->pollInterval);
                    continue;
                }

                $tasks = $this->absurd->claimTasks([
                    'batchSize' => $toClaim,
                    'claimTimeout' => $this->claimTimeout,
                    'workerId' => $this->workerId
                ]);

                if (empty($tasks)) {
                    $this->wait($this->pollInterval);
                    continue;
                }

                // Process tasks (in this simple implementation, we process synchronously)
                // A real implementation might use async/parallel processing
                foreach ($tasks as $task) {
                    $executing++;
                    try {
                        $this->absurd->executeTask($task, $this->claimTimeout, $this->fatalOnLeaseTimeout);
                    } catch (\Throwable $e) {
                        ($this->onError)($e);
                    } finally {
                        $executing--;
                    }
                }

            } catch (\Throwable $e) {
                ($this->onError)($e);
                $this->wait($this->pollInterval);
            }
        }
    }

    /**
     * Stop the worker gracefully.
     */
    public function stop(): void
    {
        $this->running = false;
    }

    /**
     * Wait for a duration (in seconds).
     */
    private function wait(float $seconds): void
    {
        usleep((int)($seconds * 1_000_000));
    }
}
