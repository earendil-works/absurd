<?php declare(strict_types=1);

namespace Absurd;

/**
 * Command to spawn a child task and optionally await its result.
 */
final readonly class SpawnTask implements Command
{
    /**
     * @param string $taskName
     * @param mixed $params
     * @param array{maxAttempts?: int, retryStrategy?: array, headers?: array, queue?: string, cancellation?: array} $options
     * @param bool $await Whether to wait for the spawned task to complete
     */
    public function __construct(
        public string $taskName,
        public mixed $params,
        public array $options = [],
        public bool $await = false
    ) {}
}
