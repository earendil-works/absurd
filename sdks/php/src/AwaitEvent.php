<?php declare(strict_types=1);

namespace Absurd;

/**
 * Command to await an event with optional timeout.
 */
final readonly class AwaitEvent implements Command
{
    /**
     * @param string $eventName
     * @param array{timeout?: int} $options
     */
    public function __construct(
        public string $eventName,
        public array $options = []
    ) {}
}
