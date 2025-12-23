<?php declare(strict_types=1);

namespace Absurd;

/**
 * Command to emit an event.
 */
final readonly class EmitEvent implements Command
{
    public function __construct(
        public string $eventName,
        public mixed $payload = null
    ) {}
}
