<?php declare(strict_types=1);

namespace Absurd;

/**
 * Command to execute and checkpoint an operation with a named step.
 */
final readonly class Checkpoint implements Command
{
    public function __construct(
        public string $name,
        public mixed $value
    ) {}
}
