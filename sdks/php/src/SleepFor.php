<?php declare(strict_types=1);

namespace Absurd;

/**
 * Command to sleep for a duration (in seconds).
 */
final readonly class SleepFor implements Command
{
    public function __construct(
        public string $stepName,
        public float $duration
    ) {}
}
