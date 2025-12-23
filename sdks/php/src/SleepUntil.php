<?php declare(strict_types=1);

namespace Absurd;

use DateTimeInterface;

/**
 * Command to sleep until a specific timestamp.
 */
final readonly class SleepUntil implements Command
{
    public function __construct(
        public string $stepName,
        public DateTimeInterface $wakeAt
    ) {}
}
