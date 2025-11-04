<?php declare(strict_types=1);

namespace Absurd;

use Exception;

/**
 * Exception thrown when awaiting an event times out.
 */
final class TimeoutError extends Exception
{
    public function __construct(string $eventName)
    {
        parent::__construct("Timed out waiting for event \"{$eventName}\"");
    }
}
