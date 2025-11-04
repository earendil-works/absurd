<?php declare(strict_types=1);

namespace Absurd;

use Exception;
use Throwable;

/**
 * Exception thrown when task execution fails.
 */
final class TaskExecutionError extends Exception
{
    public function __construct(string $message, ?Throwable $previous = null)
    {
        parent::__construct($message, 0, $previous);
    }
}
