<?php declare(strict_types=1);

namespace Absurd;

use Exception;

/**
 * Exception thrown to suspend task execution.
 * This is internal to the SDK and should not be caught by user code.
 */
final class SuspendTask extends Exception
{
    public function __construct()
    {
        parent::__construct('Task suspended');
    }
}
