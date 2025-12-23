<?php declare(strict_types=1);

namespace Absurd;

use DateTimeInterface;

function step(string $name, mixed $value): Checkpoint
{
    return new Checkpoint($name, $value);
}

/** @param array{stepName?: string, timeout?: int} $options */
function awaitEvent(string $eventName, array $options = []): AwaitEvent
{
    return new AwaitEvent($eventName, $options);
}

function sleepFor(string $stepName, float $duration): SleepFor
{
    return new SleepFor($stepName, $duration);
}

function sleepUntil(string $stepName, DateTimeInterface $wakeAt): SleepUntil
{
    return new SleepUntil($stepName, $wakeAt);
}

function emitEvent(string $eventName, mixed $payload = null): EmitEvent
{
    return new EmitEvent($eventName, $payload);
}

/** @param array{maxAttempts?: int, retryStrategy?: array, headers?: array, queue?: string, cancellation?: array} $options */
function spawn(string $taskName, mixed $params, array $options = [], bool $await = false): SpawnTask
{
    return new SpawnTask($taskName, $params, $options, $await);
}
