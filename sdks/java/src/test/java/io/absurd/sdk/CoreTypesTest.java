package io.absurd.sdk;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CoreTypesTest {

    @Test
    void testTaskStateEnum() {
        // Test that TaskState enum has all required states
        assertEquals(TaskState.PENDING, TaskState.valueOf("PENDING"));
        assertEquals(TaskState.RUNNING, TaskState.valueOf("RUNNING"));
        assertEquals(TaskState.COMPLETED, TaskState.valueOf("COMPLETED"));
        assertEquals(TaskState.FAILED, TaskState.valueOf("FAILED"));
        assertEquals(TaskState.CANCELLED, TaskState.valueOf("CANCELLED"));
        assertEquals(TaskState.TIMED_OUT, TaskState.valueOf("TIMED_OUT"));
    }

    @Test
    void testAbsurdException() {
        AbsurdException exception = new AbsurdException("Test error");
        assertEquals("Test error", exception.getMessage());
        assertNull(exception.getCause());
        
        Exception cause = new RuntimeException("Root cause");
        AbsurdException exceptionWithCause = new AbsurdException("Test error", cause);
        assertEquals("Test error", exceptionWithCause.getMessage());
        assertEquals(cause, exceptionWithCause.getCause());
    }

    @Test
    void testSpawnOptionsBuilder() {
        SpawnOptions options = SpawnOptions.builder()
                .queueName("test-queue")
                .taskName("test-task")
                .input("test-input")
                .build();
        
        assertEquals("test-queue", options.getQueueName());
        assertEquals("test-task", options.getTaskName());
        assertEquals("test-input", options.getInput());
    }

    @Test
    void testWorkerOptionsBuilder() {
        WorkerOptions options = WorkerOptions.builder()
                .queueName("test-queue")
                .concurrency(5)
                .build();
        
        assertEquals("test-queue", options.getQueueName());
        assertEquals(5, options.getConcurrency());
    }

    @Test
    void testRetryStrategy() {
        RetryStrategy strategy = new RetryStrategy(3, 1000, 2.0);
        assertEquals(3, strategy.getMaxAttempts());
        assertEquals(1000, strategy.getInitialDelayMs());
        assertEquals(2.0, strategy.getBackoffFactor());
    }

    @Test
    void testCancellationPolicy() {
        CancellationPolicy policy = new CancellationPolicy(30000, true);
        assertEquals(30000, policy.getTimeoutMs());
        assertTrue(policy.isInterruptible());
    }

    @Test
    void testCreateQueueOptions() {
        CreateQueueOptions options = CreateQueueOptions.builder()
                .queueName("test-queue")
                .retryStrategy(new RetryStrategy(3, 1000, 2.0))
                .build();
        
        assertEquals("test-queue", options.getQueueName());
        assertNotNull(options.getRetryStrategy());
        assertEquals(3, options.getRetryStrategy().getMaxAttempts());
    }
}
