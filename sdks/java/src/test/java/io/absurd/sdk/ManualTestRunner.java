package io.absurd.sdk;

public class ManualTestRunner {
    public static void main(String[] args) {
        System.out.println("Testing TaskState enum...");
        testTaskStateEnum();
        
        System.out.println("Testing AbsurdException...");
        testAbsurdException();
        
        System.out.println("Testing SpawnOptions builder...");
        testSpawnOptionsBuilder();
        
        System.out.println("Testing WorkerOptions builder...");
        testWorkerOptionsBuilder();
        
        System.out.println("Testing RetryStrategy...");
        testRetryStrategy();
        
        System.out.println("Testing CancellationPolicy...");
        testCancellationPolicy();
        
        System.out.println("Testing CreateQueueOptions...");
        testCreateQueueOptions();
        
        System.out.println("All tests passed!");
    }
    
    private static void testTaskStateEnum() {
        // Test that TaskState enum has all required states
        assert TaskState.PENDING == TaskState.valueOf("PENDING");
        assert TaskState.RUNNING == TaskState.valueOf("RUNNING");
        assert TaskState.COMPLETED == TaskState.valueOf("COMPLETED");
        assert TaskState.FAILED == TaskState.valueOf("FAILED");
        assert TaskState.CANCELLED == TaskState.valueOf("CANCELLED");
        assert TaskState.TIMED_OUT == TaskState.valueOf("TIMED_OUT");
    }
    
    private static void testAbsurdException() {
        AbsurdException exception = new AbsurdException("Test error");
        assertEquals("Test error", exception.getMessage());
        assertTrue(exception.getCause() == null);
        
        Exception cause = new RuntimeException("Root cause");
        AbsurdException exceptionWithCause = new AbsurdException("Test error", cause);
        assertEquals("Test error", exceptionWithCause.getMessage());
        assertTrue(cause == exceptionWithCause.getCause());
    }
    
    private static void testSpawnOptionsBuilder() {
        SpawnOptions options = SpawnOptions.builder()
                .queueName("test-queue")
                .taskName("test-task")
                .input("test-input")
                .build();
        
        assertEquals("test-queue", options.getQueueName());
        assertEquals("test-task", options.getTaskName());
        assertEquals("test-input", options.getInput());
    }
    
    private static void testWorkerOptionsBuilder() {
        WorkerOptions options = WorkerOptions.builder()
                .queueName("test-queue")
                .concurrency(5)
                .build();
        
        assertEquals("test-queue", options.getQueueName());
        assertTrue(options.getConcurrency() == 5);
    }
    
    private static void testRetryStrategy() {
        RetryStrategy strategy = new RetryStrategy(3, 1000, 2.0);
        assertTrue(strategy.getMaxAttempts() == 3);
        assertTrue(strategy.getInitialDelayMs() == 1000);
        assertTrue(strategy.getBackoffFactor() == 2.0);
    }
    
    private static void testCancellationPolicy() {
        CancellationPolicy policy = new CancellationPolicy(30000, true);
        assertTrue(policy.getTimeoutMs() == 30000);
        assertTrue(policy.isInterruptible() == true);
    }
    
    private static void testCreateQueueOptions() {
        CreateQueueOptions options = CreateQueueOptions.builder()
                .queueName("test-queue")
                .retryStrategy(new RetryStrategy(3, 1000, 2.0))
                .build();
        
        assertEquals("test-queue", options.getQueueName());
        assertNotNull(options.getRetryStrategy());
        assertTrue(options.getRetryStrategy().getMaxAttempts() == 3);
    }
    
    private static void assertTrue(boolean condition) {
        if (!condition) {
            throw new AssertionError("Assertion failed");
        }
    }
    
    private static void assertEquals(Object expected, Object actual) {
        if (expected == null) {
            if (actual != null) {
                throw new AssertionError("Expected null, but got: " + actual);
            }
        } else {
            if (!expected.equals(actual)) {
                throw new AssertionError("Expected: " + expected + ", but got: " + actual);
            }
        }
    }
    
    private static void assertNotNull(Object actual) {
        if (actual == null) {
            throw new AssertionError("Expected non-null, but got null");
        }
    }
}
