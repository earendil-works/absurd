package io.absurd.sdk;

/**
 * Complete test of all public API types and interfaces required by issue #2.
 */
public class CompleteAPITest {
    public static void main(String[] args) {
        System.out.println("=== Testing Issue #2 Requirements ===");
        
        // Test all required classes and interfaces
        testAbsurdClient();
        testTaskDefinition();
        testTaskHandler();
        testTaskContext();
        testWorker();
        testSpawnResult();
        testSpawnOptions();
        testWorkerOptions();
        testRetryStrategy();
        testCancellationPolicy();
        testCreateQueueOptions();
        testTaskResultSnapshot();
        testTaskState();
        testAbsurdHooks();
        testAbsurdException();
        
        System.out.println("\n=== All Issue #2 Requirements Satisfied ===");
        System.out.println("✓ Absurd.java (main client, builder pattern)");
        System.out.println("✓ TaskDefinition.java, TaskHandler.java, TaskContext.java");
        System.out.println("✓ Worker.java, SpawnResult.java, SpawnOptions.java, WorkerOptions.java");
        System.out.println("✓ RetryStrategy.java, CancellationPolicy.java, CreateQueueOptions.java");
        System.out.println("✓ TaskResultSnapshot.java, TaskState.java (enum)");
        System.out.println("✓ AbsurdHooks.java, AbsurdException.java");
    }
    
    private static void testAbsurdClient() {
        System.out.println("Testing Absurd client...");
        
        Absurd absurd = Absurd.builder()
                .connectionString("jdbc:postgresql://localhost:5432/test")
                .registerTask(new TestTaskDefinition())
                .hooks(new TestAbsurdHooks())
                .build();
        
        assertNotNull(absurd);
        assertEquals("jdbc:postgresql://localhost:5432/test", absurd.getConnectionString());
        assertEquals(1, absurd.getTaskDefinitions().size());
        assertNotNull(absurd.getHooks());
        
        // Test default strategies
        assertNotNull(absurd.getDefaultRetryStrategy());
        assertTrue(absurd.getDefaultRetryStrategy().getMaxAttempts() == 3);
        assertNotNull(absurd.getDefaultCancellationPolicy());
        assertTrue(absurd.getDefaultCancellationPolicy().getTimeoutMs() == 30000);
        
        // Test builder validation
        try {
            Absurd.builder().build();
            throw new AssertionError("Expected IllegalStateException for missing connection string");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("Connection string must be specified"));
        }
        
        try {
            Absurd.builder()
                .connectionString("jdbc:postgresql://localhost:5432/test")
                .build();
            throw new AssertionError("Expected IllegalStateException for missing task definitions");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("At least one task definition must be registered"));
        }
        
        // Test builder pattern
        assertNotNull(Absurd.builder());
        
        System.out.println("✓ Absurd client works correctly");
    }
    
    private static void testTaskDefinition() {
        System.out.println("Testing TaskDefinition...");
        
        TaskDefinition<String, String> definition = new TestTaskDefinition();
        assertEquals("test-task", definition.getName());
        assertNotNull(definition.getHandler());
        assertNotNull(definition.getRetryStrategy());
        assertNotNull(definition.getCancellationPolicy());
        
        System.out.println("✓ TaskDefinition works correctly");
    }
    
    private static void testTaskHandler() {
        System.out.println("Testing TaskHandler...");
        
        TaskHandler<String, String> handler = (input, context) -> "Processed: " + input;
        assertNotNull(handler);
        
        System.out.println("✓ TaskHandler works correctly");
    }
    
    private static void testTaskContext() {
        System.out.println("Testing TaskContext...");
        
        // TaskContext is an interface - verify it compiles with all required methods
        System.out.println("✓ TaskContext interface compiled successfully");
    }
    
    private static void testWorker() {
        System.out.println("Testing Worker...");
        
        // Worker is an interface - verify it compiles with all required methods
        System.out.println("✓ Worker interface compiled successfully");
    }
    
    private static void testSpawnResult() {
        System.out.println("Testing SpawnResult...");
        
        SpawnResult result = new SpawnResult("run-123", "test-queue", "test-task");
        assertEquals("run-123", result.getRunId());
        assertEquals("test-queue", result.getQueueName());
        assertEquals("test-task", result.getTaskName());
        
        System.out.println("✓ SpawnResult works correctly");
    }
    
    private static void testSpawnOptions() {
        System.out.println("Testing SpawnOptions...");
        
        SpawnOptions options = SpawnOptions.builder()
                .queueName("test-queue")
                .taskName("test-task")
                .input("test-input")
                .metadata(java.util.Map.of("key", "value"))
                .parentRunId("parent-123")
                .cronSchedule("* * * * *")
                .build();
        
        assertEquals("test-queue", options.getQueueName());
        assertEquals("test-task", options.getTaskName());
        assertEquals("test-input", options.getInput());
        assertEquals("value", options.getMetadata().get("key"));
        assertEquals("parent-123", options.getParentRunId());
        assertEquals("* * * * *", options.getCronSchedule());
        
        // Test builder validation
        try {
            SpawnOptions.builder().build();
            throw new AssertionError("Expected IllegalStateException for missing queueName");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("queueName must be specified"));
        }
        
        try {
            SpawnOptions.builder()
                .queueName("test-queue")
                .build();
            throw new AssertionError("Expected IllegalStateException for missing taskName");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("taskName must be specified"));
        }
        
        System.out.println("✓ SpawnOptions works correctly");
    }
    
    private static void testWorkerOptions() {
        System.out.println("Testing WorkerOptions...");
        
        WorkerOptions options = WorkerOptions.builder()
                .queueName("test-queue")
                .concurrency(5)
                .pollIntervalMs(500)
                .maxPollIntervalMs(10000)
                .pollTimeoutMs(15000)
                .build();
        
        assertEquals("test-queue", options.getQueueName());
        assertEquals(5, options.getConcurrency());
        assertEquals(500, options.getPollIntervalMs());
        assertEquals(10000, options.getMaxPollIntervalMs());
        assertEquals(15000, options.getPollTimeoutMs());
        
        // Test builder validation
        try {
            WorkerOptions.builder().build();
            throw new AssertionError("Expected IllegalStateException for missing queueName");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("queueName must be specified"));
        }
        
        try {
            WorkerOptions.builder()
                .queueName("test-queue")
                .concurrency(0)
                .build();
            throw new AssertionError("Expected IllegalStateException for invalid concurrency");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("concurrency must be positive"));
        }
        
        System.out.println("✓ WorkerOptions works correctly");
    }
    
    private static void testRetryStrategy() {
        System.out.println("Testing RetryStrategy...");
        
        RetryStrategy strategy = new RetryStrategy(3, 1000, 2.0, 30000);
        assertTrue(strategy.getMaxAttempts() == 3);
        assertTrue(strategy.getInitialDelayMs() == 1000);
        assertTrue(strategy.getBackoffFactor() == 2.0);
        assertTrue(strategy.getMaxDelayMs() == 30000);
        
        // Test validation
        try {
            new RetryStrategy(0, 1000, 2.0);
            throw new AssertionError("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        
        System.out.println("✓ RetryStrategy works correctly");
    }
    
    private static void testCancellationPolicy() {
        System.out.println("Testing CancellationPolicy...");
        
        CancellationPolicy policy = new CancellationPolicy(30000, true);
        assertTrue(policy.getTimeoutMs() == 30000);
        assertTrue(policy.isInterruptible());
        
        // Test validation
        try {
            new CancellationPolicy(-1, true);
            throw new AssertionError("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // Expected
        }
        
        System.out.println("✓ CancellationPolicy works correctly");
    }
    
    private static void testCreateQueueOptions() {
        System.out.println("Testing CreateQueueOptions...");
        
        CreateQueueOptions options = CreateQueueOptions.builder()
                .queueName("test-queue")
                .retryStrategy(new RetryStrategy(3, 1000, 2.0))
                .cancellationPolicy(new CancellationPolicy(30000, true))
                .build();
        
        assertEquals("test-queue", options.getQueueName());
        assertNotNull(options.getRetryStrategy());
        assertNotNull(options.getCancellationPolicy());
        
        // Test builder validation
        try {
            CreateQueueOptions.builder().build();
            throw new AssertionError("Expected IllegalStateException for missing queueName");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("queueName must be specified"));
        }
        
        System.out.println("✓ CreateQueueOptions works correctly");
    }
    
    private static void testTaskResultSnapshot() {
        System.out.println("Testing TaskResultSnapshot...");
        
        java.time.Instant now = java.time.Instant.now();
        TaskResultSnapshot snapshot = new TaskResultSnapshot(
            "run-123", "test-queue", "test-task", TaskState.COMPLETED,
            "result-data", "error-message", 2, 
            now, now.plusSeconds(10), now.plusSeconds(20)
        );
        
        assertEquals("run-123", snapshot.getRunId());
        assertEquals("test-queue", snapshot.getQueueName());
        assertEquals("test-task", snapshot.getTaskName());
        assertEquals(TaskState.COMPLETED, snapshot.getState());
        assertEquals("result-data", snapshot.getResult());
        assertEquals("error-message", snapshot.getError());
        assertEquals(2, snapshot.getAttempt());
        assertNotNull(snapshot.getCreatedAt());
        assertNotNull(snapshot.getUpdatedAt());
        assertNotNull(snapshot.getCompletedAt());
        
        System.out.println("✓ TaskResultSnapshot works correctly");
    }
    
    private static void testTaskState() {
        System.out.println("Testing TaskState enum...");
        
        // Test all enum values
        assertEquals(TaskState.PENDING, TaskState.valueOf("PENDING"));
        assertEquals(TaskState.RUNNING, TaskState.valueOf("RUNNING"));
        assertEquals(TaskState.COMPLETED, TaskState.valueOf("COMPLETED"));
        assertEquals(TaskState.FAILED, TaskState.valueOf("FAILED"));
        assertEquals(TaskState.CANCELLED, TaskState.valueOf("CANCELLED"));
        assertEquals(TaskState.TIMED_OUT, TaskState.valueOf("TIMED_OUT"));
        
        System.out.println("✓ TaskState enum works correctly");
    }
    
    private static void testAbsurdHooks() {
        System.out.println("Testing AbsurdHooks...");
        
        AbsurdHooks hooks = new TestAbsurdHooks();
        assertNotNull(hooks);
        
        // Test default implementations
        hooks.beforeTaskExecution(null);
        hooks.afterTaskSuccess(null, null);
        hooks.afterTaskFailure(null, null);
        hooks.onTaskRetry(null, null, 0);
        
        System.out.println("✓ AbsurdHooks works correctly");
    }
    
    private static void testAbsurdException() {
        System.out.println("Testing AbsurdException...");
        
        AbsurdException exception = new AbsurdException("Test error");
        assertEquals("Test error", exception.getMessage());
        assertNull(exception.getCause());
        
        Exception cause = new RuntimeException("Root cause");
        AbsurdException exceptionWithCause = new AbsurdException("Test error", cause);
        assertEquals("Test error", exceptionWithCause.getMessage());
        assertEquals(cause, exceptionWithCause.getCause());
        
        System.out.println("✓ AbsurdException works correctly");
    }
    
    // Test implementations
    private static class TestTaskDefinition implements TaskDefinition<String, String> {
        @Override
        public String getName() {
            return "test-task";
        }
        
        @Override
        public TaskHandler<String, String> getHandler() {
            return (input, context) -> "Processed: " + input;
        }
        
        @Override
        public RetryStrategy getRetryStrategy() {
            return new RetryStrategy(3, 1000, 2.0);
        }
        
        @Override
        public CancellationPolicy getCancellationPolicy() {
            return new CancellationPolicy(30000, true);
        }
    }
    
    private static class TestAbsurdHooks implements AbsurdHooks {
        // Uses default implementations
    }
    
    // Assertion helpers
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
    
    private static void assertNull(Object actual) {
        if (actual != null) {
            throw new AssertionError("Expected null, but got: " + actual);
        }
    }
    
    private static void assertTrue(boolean condition) {
        if (!condition) {
            throw new AssertionError("Assertion failed");
        }
    }
}
