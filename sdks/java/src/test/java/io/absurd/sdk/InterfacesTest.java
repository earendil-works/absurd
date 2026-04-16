package io.absurd.sdk;

public class InterfacesTest {
    public static void main(String[] args) {
        System.out.println("Testing TaskDefinition interface...");
        testTaskDefinition();
        
        System.out.println("Testing TaskHandler interface...");
        testTaskHandler();
        
        System.out.println("Testing TaskContext interface...");
        testTaskContext();
        
        System.out.println("Testing SpawnResult class...");
        testSpawnResult();
        
        System.out.println("Testing Worker interface...");
        testWorker();
        
        System.out.println("Testing TaskResultSnapshot class...");
        testTaskResultSnapshot();
        
        System.out.println("Testing AbsurdHooks interface...");
        testAbsurdHooks();
        
        System.out.println("Testing Absurd client...");
        testAbsurdClient();
        
        System.out.println("All interface tests passed!");
    }
    
    private static void testTaskDefinition() {
        TaskDefinition<String, String> definition = new TestTaskDefinition();
        assertEquals("test-task", definition.getName());
        assertNotNull(definition.getHandler());
        assertNotNull(definition.getRetryStrategy());
        assertNotNull(definition.getCancellationPolicy());
    }
    
    private static void testTaskHandler() {
        TaskHandler<String, String> handler = (input, context) -> "Processed: " + input;
        assertNotNull(handler);
    }
    
    private static void testTaskContext() {
        // TaskContext is an interface, we can't instantiate it directly
        // but we can verify it compiles and has the expected methods
        System.out.println("TaskContext interface compiled successfully");
    }
    
    private static void testSpawnResult() {
        SpawnResult result = new SpawnResult("run-123", "test-queue", "test-task");
        assertEquals("run-123", result.getRunId());
        assertEquals("test-queue", result.getQueueName());
        assertEquals("test-task", result.getTaskName());
    }
    
    private static void testWorker() {
        // Worker is an interface, we can't instantiate it directly
        // but we can verify it compiles and has the expected methods
        System.out.println("Worker interface compiled successfully");
    }
    
    private static void testTaskResultSnapshot() {
        TaskResultSnapshot snapshot = new TaskResultSnapshot(
            "run-123", "test-queue", "test-task", TaskState.COMPLETED,
            "result-data", null, 1, 
            java.time.Instant.now(), java.time.Instant.now(), java.time.Instant.now()
        );
        assertEquals("run-123", snapshot.getRunId());
        assertEquals("test-queue", snapshot.getQueueName());
        assertEquals("test-task", snapshot.getTaskName());
        assertEquals(TaskState.COMPLETED, snapshot.getState());
        assertEquals("result-data", snapshot.getResult());
        assertNull(snapshot.getError());
        assertEquals(1, snapshot.getAttempt());
    }
    
    private static void testAbsurdHooks() {
        AbsurdHooks hooks = new TestAbsurdHooks();
        assertNotNull(hooks);
        
        // Test that default methods work
        hooks.beforeTaskExecution(null);
        hooks.afterTaskSuccess(null, null);
        hooks.afterTaskFailure(null, null);
        hooks.onTaskRetry(null, null, 0);
    }
    
    private static void testAbsurdClient() {
        Absurd absurd = Absurd.builder()
                .connectionString("jdbc:postgresql://localhost:5432/test")
                .registerTask(new TestTaskDefinition())
                .hooks(new TestAbsurdHooks())
                .build();
        
        assertEquals("jdbc:postgresql://localhost:5432/test", absurd.getConnectionString());
        assertEquals(1, absurd.getTaskDefinitions().size());
        assertNotNull(absurd.getHooks());
        
        // Test that unsupported operations throw appropriate exceptions
        try {
            absurd.createWorker("test-queue");
            throw new AssertionError("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // Expected
        }
        
        try {
            absurd.spawn(SpawnOptions.builder()
                    .queueName("test-queue")
                    .taskName("test-task")
                    .input("test-input")
                    .build());
            throw new AssertionError("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // Expected
        }
        
        try {
            absurd.createQueue(CreateQueueOptions.builder()
                    .queueName("test-queue")
                    .build());
            throw new AssertionError("Expected UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {
            // Expected
        }
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
        // All methods use default implementations
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
