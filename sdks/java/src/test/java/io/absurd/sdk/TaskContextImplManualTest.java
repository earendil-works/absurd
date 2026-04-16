package io.absurd.sdk;

import io.absurd.sdk.internal.*;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * Manual test for TaskContextImpl that doesn't require JUnit/Mockito.
 */
public class TaskContextImplManualTest {
    
    public static void main(String[] args) {
        System.out.println("=== Testing TaskContextImpl Implementation ===");
        
        // Test 1: Basic creation and properties
        testBasicFunctionality();
        
        // Test 2: Exception classes
        testExceptionClasses();
        
        // Test 3: Method signatures
        testMethodSignatures();
        
        System.out.println("\n=== All TaskContextImpl Tests Passed ===");
    }
    
    private static void testBasicFunctionality() {
        System.out.println("Testing basic functionality...");
        
        // Create mock objects
        DbClient.ClaimedTask claimedTask = new DbClient.ClaimedTask("run123", "test-task", "{}", 1);
        MockDbClient mockDbClient = new MockDbClient();
        
        TaskContextImpl context = new TaskContextImpl(mockDbClient, claimedTask, "test-queue", Map.of("key", "value"));
        
        // Test basic properties
        if ("run123".equals(context.getRunId()) &&
            "test-queue".equals(context.getQueueName()) &&
            "test-task".equals(context.getTaskName()) &&
            context.getAttempt() == 1 &&
            context.getMaxAttempts() == 3 &&
            "value".equals(context.getMetadata().get("key"))) {
            System.out.println("✓ Basic properties work correctly");
        } else {
            throw new AssertionError("Basic properties incorrect");
        }
        
        // Test isCancelled (should return false since we can't check state)
        if (!context.isCancelled()) {
            System.out.println("✓ isCancelled works correctly");
        } else {
            throw new AssertionError("isCancelled should return false");
        }
    }
    
    private static void testExceptionClasses() {
        System.out.println("Testing exception classes...");
        
        // Test SuspendTask
        SuspendTask suspendTask = new SuspendTask("Test suspension");
        if ("Test suspension".equals(suspendTask.getMessage())) {
            System.out.println("✓ SuspendTask works correctly");
        } else {
            throw new AssertionError("SuspendTask message incorrect");
        }
        
        // Test CancelledTask
        CancelledTask cancelledTask = new CancelledTask("Test cancellation");
        if ("Test cancellation".equals(cancelledTask.getMessage())) {
            System.out.println("✓ CancelledTask works correctly");
        } else {
            throw new AssertionError("CancelledTask message incorrect");
        }
        
        // Test FailedTask
        FailedTask failedTask = new FailedTask("Test failure");
        if ("Test failure".equals(failedTask.getMessage())) {
            System.out.println("✓ FailedTask works correctly");
        } else {
            throw new AssertionError("FailedTask message incorrect");
        }
    }
    
    private static void testMethodSignatures() {
        System.out.println("Testing method signatures...");
        
        DbClient.ClaimedTask claimedTask = new DbClient.ClaimedTask("run123", "test-task", "{}", 1);
        MockDbClient mockDbClient = new MockDbClient();
        TaskContextImpl context = new TaskContextImpl(mockDbClient, claimedTask, "test-queue", Map.of());
        
        try {
            // Test step method signature
            java.lang.reflect.Method stepMethod = TaskContextImpl.class.getMethod(
                "step", String.class, String.class, TaskContextImpl.CheckpointFunction.class);
            
            // Test sleepFor method signature
            java.lang.reflect.Method sleepForMethod = TaskContextImpl.class.getMethod(
                "sleepFor", Duration.class);
            
            // Test sleepUntil method signature
            java.lang.reflect.Method sleepUntilMethod = TaskContextImpl.class.getMethod(
                "sleepUntil", Instant.class);
            
            // Test awaitEvent method signature
            java.lang.reflect.Method awaitEventMethod = TaskContextImpl.class.getMethod(
                "awaitEvent", String.class, Duration.class);
            
            // Test emitEvent method signature
            java.lang.reflect.Method emitEventMethod = TaskContextImpl.class.getMethod(
                "emitEvent", String.class, String.class);
            
            // Test heartbeat method signature
            java.lang.reflect.Method heartbeatMethod = TaskContextImpl.class.getMethod(
                "heartbeat");
            
            // Test awaitTaskResult method signature
            java.lang.reflect.Method awaitTaskResultMethod = TaskContextImpl.class.getMethod(
                "awaitTaskResult", String.class, Duration.class);
            
            System.out.println("✓ All method signatures are correct");
            
        } catch (NoSuchMethodException e) {
            throw new AssertionError("Method signature missing: " + e.getMessage());
        }
        
        // Test that suspension methods throw SuspendTask
        try {
            context.sleepFor(Duration.ofSeconds(10));
            throw new AssertionError("Expected SuspendTask exception");
        } catch (SuspendTask e) {
            System.out.println("✓ sleepFor throws SuspendTask correctly");
        } catch (Exception e) {
            throw new AssertionError("Wrong exception type: " + e.getClass().getName());
        }
        
        try {
            context.sleepUntil(Instant.now().plusSeconds(60));
            throw new AssertionError("Expected SuspendTask exception");
        } catch (SuspendTask e) {
            System.out.println("✓ sleepUntil throws SuspendTask correctly");
        } catch (Exception e) {
            throw new AssertionError("Wrong exception type: " + e.getClass().getName());
        }
        
        try {
            context.awaitEvent("test-event", Duration.ofSeconds(30));
            throw new AssertionError("Expected SuspendTask exception");
        } catch (SuspendTask e) {
            System.out.println("✓ awaitEvent throws SuspendTask correctly");
        } catch (Exception e) {
            throw new AssertionError("Wrong exception type: " + e.getClass().getName());
        }
        
        try {
            context.awaitTaskResult("other-run", Duration.ofSeconds(30));
            throw new AssertionError("Expected SuspendTask exception");
        } catch (SuspendTask e) {
            System.out.println("✓ awaitTaskResult throws SuspendTask correctly");
        } catch (Exception e) {
            throw new AssertionError("Wrong exception type: " + e.getClass().getName());
        }
    }
    
    /**
     * Mock DbClient for testing without actual database connection.
     */
    static class MockDbClient extends DbClient {
        public MockDbClient() {
            super(new MockDataSource());
        }
        
        @Override
        public String spawnTask(String queue, String taskName, String input, String metadata, String parentRunId, String cronSchedule) {
            return "mock-run-" + System.currentTimeMillis();
        }
        
        @Override
        public TaskState getTaskState(String queue, String runId) {
            return TaskState.RUNNING; // Default for testing
        }
        
        @Override
        public void heartbeatTask(String queue, String runId) {
            // No-op for testing
        }
    }
    
    /**
     * Mock DataSource for testing.
     */
    static class MockDataSource implements DataSource {
        @Override
        public java.sql.Connection getConnection() throws SQLException {
            throw new SQLException("Mock connection - not implemented");
        }
        
        @Override
        public java.sql.Connection getConnection(String username, String password) throws SQLException {
            throw new SQLException("Mock connection - not implemented");
        }
        
        // Other required methods omitted for brevity
        @Override
        public java.io.PrintWriter getLogWriter() throws SQLException {
            throw new SQLException("Mock - not implemented");
        }
        
        @Override
        public void setLogWriter(java.io.PrintWriter out) throws SQLException {
            throw new SQLException("Mock - not implemented");
        }
        
        @Override
        public void setLoginTimeout(int seconds) throws SQLException {
            throw new SQLException("Mock - not implemented");
        }
        
        @Override
        public int getLoginTimeout() throws SQLException {
            throw new SQLException("Mock - not implemented");
        }
        
        @Override
        public java.util.logging.Logger getParentLogger() throws java.sql.SQLFeatureNotSupportedException {
            throw new java.sql.SQLFeatureNotSupportedException("Mock - not implemented");
        }
        
        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLException("Mock - not implemented");
        }
        
        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            throw new SQLException("Mock - not implemented");
        }
    }
}