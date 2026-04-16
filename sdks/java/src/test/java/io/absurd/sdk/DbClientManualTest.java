package io.absurd.sdk;

import io.absurd.sdk.internal.DbClient;
import java.io.PrintWriter;

import javax.sql.DataSource;
import java.sql.*;

/**
 * Manual test for DbClient that doesn't require JUnit/Mockito.
 * This verifies the basic structure and compilation of the DbClient class.
 */
public class DbClientManualTest {
    
    public static void main(String[] args) {
        System.out.println("=== Testing DbClient Implementation ===");
        
        // Test 1: DbClient creation
        testDbClientCreation();
        
        // Test 2: ClaimedTask record
        testClaimedTaskRecord();
        
        // Test 3: Basic method signatures
        testMethodSignatures();
        
        // Test 4: Input validation
        testValidation();
        
        System.out.println("\n=== All DbClient Tests Passed ===");
    }
    
    private static void testDbClientCreation() {
        System.out.println("Testing DbClient creation...");
        
        try {
            // This should throw IllegalArgumentException for null dataSource
            new DbClient(null);
            throw new AssertionError("Expected IllegalArgumentException for null dataSource");
        } catch (IllegalArgumentException e) {
            if (e.getMessage().contains("dataSource cannot be null")) {
                System.out.println("✓ DbClient properly validates null dataSource");
            } else {
                throw new AssertionError("Wrong error message: " + e.getMessage());
            }
        }
        
        // Test with mock DataSource
        DataSource mockDataSource = new MockDataSource();
        DbClient dbClient = new DbClient(mockDataSource);
        
        if (dbClient != null) {
            System.out.println("✓ DbClient created successfully");
        } else {
            throw new AssertionError("DbClient creation failed");
        }
    }
    
    private static void testClaimedTaskRecord() {
        System.out.println("Testing ClaimedTask record...");
        
        DbClient.ClaimedTask task = new DbClient.ClaimedTask("run123", "test-task", "{}", 1);
        
        if ("run123".equals(task.runId()) &&
            "test-task".equals(task.taskName()) &&
            "{}".equals(task.input()) &&
            task.attempt() == 1) {
            System.out.println("✓ ClaimedTask record works correctly");
        } else {
            throw new AssertionError("ClaimedTask record values incorrect");
        }
    }
    
    private static void testMethodSignatures() {
        System.out.println("Testing method signatures...");
        
        DataSource mockDataSource = new MockDataSource();
        DbClient dbClient = new DbClient(mockDataSource);
        
        // Verify all methods exist and have correct signatures
        try {
            // Test claimTasks signature
            java.lang.reflect.Method claimTasksMethod = DbClient.class.getMethod(
                "claimTasks", String.class, String.class, int.class, int.class);
            
            // Test spawnTask signature
            java.lang.reflect.Method spawnTaskMethod = DbClient.class.getMethod(
                "spawnTask", String.class, String.class, String.class, String.class, String.class, String.class);
            
            // Test completeTask signature
            java.lang.reflect.Method completeTaskMethod = DbClient.class.getMethod(
                "completeTask", String.class, String.class, String.class);
            
            // Test failTask signature
            java.lang.reflect.Method failTaskMethod = DbClient.class.getMethod(
                "failTask", String.class, String.class, String.class);
            
            // Test cancelTask signature
            java.lang.reflect.Method cancelTaskMethod = DbClient.class.getMethod(
                "cancelTask", String.class, String.class, String.class);
            
            // Test getTaskState signature
            java.lang.reflect.Method getTaskStateMethod = DbClient.class.getMethod(
                "getTaskState", String.class, String.class);
            
            // Test getTaskResult signature
            java.lang.reflect.Method getTaskResultMethod = DbClient.class.getMethod(
                "getTaskResult", String.class, String.class);
            
            // Test createQueue signature
            java.lang.reflect.Method createQueueMethod = DbClient.class.getMethod(
                "createQueue", String.class, String.class, String.class);
            
            // Test heartbeatTask signature
            java.lang.reflect.Method heartbeatTaskMethod = DbClient.class.getMethod(
                "heartbeatTask", String.class, String.class);
            
            System.out.println("✓ All method signatures are correct");
            
        } catch (NoSuchMethodException e) {
            throw new AssertionError("Method signature missing: " + e.getMessage());
        }
    }
    
    private static void testValidation() {
        System.out.println("Testing input validation...");
        
        DataSource mockDataSource = new MockDataSource();
        DbClient dbClient = new DbClient(mockDataSource);
        
        // Test claimTasks validation
        try {
            dbClient.claimTasks(null, "worker1", 30000, 10);
            throw new AssertionError("Expected IllegalArgumentException for null queue");
        } catch (IllegalArgumentException e) {
            if (e.getMessage().contains("queue cannot be null or empty")) {
                System.out.println("✓ claimTasks validates null queue");
            } else {
                throw new AssertionError("Wrong error message: " + e.getMessage());
            }
        }
        
        try {
            dbClient.claimTasks("test-queue", null, 30000, 10);
            throw new AssertionError("Expected IllegalArgumentException for null workerId");
        } catch (IllegalArgumentException e) {
            if (e.getMessage().contains("workerId cannot be null or empty")) {
                System.out.println("✓ claimTasks validates null workerId");
            } else {
                throw new AssertionError("Wrong error message: " + e.getMessage());
            }
        }
        
        try {
            dbClient.claimTasks("test-queue", "worker1", 0, 10);
            throw new AssertionError("Expected IllegalArgumentException for zero claimTimeout");
        } catch (IllegalArgumentException e) {
            if (e.getMessage().contains("claimTimeout must be positive")) {
                System.out.println("✓ claimTasks validates positive claimTimeout");
            } else {
                throw new AssertionError("Wrong error message: " + e.getMessage());
            }
        }
        
        try {
            dbClient.claimTasks("test-queue", "worker1", 30000, 0);
            throw new AssertionError("Expected IllegalArgumentException for zero batchSize");
        } catch (IllegalArgumentException e) {
            if (e.getMessage().contains("batchSize must be positive")) {
                System.out.println("✓ claimTasks validates positive batchSize");
            } else {
                throw new AssertionError("Wrong error message: " + e.getMessage());
            }
        }
        
        // Test spawnTask validation
        try {
            dbClient.spawnTask(null, "task1", "{}", "{}", null, null);
            throw new AssertionError("Expected IllegalArgumentException for null queue");
        } catch (IllegalArgumentException e) {
            if (e.getMessage().contains("queue cannot be null or empty")) {
                System.out.println("✓ spawnTask validates null queue");
            } else {
                throw new AssertionError("Wrong error message: " + e.getMessage());
            }
        }
        
        try {
            dbClient.spawnTask("test-queue", null, "{}", "{}", null, null);
            throw new AssertionError("Expected IllegalArgumentException for null taskName");
        } catch (IllegalArgumentException e) {
            if (e.getMessage().contains("taskName cannot be null or empty")) {
                System.out.println("✓ spawnTask validates null taskName");
            } else {
                throw new AssertionError("Wrong error message: " + e.getMessage());
            }
        }
        
        try {
            dbClient.spawnTask("test-queue", "task1", null, "{}", null, null);
            throw new AssertionError("Expected IllegalArgumentException for null input");
        } catch (IllegalArgumentException e) {
            if (e.getMessage().contains("input cannot be null")) {
                System.out.println("✓ spawnTask validates JSON input");
            } else {
                throw new AssertionError("Wrong error message: " + e.getMessage());
            }
        }
        
        System.out.println("✓ All validation tests passed");
    }
    
    /**
     * Mock DataSource for testing without actual database connection.
     */
    static class MockDataSource implements DataSource {
        @Override
        public Connection getConnection() throws SQLException {
            throw new SQLException("Mock connection - not implemented");
        }
        
        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            throw new SQLException("Mock connection - not implemented");
        }
        
        @Override
        public PrintWriter getLogWriter() throws SQLException {
            throw new SQLException("Mock - not implemented");
        }
        
        @Override
        public void setLogWriter(PrintWriter out) throws SQLException {
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
        public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException("Mock - not implemented");
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