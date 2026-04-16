package io.absurd.sdk.internal;

import io.absurd.sdk.AbsurdException;
import io.absurd.sdk.TaskState;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Internal JDBC wrapper for calling Absurd stored procedures.
 * 
 * <p>This class handles the low-level JDBC operations and provides
 * type-safe methods for interacting with Absurd's PostgreSQL stored procedures.</p>
 */
public class DbClient {
    private final DataSource dataSource;
    
    /**
     * Creates a new DbClient that wraps the given DataSource.
     * 
     * @param dataSource the data source to use for database connections
     * @throws IllegalArgumentException if dataSource is null
     */
    public DbClient(DataSource dataSource) {
        if (dataSource == null) {
            throw new IllegalArgumentException("dataSource cannot be null");
        }
        this.dataSource = dataSource;
    }
    
    /**
     * Validates that a string is valid JSON.
     * 
     * @param json the JSON string to validate
     * @param parameterName the name of the parameter for error messages
     * @throws IllegalArgumentException if the JSON is invalid
     */
    private void validateJson(String json, String parameterName) {
        if (json == null) {
            throw new IllegalArgumentException(parameterName + " cannot be null");
        }
        if (json.trim().isEmpty()) {
            throw new IllegalArgumentException(parameterName + " cannot be empty");
        }
        // Basic JSON validation - check if it starts and ends with valid JSON delimiters
        String trimmed = json.trim();
        if (!trimmed.startsWith("{") && !trimmed.startsWith("[") && !trimmed.equals("null")) {
            throw new IllegalArgumentException(parameterName + " must be valid JSON");
        }
        if (trimmed.startsWith("{") && !trimmed.endsWith("}")) {
            throw new IllegalArgumentException(parameterName + " must be valid JSON object");
        }
        if (trimmed.startsWith("[") && !trimmed.endsWith("]")) {
            throw new IllegalArgumentException(parameterName + " must be valid JSON array");
        }
    }
    
    /**
     * Helper record representing a claimed task.
     * 
     * @param runId the run ID of the claimed task
     * @param taskName the name of the task
     * @param input the input data for the task
     * @param attempt the current attempt number
     */
    public record ClaimedTask(String runId, String taskName, String input, int attempt) {}
    
    /**
     * Claims tasks from the queue for processing.
     * 
     * @param queue the queue name
     * @param workerId the worker identifier
     * @param claimTimeout the timeout in milliseconds for the claim
     * @param batchSize the maximum number of tasks to claim
     * @return list of claimed tasks
     * @throws AbsurdException if there's an error claiming tasks
     */
    public List<ClaimedTask> claimTasks(String queue, String workerId, int claimTimeout, int batchSize) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (workerId == null || workerId.trim().isEmpty()) {
            throw new IllegalArgumentException("workerId cannot be null or empty");
        }
        if (claimTimeout <= 0) {
            throw new IllegalArgumentException("claimTimeout must be positive");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive");
        }
        
        String sql = "CALL absurd.claim_tasks(?, ?, ?, ?)";
        
        try (Connection conn = dataSource.getConnection();
             CallableStatement stmt = conn.prepareCall(sql)) {
            
            stmt.setString(1, queue);
            stmt.setString(2, workerId);
            stmt.setInt(3, claimTimeout);
            stmt.setInt(4, batchSize);
            
            try (ResultSet rs = stmt.executeQuery()) {
                List<ClaimedTask> tasks = new ArrayList<>();
                while (rs.next()) {
                    tasks.add(new ClaimedTask(
                        rs.getString("run_id"),
                        rs.getString("task_name"),
                        rs.getString("input"),
                        rs.getInt("attempt")
                    ));
                }
                return tasks;
            }
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to claim tasks: " + e.getMessage(), e);
        }
    }
    
    /**
     * Spawns a new task in the queue.
     * 
     * @param queue the queue name
     * @param taskName the task name
     * @param input the input data as JSON
     * @param metadata optional metadata as JSON
     * @param parentRunId optional parent run ID
     * @param cronSchedule optional cron schedule
     * @return the run ID of the spawned task
     * @throws AbsurdException if there's an error spawning the task
     */
    public String spawnTask(String queue, String taskName, String input, String metadata, String parentRunId, String cronSchedule) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (taskName == null || taskName.trim().isEmpty()) {
            throw new IllegalArgumentException("taskName cannot be null or empty");
        }
        validateJson(input, "input");
        
        String sql = "SELECT absurd.spawn_task(?, ?, CAST(? AS jsonb), CAST(? AS jsonb), ?, ?)";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, queue);
            stmt.setString(2, taskName);
            stmt.setString(3, input);
            stmt.setString(4, metadata);
            stmt.setString(5, parentRunId);
            stmt.setString(6, cronSchedule);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
                throw new AbsurdException("No result returned from spawn_task");
            }
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to spawn task: " + e.getMessage(), e);
        }
    }
    
    /**
     * Completes a task run.
     * 
     * @param queue the queue name
     * @param runId the run ID to complete
     * @param output the output data as JSON
     * @throws AbsurdException if there's an error completing the task
     */
    public void completeTask(String queue, String runId, String output) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (runId == null || runId.trim().isEmpty()) {
            throw new IllegalArgumentException("runId cannot be null or empty");
        }
        validateJson(output, "output");
        
        String sql = "CALL absurd.complete_task(?, ?, CAST(? AS jsonb))";
        
        try (Connection conn = dataSource.getConnection();
             CallableStatement stmt = conn.prepareCall(sql)) {
            
            stmt.setString(1, queue);
            stmt.setString(2, runId);
            stmt.setString(3, output);
            
            stmt.execute();
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to complete task: " + e.getMessage(), e);
        }
    }
    
    /**
     * Fails a task run.
     * 
     * @param queue the queue name
     * @param runId the run ID to fail
     * @param error the error message
     * @throws AbsurdException if there's an error failing the task
     */
    public void failTask(String queue, String runId, String error) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (runId == null || runId.trim().isEmpty()) {
            throw new IllegalArgumentException("runId cannot be null or empty");
        }
        if (error == null || error.trim().isEmpty()) {
            throw new IllegalArgumentException("error cannot be null or empty");
        }
        
        String sql = "CALL absurd.fail_task(?, ?, ?)";
        
        try (Connection conn = dataSource.getConnection();
             CallableStatement stmt = conn.prepareCall(sql)) {
            
            stmt.setString(1, queue);
            stmt.setString(2, runId);
            stmt.setString(3, error);
            
            stmt.execute();
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to fail task: " + e.getMessage(), e);
        }
    }
    
    /**
     * Cancels a task run.
     * 
     * @param queue the queue name
     * @param runId the run ID to cancel
     * @param reason the cancellation reason
     * @throws AbsurdException if there's an error cancelling the task
     */
    public void cancelTask(String queue, String runId, String reason) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (runId == null || runId.trim().isEmpty()) {
            throw new IllegalArgumentException("runId cannot be null or empty");
        }
        if (reason == null || reason.trim().isEmpty()) {
            throw new IllegalArgumentException("reason cannot be null or empty");
        }
        
        String sql = "CALL absurd.cancel_task(?, ?, ?)";
        
        try (Connection conn = dataSource.getConnection();
             CallableStatement stmt = conn.prepareCall(sql)) {
            
            stmt.setString(1, queue);
            stmt.setString(2, runId);
            stmt.setString(3, reason);
            
            stmt.execute();
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to cancel task: " + e.getMessage(), e);
        }
    }
    
    /**
     * Gets the state of a task run.
     * 
     * @param queue the queue name
     * @param runId the run ID to check
     * @return the task state
     * @throws AbsurdException if there's an error getting the task state
     */
    public TaskState getTaskState(String queue, String runId) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (runId == null || runId.trim().isEmpty()) {
            throw new IllegalArgumentException("runId cannot be null or empty");
        }
        
        String sql = "SELECT absurd.get_task_state(?, ?)";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, queue);
            stmt.setString(2, runId);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String state = rs.getString(1);
                    return TaskState.valueOf(state);
                }
                throw new AbsurdException("No result returned from get_task_state");
            }
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to get task state: " + e.getMessage(), e);
        }
    }
    
    /**
     * Gets task result snapshot.
     * 
     * @param queue the queue name
     * @param runId the run ID to get results for
     * @return the task result as JSON string
     * @throws AbsurdException if there's an error getting the task result
     */
    public String getTaskResult(String queue, String runId) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (runId == null || runId.trim().isEmpty()) {
            throw new IllegalArgumentException("runId cannot be null or empty");
        }
        
        String sql = "SELECT absurd.get_task_result(?, ?)";
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, queue);
            stmt.setString(2, runId);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
                throw new AbsurdException("No result returned from get_task_result");
            }
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to get task result: " + e.getMessage(), e);
        }
    }
    
    /**
     * Creates a new queue.
     * 
     * @param queueName the name of the queue to create
     * @param retryStrategyJson retry strategy as JSON
     * @param cancellationPolicyJson cancellation policy as JSON
     * @throws AbsurdException if there's an error creating the queue
     */
    public void createQueue(String queueName, String retryStrategyJson, String cancellationPolicyJson) throws AbsurdException {
        if (queueName == null || queueName.trim().isEmpty()) {
            throw new IllegalArgumentException("queueName cannot be null or empty");
        }
        validateJson(retryStrategyJson, "retryStrategyJson");
        validateJson(cancellationPolicyJson, "cancellationPolicyJson");
        
        String sql = "CALL absurd.create_queue(?, CAST(? AS jsonb), CAST(? AS jsonb))";
        
        try (Connection conn = dataSource.getConnection();
             CallableStatement stmt = conn.prepareCall(sql)) {
            
            stmt.setString(1, queueName);
            stmt.setString(2, retryStrategyJson);
            stmt.setString(3, cancellationPolicyJson);
            
            stmt.execute();
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to create queue: " + e.getMessage(), e);
        }
    }
    
    /**
     * Heartbeats a task run to keep it alive.
     * 
     * @param queue the queue name
     * @param runId the run ID to heartbeat
     * @throws AbsurdException if there's an error heartbeat the task
     */
    public void heartbeatTask(String queue, String runId) throws AbsurdException {
        if (queue == null || queue.trim().isEmpty()) {
            throw new IllegalArgumentException("queue cannot be null or empty");
        }
        if (runId == null || runId.trim().isEmpty()) {
            throw new IllegalArgumentException("runId cannot be null or empty");
        }
        
        String sql = "CALL absurd.heartbeat_task(?, ?)";
        
        try (Connection conn = dataSource.getConnection();
             CallableStatement stmt = conn.prepareCall(sql)) {
            
            stmt.setString(1, queue);
            stmt.setString(2, runId);
            
            stmt.execute();
        } catch (SQLException e) {
            handleSQLError(e);
            throw new AbsurdException("Failed to heartbeat task: " + e.getMessage(), e);
        }
    }
    
    /**
     * Handles SQL errors and converts them to appropriate exceptions.
     * 
     * @param e the SQL exception to handle
     * @throws AbsurdException with appropriate error details
     */
    private void handleSQLError(SQLException e) throws AbsurdException {
        String sqlState = e.getSQLState();
        
        // Handle specific Absurd error codes
        if ("AB001".equals(sqlState)) {
            throw new AbsurdException("Task was cancelled: " + e.getMessage(), e);
        } else if ("AB002".equals(sqlState)) {
            throw new AbsurdException("Task failed: " + e.getMessage(), e);
        }
        
        // For other errors, just let them propagate with context
    }
}