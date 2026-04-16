package io.absurd.sdk;

import java.time.Instant;

/**
 * Snapshot of a task result at a point in time.
 */
public class TaskResultSnapshot {
    private final String runId;
    private final String queueName;
    private final String taskName;
    private final TaskState state;
    private final String result;
    private final String error;
    private final int attempt;
    private final Instant createdAt;
    private final Instant updatedAt;
    private final Instant completedAt;
    
    public TaskResultSnapshot(String runId, String queueName, String taskName, TaskState state, 
                             String result, String error, int attempt, Instant createdAt, 
                             Instant updatedAt, Instant completedAt) {
        this.runId = runId;
        this.queueName = queueName;
        this.taskName = taskName;
        this.state = state;
        this.result = result;
        this.error = error;
        this.attempt = attempt;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.completedAt = completedAt;
    }
    
    /**
     * Gets the unique identifier of the task run.
     * @return the run ID
     */
    public String getRunId() {
        return runId;
    }
    
    /**
     * Gets the name of the queue this task ran in.
     * @return the queue name
     */
    public String getQueueName() {
        return queueName;
    }
    
    /**
     * Gets the name of the task.
     * @return the task name
     */
    public String getTaskName() {
        return taskName;
    }
    
    /**
     * Gets the current state of the task.
     * @return the task state
     */
    public TaskState getState() {
        return state;
    }
    
    /**
     * Gets the result output of the task (if completed).
     * @return the result, or null if not completed
     */
    public String getResult() {
        return result;
    }
    
    /**
     * Gets the error message (if failed).
     * @return the error message, or null if not failed
     */
    public String getError() {
        return error;
    }
    
    /**
     * Gets the attempt number.
     * @return the attempt number
     */
    public int getAttempt() {
        return attempt;
    }
    
    /**
     * Gets when the task was created.
     * @return the creation timestamp
     */
    public Instant getCreatedAt() {
        return createdAt;
    }
    
    /**
     * Gets when the task was last updated.
     * @return the last update timestamp
     */
    public Instant getUpdatedAt() {
        return updatedAt;
    }
    
    /**
     * Gets when the task was completed (if completed).
     * @return the completion timestamp, or null if not completed
     */
    public Instant getCompletedAt() {
        return completedAt;
    }
}
