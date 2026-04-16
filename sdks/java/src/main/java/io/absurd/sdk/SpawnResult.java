package io.absurd.sdk;

/**
 * Result of spawning a new task.
 */
public class SpawnResult {
    private final String runId;
    private final String queueName;
    private final String taskName;
    
    public SpawnResult(String runId, String queueName, String taskName) {
        this.runId = runId;
        this.queueName = queueName;
        this.taskName = taskName;
    }
    
    /**
     * Gets the unique identifier of the spawned task run.
     * @return the run ID
     */
    public String getRunId() {
        return runId;
    }
    
    /**
     * Gets the name of the queue the task was spawned in.
     * @return the queue name
     */
    public String getQueueName() {
        return queueName;
    }
    
    /**
     * Gets the name of the spawned task.
     * @return the task name
     */
    public String getTaskName() {
        return taskName;
    }
}
