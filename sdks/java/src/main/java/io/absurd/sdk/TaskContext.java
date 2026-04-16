package io.absurd.sdk;

import java.util.Map;

/**
 * Context provided to a task during execution.
 */
public interface TaskContext {
    
    /**
     * Gets the unique identifier of the current task run.
     * @return the run ID
     */
    String getRunId();
    
    /**
     * Gets the name of the queue this task is running in.
     * @return the queue name
     */
    String getQueueName();
    
    /**
     * Gets the name of the task being executed.
     * @return the task name
     */
    String getTaskName();
    
    /**
     * Gets the current attempt number (1-based).
     * @return the attempt number
     */
    int getAttempt();
    
    /**
     * Gets the maximum number of attempts allowed for this task.
     * @return the maximum attempts
     */
    int getMaxAttempts();
    
    /**
     * Gets metadata associated with this task run.
     * @return the metadata map
     */
    Map<String, String> getMetadata();
    
    /**
     * Spawns a new task for execution.
     * 
     * @param options the spawn options
     * @return the spawn result
     */
    SpawnResult spawn(SpawnOptions options);
    
    /**
     * Spawns a new task for execution with default options.
     * 
     * @param taskName the name of the task to spawn
     * @param input the input for the spawned task
     * @return the spawn result
     */
    SpawnResult spawn(String taskName, String input);
    
    /**
     * Checks if the task has been cancelled.
     * @return true if the task has been cancelled
     */
    boolean isCancelled();
    
    /**
     * Logs a message at INFO level.
     * @param message the message to log
     */
    void logInfo(String message);
    
    /**
     * Logs a message at ERROR level.
     * @param message the message to log
     */
    void logError(String message);
    
    /**
     * Logs a message at ERROR level with an exception.
     * @param message the message to log
     * @param throwable the exception to log
     */
    void logError(String message, Throwable throwable);
}
