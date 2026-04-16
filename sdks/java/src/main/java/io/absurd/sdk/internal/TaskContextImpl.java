package io.absurd.sdk.internal;

import io.absurd.sdk.*;
import io.absurd.sdk.internal.DbClient.ClaimedTask;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of TaskContext interface that provides step/sleep/event functionality.
 */
public class TaskContextImpl implements TaskContext {
    private final DbClient dbClient;
    private final ClaimedTask claimedTask;
    private final String queueName;
    private final Map<String, String> metadata;
    private final Map<String, Object> checkpointCache;
    private int stepCounter = 0;
    
    /**
     * Creates a new TaskContextImpl.
     * 
     * @param dbClient the database client
     * @param claimedTask the claimed task information
     * @param queueName the queue name
     * @param metadata the task metadata
     */
    public TaskContextImpl(DbClient dbClient, ClaimedTask claimedTask, String queueName, Map<String, String> metadata) {
        this.dbClient = dbClient;
        this.claimedTask = claimedTask;
        this.queueName = queueName;
        this.metadata = metadata != null ? new HashMap<>(metadata) : new HashMap<>();
        this.checkpointCache = new ConcurrentHashMap<>();
    }
    
    @Override
    public String getRunId() {
        return claimedTask.runId();
    }
    
    @Override
    public String getQueueName() {
        return queueName;
    }
    
    @Override
    public String getTaskName() {
        return claimedTask.taskName();
    }
    
    @Override
    public int getAttempt() {
        return claimedTask.attempt();
    }
    
    @Override
    public int getMaxAttempts() {
        // This would come from the task definition in a real implementation
        return 3; // Default value
    }
    
    @Override
    public Map<String, String> getMetadata() {
        return new HashMap<>(metadata);
    }
    
    @Override
    public SpawnResult spawn(SpawnOptions options) {
        try {
            String runId = dbClient.spawnTask(
                options.getQueueName(),
                options.getTaskName(),
                options.getInput(),
                "{}", // metadata as JSON
                options.getParentRunId(),
                options.getCronSchedule()
            );
            return new SpawnResult(runId, options.getQueueName(), options.getTaskName());
        } catch (AbsurdException e) {
            throw new RuntimeException("Failed to spawn task: " + e.getMessage(), e);
        }
    }
    
    @Override
    public SpawnResult spawn(String taskName, String input) {
        SpawnOptions options = SpawnOptions.builder()
                .queueName(queueName)
                .taskName(taskName)
                .input(input)
                .build();
        return spawn(options);
    }
    
    @Override
    public boolean isCancelled() {
        try {
            TaskState state = dbClient.getTaskState(queueName, getRunId());
            return state == TaskState.CANCELLED;
        } catch (AbsurdException e) {
            // If we can't check the state, assume not cancelled
            return false;
        }
    }
    
    @Override
    public void logInfo(String message) {
        System.out.println("[INFO] " + message);
    }
    
    @Override
    public void logError(String message) {
        System.err.println("[ERROR] " + message);
    }
    
    @Override
    public void logError(String message, Throwable throwable) {
        System.err.println("[ERROR] " + message);
        throwable.printStackTrace();
    }
    
    /**
     * Executes a step with checkpoint/resume pattern.
     * 
     * @param name the step name
     * @param type the step type
     * @param fn the function to execute
     * @param <T> the return type
     * @return the result of the function
     * @throws SuspendTask if the task should be suspended
     */
    public <T> T step(String name, String type, CheckpointFunction<T> fn) throws SuspendTask {
        String checkpointKey = "step#" + (++stepCounter) + "-" + name;
        
        // Check if we're resuming from this checkpoint
        if (checkpointCache.containsKey(checkpointKey)) {
            @SuppressWarnings("unchecked")
            T cachedResult = (T) checkpointCache.get(checkpointKey);
            return cachedResult;
        }
        
        try {
            // Execute the function
            T result = fn.execute();
            
            // Cache the result for potential resume
            checkpointCache.put(checkpointKey, result);
            
            return result;
        } catch (Exception e) {
            throw new RuntimeException("Step execution failed: " + e.getMessage(), e);
        }
    }
    
    /**
     * Suspends the task for a specified duration.
     * 
     * @param duration the duration to sleep
     * @throws SuspendTask always thrown to suspend the task
     */
    public void sleepFor(Duration duration) throws SuspendTask {
        Instant wakeTime = Instant.now().plus(duration);
        sleepUntil(wakeTime);
    }
    
    /**
     * Suspends the task until a specific time.
     * 
     * @param wakeTime the time to wake up
     * @throws SuspendTask always thrown to suspend the task
     */
    public void sleepUntil(Instant wakeTime) throws SuspendTask {
        // In a real implementation, this would:
        // 1. Persist the wake time in the database
        // 2. Schedule the task to run again at wakeTime
        // 3. Throw SuspendTask to indicate suspension
        throw new SuspendTask("Task suspended until " + wakeTime);
    }
    
    /**
     * Awaits an event with optional timeout.
     * 
     * @param eventName the event name to wait for
     * @param timeout the maximum time to wait
     * @return the event data
     * @throws SuspendTask if the task should be suspended
     * @throws FailedTask if the timeout is reached
     */
    public String awaitEvent(String eventName, Duration timeout) throws SuspendTask, FailedTask {
        // In a real implementation, this would:
        // 1. Call absurd.await_event stored procedure
        // 2. Handle suspension if event not available
        // 3. Return event data if available
        // 4. Throw FailedTask if timeout reached
        
        // For now, simulate by throwing SuspendTask
        throw new SuspendTask("Task suspended waiting for event: " + eventName);
    }
    
    /**
     * Emits an event.
     * 
     * @param eventName the event name
     * @param eventData the event data
     */
    public void emitEvent(String eventName, String eventData) {
        // In a real implementation, this would call the appropriate stored procedure
        // to emit an event that other tasks can await
    }
    
    /**
     * Heartbeats the task to keep it alive.
     */
    public void heartbeat() {
        try {
            dbClient.heartbeatTask(queueName, getRunId());
        } catch (AbsurdException e) {
            throw new RuntimeException("Failed to heartbeat task: " + e.getMessage(), e);
        }
    }
    
    /**
     * Awaits the result of another task.
     * 
     * @param runId the run ID of the task to await
     * @param timeout the maximum time to wait
     * @return the task result
     * @throws SuspendTask if the task should be suspended
     * @throws FailedTask if the timeout is reached or the task failed
     */
    public String awaitTaskResult(String runId, Duration timeout) throws SuspendTask, FailedTask {
        // In a real implementation, this would:
        // 1. Check the state of the specified task
        // 2. Return the result if completed
        // 3. Suspend if still running
        // 4. Throw FailedTask if timeout reached or task failed
        
        // For now, simulate by throwing SuspendTask
        throw new SuspendTask("Task suspended waiting for result of: " + runId);
    }
    
    /**
     * Functional interface for checkpoint functions.
     * 
     * @param <T> the return type
     */
    @FunctionalInterface
    public interface CheckpointFunction<T> {
        T execute() throws Exception;
    }
}