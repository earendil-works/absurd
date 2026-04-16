package io.absurd.sdk;

/**
 * Definition of a task that can be executed by the Absurd workflow system.
 */
public interface TaskDefinition<T, R> {
    
    /**
     * Gets the name of the task.
     * @return the task name
     */
    String getName();
    
    /**
     * Gets the handler that executes the task logic.
     * @return the task handler
     */
    TaskHandler<T, R> getHandler();
    
    /**
     * Gets the retry strategy for this task.
     * @return the retry strategy, or null if using default
     * @see Absurd#getDefaultRetryStrategy() for default implementation
     */
    RetryStrategy getRetryStrategy();
    
    /**
     * Gets the cancellation policy for this task.
     * @return the cancellation policy, or null if using default
     * @see Absurd#getDefaultCancellationPolicy() for default implementation
     */
    CancellationPolicy getCancellationPolicy();
}
