package io.absurd.sdk;

/**
 * Hooks for customizing Absurd behavior.
 */
public interface AbsurdHooks {
    
    /**
     * Called before a task is executed.
     * 
     * @param context the task context
     */
    default void beforeTaskExecution(TaskContext context) {
        // Default no-op implementation
    }
    
    /**
     * Called after a task completes successfully.
     * 
     * @param context the task context
     * @param result the task result
     */
    default void afterTaskSuccess(TaskContext context, Object result) {
        // Default no-op implementation
    }
    
    /**
     * Called when a task fails.
     * 
     * @param context the task context
     * @param error the exception that caused the failure
     */
    default void afterTaskFailure(TaskContext context, Throwable error) {
        // Default no-op implementation
    }
    
    /**
     * Called when a task is retried.
     * 
     * @param context the task context
     * @param error the exception that caused the retry
     * @param nextAttempt the next attempt number
     */
    default void onTaskRetry(TaskContext context, Throwable error, int nextAttempt) {
        // Default no-op implementation
    }
}
