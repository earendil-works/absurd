package io.absurd.sdk.internal;

import io.absurd.sdk.AbsurdException;

/**
 * Exception thrown when a task has failed.
 * This indicates that the task execution encountered an error.
 */
public class FailedTask extends AbsurdException {
    
    /**
     * Creates a new FailedTask exception.
     * 
     * @param message the failure message
     */
    public FailedTask(String message) {
        super(message);
    }
    
    /**
     * Creates a new FailedTask exception with a cause.
     * 
     * @param message the failure message
     * @param cause the cause of the failure
     */
    public FailedTask(String message, Throwable cause) {
        super(message, cause);
    }
}