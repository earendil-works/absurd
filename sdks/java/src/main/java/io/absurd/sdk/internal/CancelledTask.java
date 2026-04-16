package io.absurd.sdk.internal;

import io.absurd.sdk.AbsurdException;

/**
 * Exception thrown when a task has been cancelled.
 * This indicates that the task execution should be terminated.
 */
public class CancelledTask extends AbsurdException {
    
    /**
     * Creates a new CancelledTask exception.
     * 
     * @param message the cancellation message
     */
    public CancelledTask(String message) {
        super(message);
    }
    
    /**
     * Creates a new CancelledTask exception with a cause.
     * 
     * @param message the cancellation message
     * @param cause the cause of the cancellation
     */
    public CancelledTask(String message, Throwable cause) {
        super(message, cause);
    }
}