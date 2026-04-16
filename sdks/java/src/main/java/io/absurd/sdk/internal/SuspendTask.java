package io.absurd.sdk.internal;

import io.absurd.sdk.AbsurdException;

/**
 * Exception thrown when a task needs to be suspended (e.g., for sleep or events).
 * This indicates that the task should be persisted and resumed later.
 */
public class SuspendTask extends AbsurdException {
    
    /**
     * Creates a new SuspendTask exception.
     * 
     * @param message the suspension message
     */
    public SuspendTask(String message) {
        super(message);
    }
    
    /**
     * Creates a new SuspendTask exception with a cause.
     * 
     * @param message the suspension message
     * @param cause the cause of the suspension
     */
    public SuspendTask(String message, Throwable cause) {
        super(message, cause);
    }
}