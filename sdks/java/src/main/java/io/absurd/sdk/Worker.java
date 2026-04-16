package io.absurd.sdk;

/**
 * Worker that processes tasks from a queue.
 */
public interface Worker {
    
    /**
     * Starts the worker to begin processing tasks.
     */
    void start();
    
    /**
     * Stops the worker gracefully.
     */
    void stop();
    
    /**
     * Checks if the worker is currently running.
     * @return true if the worker is running
     */
    boolean isRunning();
    
    /**
     * Gets the options this worker was configured with.
     * @return the worker options
     */
    WorkerOptions getOptions();
}
