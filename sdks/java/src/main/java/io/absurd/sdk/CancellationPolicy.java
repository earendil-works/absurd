package io.absurd.sdk;

/**
 * Configuration for task cancellation behavior.
 */
public class CancellationPolicy {
    private final long timeoutMs;
    private final boolean interruptible;
    
    public CancellationPolicy(long timeoutMs, boolean interruptible) {
        if (timeoutMs < 0) {
            throw new IllegalArgumentException("timeoutMs cannot be negative");
        }
        
        this.timeoutMs = timeoutMs;
        this.interruptible = interruptible;
    }
    
    public long getTimeoutMs() {
        return timeoutMs;
    }
    
    public boolean isInterruptible() {
        return interruptible;
    }
}
