package io.absurd.sdk;

/**
 * Configuration for retry behavior when tasks fail.
 */
public class RetryStrategy {
    private final int maxAttempts;
    private final long initialDelayMs;
    private final double backoffFactor;
    private final long maxDelayMs;
    
    public RetryStrategy(int maxAttempts, long initialDelayMs, double backoffFactor) {
        this(maxAttempts, initialDelayMs, backoffFactor, Long.MAX_VALUE);
    }
    
    public RetryStrategy(int maxAttempts, long initialDelayMs, double backoffFactor, long maxDelayMs) {
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException("maxAttempts must be positive");
        }
        if (initialDelayMs < 0) {
            throw new IllegalArgumentException("initialDelayMs cannot be negative");
        }
        if (backoffFactor <= 0) {
            throw new IllegalArgumentException("backoffFactor must be positive");
        }
        if (maxDelayMs < 0) {
            throw new IllegalArgumentException("maxDelayMs cannot be negative");
        }
        
        this.maxAttempts = maxAttempts;
        this.initialDelayMs = initialDelayMs;
        this.backoffFactor = backoffFactor;
        this.maxDelayMs = maxDelayMs;
    }
    
    public int getMaxAttempts() {
        return maxAttempts;
    }
    
    public long getInitialDelayMs() {
        return initialDelayMs;
    }
    
    public double getBackoffFactor() {
        return backoffFactor;
    }
    
    public long getMaxDelayMs() {
        return maxDelayMs;
    }
}
