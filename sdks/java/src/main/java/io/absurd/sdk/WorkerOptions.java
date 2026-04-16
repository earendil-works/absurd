package io.absurd.sdk;

/**
 * Options for configuring a worker.
 */
public class WorkerOptions {
    private final String queueName;
    private final int concurrency;
    private final int pollIntervalMs;
    private final int maxPollIntervalMs;
    private final int pollTimeoutMs;
    
    private WorkerOptions(Builder builder) {
        this.queueName = builder.queueName;
        this.concurrency = builder.concurrency;
        this.pollIntervalMs = builder.pollIntervalMs;
        this.maxPollIntervalMs = builder.maxPollIntervalMs;
        this.pollTimeoutMs = builder.pollTimeoutMs;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public String getQueueName() {
        return queueName;
    }
    
    public int getConcurrency() {
        return concurrency;
    }
    
    public int getPollIntervalMs() {
        return pollIntervalMs;
    }
    
    public int getMaxPollIntervalMs() {
        return maxPollIntervalMs;
    }
    
    public int getPollTimeoutMs() {
        return pollTimeoutMs;
    }
    
    public static class Builder {
        private String queueName;
        private int concurrency = 1;
        private int pollIntervalMs = 1000;
        private int maxPollIntervalMs = 30000;
        private int pollTimeoutMs = 30000;
        
        public Builder queueName(String queueName) {
            this.queueName = queueName;
            return this;
        }
        
        public Builder concurrency(int concurrency) {
            this.concurrency = concurrency;
            return this;
        }
        
        public Builder pollIntervalMs(int pollIntervalMs) {
            this.pollIntervalMs = pollIntervalMs;
            return this;
        }
        
        public Builder maxPollIntervalMs(int maxPollIntervalMs) {
            this.maxPollIntervalMs = maxPollIntervalMs;
            return this;
        }
        
        public Builder pollTimeoutMs(int pollTimeoutMs) {
            this.pollTimeoutMs = pollTimeoutMs;
            return this;
        }
        
        public WorkerOptions build() {
            if (queueName == null || queueName.trim().isEmpty()) {
                throw new IllegalStateException("queueName must be specified");
            }
            if (concurrency <= 0) {
                throw new IllegalStateException("concurrency must be positive");
            }
            if (pollIntervalMs <= 0) {
                throw new IllegalStateException("pollIntervalMs must be positive");
            }
            if (maxPollIntervalMs <= 0) {
                throw new IllegalStateException("maxPollIntervalMs must be positive");
            }
            if (pollTimeoutMs <= 0) {
                throw new IllegalStateException("pollTimeoutMs must be positive");
            }
            return new WorkerOptions(this);
        }
    }
}
