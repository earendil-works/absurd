package io.absurd.sdk;

/**
 * Options for creating a new queue.
 */
public class CreateQueueOptions {
    private final String queueName;
    private final RetryStrategy retryStrategy;
    private final CancellationPolicy cancellationPolicy;
    
    private CreateQueueOptions(Builder builder) {
        this.queueName = builder.queueName;
        this.retryStrategy = builder.retryStrategy;
        this.cancellationPolicy = builder.cancellationPolicy;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public String getQueueName() {
        return queueName;
    }
    
    public RetryStrategy getRetryStrategy() {
        return retryStrategy;
    }
    
    public CancellationPolicy getCancellationPolicy() {
        return cancellationPolicy;
    }
    
    public static class Builder {
        private String queueName;
        private RetryStrategy retryStrategy;
        private CancellationPolicy cancellationPolicy;
        
        public Builder queueName(String queueName) {
            this.queueName = queueName;
            return this;
        }
        
        public Builder retryStrategy(RetryStrategy retryStrategy) {
            this.retryStrategy = retryStrategy;
            return this;
        }
        
        public Builder cancellationPolicy(CancellationPolicy cancellationPolicy) {
            this.cancellationPolicy = cancellationPolicy;
            return this;
        }
        
        public CreateQueueOptions build() {
            if (queueName == null || queueName.trim().isEmpty()) {
                throw new IllegalStateException("queueName must be specified");
            }
            return new CreateQueueOptions(this);
        }
    }
}
