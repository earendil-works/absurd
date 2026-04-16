package io.absurd.sdk;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Main client for interacting with the Absurd workflow system.
 */
public class Absurd {
    private final String connectionString;
    private final Map<String, TaskDefinition<?, ?>> taskDefinitions;
    private final AbsurdHooks hooks;
    
    private Absurd(Builder builder) {
        this.connectionString = builder.connectionString;
        this.taskDefinitions = new ConcurrentHashMap<>(builder.taskDefinitions);
        this.hooks = builder.hooks != null ? builder.hooks : new DefaultAbsurdHooks();
    }
    
    /**
     * Creates a new Absurd builder.
     * 
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Gets the database connection string.
     * 
     * @return the connection string
     */
    public String getConnectionString() {
        return connectionString;
    }
    
    /**
     * Gets the registered task definitions.
     * 
     * @return the task definitions map
     */
    public Map<String, TaskDefinition<?, ?>> getTaskDefinitions() {
        return taskDefinitions;
    }
    
    /**
     * Gets the hooks for customizing behavior.
     * 
     * @return the hooks
     */
    public AbsurdHooks getHooks() {
        return hooks;
    }
    
    /**
     * Gets the default retry strategy used when task definitions don't specify one.
     * 
     * @return the default retry strategy
     */
    public RetryStrategy getDefaultRetryStrategy() {
        return new RetryStrategy(3, 1000, 2.0);
    }
    
    /**
     * Gets the default cancellation policy used when task definitions don't specify one.
     * 
     * @return the default cancellation policy
     */
    public CancellationPolicy getDefaultCancellationPolicy() {
        return new CancellationPolicy(30000, true);
    }
    
    /**
     * Registers a task definition.
     * 
     * @param definition the task definition to register
     */
    public void registerTask(TaskDefinition<?, ?> definition) {
        taskDefinitions.put(definition.getName(), definition);
    }
    
    /**
     * Creates a new worker for the specified queue.
     * 
     * @param queueName the name of the queue to work on
     * @return a new worker instance
     */
    public Worker createWorker(String queueName) {
        return createWorker(WorkerOptions.builder()
                .queueName(queueName)
                .build());
    }
    
    /**
     * Creates a new worker with the specified options.
     * 
     * @param options the worker options
     * @return a new worker instance
     */
    public Worker createWorker(WorkerOptions options) {
        // TODO: Implement actual worker creation
        throw new UnsupportedOperationException("Worker creation not yet implemented");
    }
    
    /**
     * Spawns a new task for execution.
     * 
     * @param options the spawn options
     * @return the spawn result
     */
    public SpawnResult spawn(SpawnOptions options) {
        // TODO: Implement actual task spawning
        throw new UnsupportedOperationException("Task spawning not yet implemented");
    }
    
    /**
     * Spawns a new task with minimal options.
     * 
     * @param queueName the queue name
     * @param taskName the task name
     * @param input the task input
     * @return the spawn result
     */
    public SpawnResult spawn(String queueName, String taskName, String input) {
        return spawn(SpawnOptions.builder()
                .queueName(queueName)
                .taskName(taskName)
                .input(input)
                .build());
    }
    
    /**
     * Creates a new queue with the specified options.
     * 
     * @param options the queue creation options
     */
    public void createQueue(CreateQueueOptions options) {
        // TODO: Implement queue creation
        throw new UnsupportedOperationException("Queue creation not yet implemented");
    }
    
    /**
     * Builder for Absurd client.
     */
    public static class Builder {
        private String connectionString;
        private final Map<String, TaskDefinition<?, ?>> taskDefinitions = new ConcurrentHashMap<>();
        private AbsurdHooks hooks;
        
        public Builder connectionString(String connectionString) {
            this.connectionString = connectionString;
            return this;
        }
        
        public Builder registerTask(TaskDefinition<?, ?> definition) {
            this.taskDefinitions.put(definition.getName(), definition);
            return this;
        }
        
        public Builder hooks(AbsurdHooks hooks) {
            this.hooks = hooks;
            return this;
        }
        
        public Absurd build() {
            if (connectionString == null || connectionString.trim().isEmpty()) {
                throw new IllegalStateException("Connection string must be specified");
            }
            if (taskDefinitions.isEmpty()) {
                throw new IllegalStateException("At least one task definition must be registered");
            }
            return new Absurd(this);
        }
    }
    
    /**
     * Default implementation of AbsurdHooks with no-op methods.
     */
    private static class DefaultAbsurdHooks implements AbsurdHooks {
        // All methods are already implemented as defaults in the interface
    }
}
