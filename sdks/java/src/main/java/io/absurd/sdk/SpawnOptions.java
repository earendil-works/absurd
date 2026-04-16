package io.absurd.sdk;

import java.util.Map;

/**
 * Options for spawning a new task.
 */
public class SpawnOptions {
    private final String queueName;
    private final String taskName;
    private final String input;
    private final Map<String, String> metadata;
    private final String parentRunId;
    private final String cronSchedule;
    
    private SpawnOptions(Builder builder) {
        this.queueName = builder.queueName;
        this.taskName = builder.taskName;
        this.input = builder.input;
        this.metadata = builder.metadata;
        this.parentRunId = builder.parentRunId;
        this.cronSchedule = builder.cronSchedule;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public String getQueueName() {
        return queueName;
    }
    
    public String getTaskName() {
        return taskName;
    }
    
    public String getInput() {
        return input;
    }
    
    public Map<String, String> getMetadata() {
        return metadata;
    }
    
    public String getParentRunId() {
        return parentRunId;
    }
    
    public String getCronSchedule() {
        return cronSchedule;
    }
    
    public static class Builder {
        private String queueName;
        private String taskName;
        private String input;
        private Map<String, String> metadata;
        private String parentRunId;
        private String cronSchedule;
        
        public Builder queueName(String queueName) {
            this.queueName = queueName;
            return this;
        }
        
        public Builder taskName(String taskName) {
            this.taskName = taskName;
            return this;
        }
        
        public Builder input(String input) {
            this.input = input;
            return this;
        }
        
        public Builder metadata(Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }
        
        public Builder parentRunId(String parentRunId) {
            this.parentRunId = parentRunId;
            return this;
        }
        
        public Builder cronSchedule(String cronSchedule) {
            this.cronSchedule = cronSchedule;
            return this;
        }
        
        public SpawnOptions build() {
            return new SpawnOptions(this);
        }
    }
}
