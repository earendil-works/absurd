package io.absurd.sdk;

/**
 * Functional interface for handling task execution.
 * 
 * @param <T> the input type
 * @param <R> the result type
 */
@FunctionalInterface
public interface TaskHandler<T, R> {
    
    /**
     * Executes the task with the given input and context.
     * 
     * @param input the task input
     * @param context the task execution context
     * @return the task result
     * @throws Exception if the task execution fails
     */
    R handle(T input, TaskContext context) throws Exception;
}
