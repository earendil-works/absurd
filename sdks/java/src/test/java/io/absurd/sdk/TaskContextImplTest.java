package io.absurd.sdk;

import io.absurd.sdk.internal.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TaskContextImplTest {

    @Mock
    private DbClient dbClient;

    @Mock
    private DbClient.ClaimedTask claimedTask;

    @Test
    void testTaskContextCreation() {
        when(claimedTask.runId()).thenReturn("run123");
        when(claimedTask.taskName()).thenReturn("test-task");
        when(claimedTask.attempt()).thenReturn(1);
        when(claimedTask.input()).thenReturn("test-input");

        TaskContextImpl context = new TaskContextImpl(dbClient, claimedTask, "test-queue", Map.of("key", "value"));

        assertEquals("run123", context.getRunId());
        assertEquals("test-queue", context.getQueueName());
        assertEquals("test-task", context.getTaskName());
        assertEquals(1, context.getAttempt());
        assertEquals(3, context.getMaxAttempts());
        assertEquals("value", context.getMetadata().get("key"));
    }

    @Test
    void testSpawn() throws Exception {
        when(claimedTask.runId()).thenReturn("run123");
        when(claimedTask.taskName()).thenReturn("test-task");
        when(claimedTask.attempt()).thenReturn(1);
        when(claimedTask.input()).thenReturn("test-input");
        when(dbClient.spawnTask(anyString(), anyString(), anyString(), anyString(), anyString(), anyString()))
            .thenReturn("new-run-456");

        TaskContextImpl context = new TaskContextImpl(dbClient, claimedTask, "test-queue", Map.of());
        
        SpawnResult result = context.spawn("other-task", "{}");
        
        assertEquals("new-run-456", result.getRunId());
        assertEquals("test-queue", result.getQueueName());
        assertEquals("other-task", result.getTaskName());
    }

    @Test
    void testIsCancelled() throws Exception {
        when(claimedTask.runId()).thenReturn("run123");
        when(claimedTask.taskName()).thenReturn("test-task");
        when(claimedTask.attempt()).thenReturn(1);
        when(claimedTask.input()).thenReturn("test-input");
        when(dbClient.getTaskState(anyString(), anyString())).thenReturn(TaskState.CANCELLED);

        TaskContextImpl context = new TaskContextImpl(dbClient, claimedTask, "test-queue", Map.of());
        
        assertTrue(context.isCancelled());
        verify(dbClient).getTaskState("test-queue", "run123");
    }

    @Test
    void testStepCheckpointPattern() throws Exception {
        when(claimedTask.runId()).thenReturn("run123");
        when(claimedTask.taskName()).thenReturn("test-task");
        when(claimedTask.attempt()).thenReturn(1);
        when(claimedTask.input()).thenReturn("test-input");

        TaskContextImpl context = new TaskContextImpl(dbClient, claimedTask, "test-queue", Map.of());
        
        // First call should execute the function
        String result1 = context.step("test-step", "test-type", () -> "result1");
        assertEquals("result1", result1);
        
        // Second call with same step name should return cached result (simulating resume)
        // Note: In real usage, this would be across different task executions
        String result2 = context.step("test-step", "test-type", () -> "result2");
        assertEquals("result1", result2); // Returns cached result
    }

    @Test
    void testSleepFor() {
        when(claimedTask.runId()).thenReturn("run123");
        when(claimedTask.taskName()).thenReturn("test-task");
        when(claimedTask.attempt()).thenReturn(1);
        when(claimedTask.input()).thenReturn("test-input");

        TaskContextImpl context = new TaskContextImpl(dbClient, claimedTask, "test-queue", Map.of());
        
        SuspendTask exception = assertThrows(SuspendTask.class, () -> {
            context.sleepFor(Duration.ofSeconds(10));
        });
        
        assertTrue(exception.getMessage().contains("Task suspended until"));
    }

    @Test
    void testSleepUntil() {
        when(claimedTask.runId()).thenReturn("run123");
        when(claimedTask.taskName()).thenReturn("test-task");
        when(claimedTask.attempt()).thenReturn(1);
        when(claimedTask.input()).thenReturn("test-input");

        TaskContextImpl context = new TaskContextImpl(dbClient, claimedTask, "test-queue", Map.of());
        Instant wakeTime = Instant.now().plusSeconds(60);
        
        SuspendTask exception = assertThrows(SuspendTask.class, () -> {
            context.sleepUntil(wakeTime);
        });
        
        assertTrue(exception.getMessage().contains("Task suspended until"));
    }

    @Test
    void testAwaitEvent() {
        when(claimedTask.runId()).thenReturn("run123");
        when(claimedTask.taskName()).thenReturn("test-task");
        when(claimedTask.attempt()).thenReturn(1);
        when(claimedTask.input()).thenReturn("test-input");

        TaskContextImpl context = new TaskContextImpl(dbClient, claimedTask, "test-queue", Map.of());
        
        SuspendTask exception = assertThrows(SuspendTask.class, () -> {
            context.awaitEvent("test-event", Duration.ofSeconds(30));
        });
        
        assertTrue(exception.getMessage().contains("Task suspended waiting for event"));
    }

    @Test
    void testHeartbeat() throws Exception {
        when(claimedTask.runId()).thenReturn("run123");
        when(claimedTask.taskName()).thenReturn("test-task");
        when(claimedTask.attempt()).thenReturn(1);
        when(claimedTask.input()).thenReturn("test-input");

        TaskContextImpl context = new TaskContextImpl(dbClient, claimedTask, "test-queue", Map.of());
        
        context.heartbeat();
        verify(dbClient).heartbeatTask("test-queue", "run123");
    }

    @Test
    void testAwaitTaskResult() {
        when(claimedTask.runId()).thenReturn("run123");
        when(claimedTask.taskName()).thenReturn("test-task");
        when(claimedTask.attempt()).thenReturn(1);
        when(claimedTask.input()).thenReturn("test-input");

        TaskContextImpl context = new TaskContextImpl(dbClient, claimedTask, "test-queue", Map.of());
        
        SuspendTask exception = assertThrows(SuspendTask.class, () -> {
            context.awaitTaskResult("other-run-456", Duration.ofSeconds(30));
        });
        
        assertTrue(exception.getMessage().contains("Task suspended waiting for result of"));
    }
}