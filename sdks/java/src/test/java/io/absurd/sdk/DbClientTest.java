package io.absurd.sdk;

import io.absurd.sdk.internal.DbClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.sql.DataSource;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DbClientTest {

    @Mock
    private DataSource dataSource;

    @Mock
    private Connection connection;

    @Mock
    private CallableStatement callableStatement;

    @Mock
    private PreparedStatement preparedStatement;

    @Mock
    private ResultSet resultSet;

    @Test
    void testDbClientCreation() {
        when(dataSource.getConnection()).thenThrow(new SQLException("Test"));
        
        DbClient dbClient = new DbClient(dataSource);
        assertNotNull(dbClient);
    }

    @Test
    void testDbClientCreationWithNullDataSource() {
        assertThrows(IllegalArgumentException.class, () -> {
            new DbClient(null);
        });
    }

    @Test
    void testClaimTasks() throws Exception {
        // Setup mocks
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareCall(anyString())).thenReturn(callableStatement);
        when(callableStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true, true, false); // Two tasks
        when(resultSet.getString("run_id")).thenReturn("run1", "run2");
        when(resultSet.getString("task_name")).thenReturn("task1", "task2");
        when(resultSet.getString("input")).thenReturn("input1", "input2");
        when(resultSet.getInt("attempt")).thenReturn(1, 2);

        DbClient dbClient = new DbClient(dataSource);
        var tasks = dbClient.claimTasks("test-queue", "worker1", 30000, 10);

        assertEquals(2, tasks.size());
        assertEquals("run1", tasks.get(0).runId());
        assertEquals("task1", tasks.get(0).taskName());
        assertEquals("input1", tasks.get(0).input());
        assertEquals(1, tasks.get(0).attempt());

        verify(callableStatement).setString(1, "test-queue");
        verify(callableStatement).setString(2, "worker1");
        verify(callableStatement).setInt(3, 30000);
        verify(callableStatement).setInt(4, 10);
    }

    @Test
    void testSpawnTask() throws Exception {
        // Setup mocks
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString(1)).thenReturn("run123");

        DbClient dbClient = new DbClient(dataSource);
        String runId = dbClient.spawnTask("test-queue", "test-task", "{}", "{}", null, null);

        assertEquals("run123", runId);
        verify(preparedStatement).setString(1, "test-queue");
        verify(preparedStatement).setString(2, "test-task");
    }

    @Test
    void testCompleteTask() throws Exception {
        // Setup mocks
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareCall(anyString())).thenReturn(callableStatement);

        DbClient dbClient = new DbClient(dataSource);
        dbClient.completeTask("test-queue", "run123", "{}");

        verify(callableStatement).setString(1, "test-queue");
        verify(callableStatement).setString(2, "run123");
        verify(callableStatement).setString(3, "{}");
        verify(callableStatement).execute();
    }

    @Test
    void testFailTask() throws Exception {
        // Setup mocks
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareCall(anyString())).thenReturn(callableStatement);

        DbClient dbClient = new DbClient(dataSource);
        dbClient.failTask("test-queue", "run123", "error message");

        verify(callableStatement).setString(1, "test-queue");
        verify(callableStatement).setString(2, "run123");
        verify(callableStatement).setString(3, "error message");
        verify(callableStatement).execute();
    }

    @Test
    void testCancelTask() throws Exception {
        // Setup mocks
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareCall(anyString())).thenReturn(callableStatement);

        DbClient dbClient = new DbClient(dataSource);
        dbClient.cancelTask("test-queue", "run123", "user requested");

        verify(callableStatement).setString(1, "test-queue");
        verify(callableStatement).setString(2, "run123");
        verify(callableStatement).setString(3, "user requested");
        verify(callableStatement).execute();
    }

    @Test
    void testGetTaskState() throws Exception {
        // Setup mocks
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString(1)).thenReturn("RUNNING");

        DbClient dbClient = new DbClient(dataSource);
        TaskState state = dbClient.getTaskState("test-queue", "run123");

        assertEquals(TaskState.RUNNING, state);
    }

    @Test
    void testGetTaskResult() throws Exception {
        // Setup mocks
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString(1)).thenReturn("{\"result\": \"success\"}");

        DbClient dbClient = new DbClient(dataSource);
        String result = dbClient.getTaskResult("test-queue", "run123");

        assertEquals("{\"result\": \"success\"}", result);
    }

    @Test
    void testCreateQueue() throws Exception {
        // Setup mocks
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareCall(anyString())).thenReturn(callableStatement);

        DbClient dbClient = new DbClient(dataSource);
        dbClient.createQueue("test-queue", "{\"maxAttempts\": 3}", "{\"timeoutMs\": 30000}");

        verify(callableStatement).setString(1, "test-queue");
        verify(callableStatement).setString(2, "{\"maxAttempts\": 3}");
        verify(callableStatement).setString(3, "{\"timeoutMs\": 30000}");
        verify(callableStatement).execute();
    }

    @Test
    void testHeartbeatTask() throws Exception {
        // Setup mocks
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.prepareCall(anyString())).thenReturn(callableStatement);

        DbClient dbClient = new DbClient(dataSource);
        dbClient.heartbeatTask("test-queue", "run123");

        verify(callableStatement).setString(1, "test-queue");
        verify(callableStatement).setString(2, "run123");
        verify(callableStatement).execute();
    }

    @Test
    void testHandleSQLErrorCancelledTask() {
        SQLException sqlException = new SQLException("Task cancelled", "AB001");
        
        DbClient dbClient = new DbClient(dataSource);
        AbsurdException exception = assertThrows(AbsurdException.class, () -> {
            dbClient.handleSQLError(sqlException);
        });

        assertTrue(exception.getMessage().contains("Task was cancelled"));
    }

    @Test
    void testHandleSQLErrorFailedTask() {
        SQLException sqlException = new SQLException("Task failed", "AB002");
        
        DbClient dbClient = new DbClient(dataSource);
        AbsurdException exception = assertThrows(AbsurdException.class, () -> {
            dbClient.handleSQLError(sqlException);
        });

        assertTrue(exception.getMessage().contains("Task failed"));
    }
}