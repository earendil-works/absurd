package absurd;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import org.junit.jupiter.api.Test;
import org.postgresql.Driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class BuildSetupSmokeTest {
    @Test
    void requiredDependenciesAreAvailable() {
        assertEquals(21, Runtime.version().feature());
        assertEquals("{}", new ObjectMapper().createObjectNode().toString());

        var hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl("jdbc:postgresql://localhost/absurd");
        assertEquals("jdbc:postgresql://localhost/absurd", hikariConfig.getJdbcUrl());

        assertNotNull(new Driver());
    }
}
