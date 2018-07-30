package com.opentable.jpgbench;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;

import org.junit.Rule;
import org.junit.Test;

import com.opentable.db.postgres.embedded.FlywayPreparer;
import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;

public class TestJPgBench {
    @Rule
    public PreparedDbRule pg = EmbeddedPostgresRules.preparedDatabase(FlywayPreparer.forClasspathLocation("/example-pgbench"));

    @Test
    public void test() throws Exception {
        final JPgBench bench = new JPgBench();
        bench.testDuration = Duration.ofSeconds(5);
        bench.scale = 1;
        assertThat(bench.run(pg.getTestDatabase())).isGreaterThan(10);
    }
}
