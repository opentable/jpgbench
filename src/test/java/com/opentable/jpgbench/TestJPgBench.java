package com.opentable.jpgbench;

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
        new JPgBench().run(pg.getTestDatabase());
    }
}
