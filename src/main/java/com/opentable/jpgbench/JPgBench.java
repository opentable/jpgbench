package com.opentable.jpgbench;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.Callable;

import javax.sql.DataSource;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opentable.function.ThrowingConsumer;

public class JPgBench {
    private static final Logger LOG = LoggerFactory.getLogger(JPgBench.class);

    final double scale = 10;
    final Random r = new Random();
    final MetricRegistry registry = new MetricRegistry();
    final BenchMetrics metrics = new BenchMetrics(registry);
    long maxAid = 10_000, maxBid = 100, maxTid = 1_000;

    public static void main(String... args) throws Exception {
        new JPgBench().run(args);
    }

    void run(String... args) throws Exception {
        run(Jdbi.create("XXX"));
    }

    void run(DataSource ds) throws Exception {
        run(Jdbi.create(ds));
    }

    void run(Jdbi db) throws Exception {
        LOG.info("Initialized with maxAid={} maxBid={} maxTid={}.  Generating data, please stand by.", maxAid, maxBid, maxTid);

        generateData(db);

        final Duration testDuration = Duration.ofSeconds(20);
        LOG.info("Data created.  Running test of duration {}", testDuration);

        final long start = System.nanoTime();
        final BenchOp bench = new BenchOp(this);
        while (System.nanoTime() - start < testDuration.toNanos()) {
            try (final Handle h = metrics.cxn.time(db::open)) {
                metrics.txn.time(run(x -> bench.accept(h)));
            }
        }

        ConsoleReporter.forRegistry(registry).build().report();
    }

    private void generateData(Jdbi db) {
        db.useHandle(h -> {
            for (String table : new String[] { "pgbench_accounts", "pgbench_branches", "pgbench_history", "pgbench_tellers"}) {
                h.createUpdate("TRUNCATE TABLE " + table).execute();
            }
            for (int bid = 0; bid < maxBid * scale; bid++) {
                h.createUpdate("INSERT INTO pgbench_branches (bid, bbalance) VALUES (:bid, 0)")
                    .bind("bid", bid)
                    .execute();
            }
            for (int tid = 0; tid < maxTid * scale; tid++) {
                h.createUpdate("INSERT INTO pgbench_tellers (tid, bid, tbalance) VALUES (:tid, :bid, 0)")
                    .bind("tid", tid)
                    .bind("bid", tid % (maxBid + 1))
                    .execute();
            }
            for (int aid = 0; aid < maxAid * scale; aid++) {
                h.createUpdate("INSERT INTO pgbench_accounts (aid, bid, abalance, filler) VALUES (:aid, :bid, 0, 'xxxxxxxxxxx')")
                    .bind("aid", aid)
                    .bind("bid", aid % (maxBid + 1))
                    .execute();
            }
        });
    }

    private static Callable<Void> run(ThrowingConsumer<Void> r) {
        return () -> {
            r.accept(null);
            return null;
        };
    }
}

class BenchMetrics {
    final Timer cxn, txn, begin, stmt, commit;
    BenchMetrics(MetricRegistry registry) {
        cxn = registry.timer("cxn");
        txn = registry.timer("txn");
        begin = registry.timer("begin");
        stmt = registry.timer("stmt");
        commit = registry.timer("commit");
    }
}

//"tpcb-like",
//"<builtin: TPC-B (sort of)>",
//"\\set aid random(1, " CppAsString2(naccounts) " * :scale)\n"
//"\\set bid random(1, " CppAsString2(nbranches) " * :scale)\n"
//"\\set tid random(1, " CppAsString2(ntellers) " * :scale)\n"
//"\\set delta random(-5000, 5000)\n"
//"BEGIN;\n"
//"UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;\n"
//"SELECT abalance FROM pgbench_accounts WHERE aid = :aid;\n"
//"UPDATE pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;\n"
//"UPDATE pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid;\n"
//"INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);\n"
//"END;\n"

class BenchOp implements ThrowingConsumer<Handle> {
    private final JPgBench bench;

    BenchOp(JPgBench bench) {
        this.bench = bench;
    }

    @Override
    public void accept(Handle h) throws Exception {
        final int delta = bench.r.nextInt(10000) - 5000;
        final long aid = bench.r.longs(0, bench.maxAid + 1).findAny().getAsLong();
        final long bid = bench.r.longs(0, bench.maxBid + 1).findAny().getAsLong();
        final long tid = bench.r.longs(0, bench.maxTid + 1).findAny().getAsLong();
        try {
            bench.metrics.begin.time(h::begin);
            try (Timer.Context stmtTime = bench.metrics.stmt.time()) {
                h.createUpdate("UPDATE pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid")
                    .bind("aid", aid)
                    .bind("delta", delta)
                    .execute();
                h.createQuery("SELECT abalance FROM pgbench_accounts WHERE aid = :aid")
                    .bind("aid", aid)
                    .mapTo(long.class)
                    .findOnly();
                h.createUpdate("UPDATE pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid")
                    .bind("tid", tid)
                    .bind("delta", delta)
                    .execute();
                h.createUpdate("UPDATE pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid")
                    .bind("bid", bid)
                    .bind("delta", delta)
                    .execute();
                h.createUpdate("INSERT INTO pgbench_history (tid, bid, aid, delta, mtime) VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP)")
                    .bind("tid", tid)
                    .bind("bid", bid)
                    .bind("aid", aid)
                    .bind("delta", delta)
                    .execute();
            }
            bench.metrics.commit.time(h::commit);
        } catch (Exception e) {
            h.rollback();
            throw e;
        }
    }
}
