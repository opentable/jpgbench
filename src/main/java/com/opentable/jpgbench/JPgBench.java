package com.opentable.jpgbench;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.PreparedBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opentable.function.ThrowingConsumer;

public class JPgBench {
    private static final Logger LOG = LoggerFactory.getLogger(JPgBench.class);

    Duration testDuration = Duration.ofSeconds(20);
    final double scale = 10;
    final Random r = new Random();
    final MetricRegistry registry = new MetricRegistry();
    final BenchMetrics metrics = new BenchMetrics(registry);
    long maxAid = 10_000, maxBid = 100, maxTid = 1_000;

    public static void main(String... args) throws Exception {
        new JPgBench().run(args);
    }

    long run(String... args) throws Exception {
        return run(Jdbi.create("XXX"));
    }

    long run(DataSource ds) throws Exception {
        return run(Jdbi.create(ds));
    }

    long run(Jdbi db) throws Exception {
        LOG.info("Initialized with maxAid={} maxBid={} maxTid={}.  Generating data, please stand by.", maxAid, maxBid, maxTid);

        generateData(db);

        LOG.info("Data created.  Running test of duration {}", testDuration);

        final long start = System.nanoTime();
        final BenchOp bench = new BenchOp(this);
        while (System.nanoTime() - start < testDuration.toNanos()) {
            try (final Handle h = metrics.cxn.time(db::open)) {
                metrics.txn.time(run(x -> bench.accept(h)));
            }
        }
        final long end = System.nanoTime();

        final StringBuilder report = new StringBuilder();
        report.append("==== Run Complete!\nn = " + metrics.txn.getCount() + " [50/95/99us]\n");
        for (String t : new String[] { "cxn", "txn", "begin", "stmt", "commit" }) {
            final Snapshot s = registry.getTimers().get(t).getSnapshot();
            report.append(String.format("%6s = [%10.2f/%10.2f/%10.2f]\n", t,
                    s.getMedian() / 1000.0, s.get95thPercentile() / 1000.0, s.get99thPercentile() / 1000.0));
        }
        final double tps = 1.0 * metrics.txn.getCount() / ((end - start) / TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS));
        report.append(String.format("tps=%.2f tpm=%.2f\n", tps, tps * 60));
        LOG.info("{}", report);
        return metrics.txn.getCount();
    }

    private void generateData(Jdbi db) {
        db.useHandle(h -> {
            for (String table : new String[] { "pgbench_accounts", "pgbench_branches", "pgbench_history", "pgbench_tellers"}) {
                h.createUpdate("TRUNCATE TABLE " + table).execute();
            }
            final PreparedBatch bs = h.prepareBatch("INSERT INTO pgbench_branches (bid, bbalance) VALUES (:bid, 0)");
            for (int bid = 0; bid < maxBid * scale; bid++) {
                    bs.bind("bid", bid).add();
            }
            bs.execute();
            final PreparedBatch ts = h.prepareBatch("INSERT INTO pgbench_tellers (tid, bid, tbalance) VALUES (:tid, :bid, 0)");
            for (int tid = 0; tid < maxTid * scale; tid++) {
                ts  .bind("tid", tid)
                    .bind("bid", tid % (maxBid + 1))
                    .add();
            }
            ts.execute();
            PreparedBatch batch = null;
            for (int aid = 0; aid < maxAid * scale; aid++) {
                if (batch == null) {
                    batch = h.prepareBatch("INSERT INTO pgbench_accounts (aid, bid, abalance, filler) VALUES (:aid, :bid, 0, 'xxxxxxxxxxx')");
                }
                batch.bind("aid", aid)
                    .bind("bid", aid % (maxBid + 1))
                    .add();
                if (batch.size() >= 10_000) {
                    LOG.info("Wrote {} of {} rows", aid, maxAid * scale);
                    batch.execute();
                    batch = null;
                }
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
