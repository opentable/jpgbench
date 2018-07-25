package com.opentable.jpgbench;

import java.time.Duration;
import java.util.function.Consumer;

import javax.sql.DataSource;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JPgBench {
    private static final Logger LOG = LoggerFactory.getLogger(JPgBench.class);

    private final MetricRegistry registry = new MetricRegistry();
    private final BenchMetrics metrics = new BenchMetrics(registry);
    long maxAid, maxBid, maxTid;

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
        db.useTransaction(h -> {
            maxAid = h.createQuery("SELECT max(aid) FROM pgbench_accounts")
                .mapTo(int.class).findOnly();
            maxBid = h.createQuery("SELECT max(bid) FROM pgbench_branches")
                .mapTo(int.class).findOnly();
            maxTid = h.createQuery("SELECT max(tid) FROM pgbench_tellers")
                .mapTo(int.class).findOnly();
        });
        LOG.info("Initialized with maxAid={} maxBid={} maxTid={}", maxAid, maxBid, maxTid);

        final Duration testDuration = Duration.ofSeconds(20);
        final long start = System.nanoTime();
        while (System.nanoTime() - start < testDuration.toNanos()) {
            try (final Handle h = metrics.cxn.time(db::open)) {

            }
        }

        ConsoleReporter.forRegistry(registry).build().report();
    }
}

class BenchMetrics {
    final Timer cxn, txn;
    BenchMetrics(MetricRegistry registry) {
        cxn = registry.timer("cxn");
        txn = registry.timer("txn");
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

class BenchOp implements Consumer<Handle> {
    private final JPgBench bench;

    BenchOp(JPgBench bench) {
        this.bench = bench;
    }

    @Override
    public void accept(Handle h) {
        h.begin();
        try {

        } finally {

        }
    }
}
