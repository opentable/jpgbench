package com.opentable.jpgbench;

import org.postgresql.ds.PGSimpleDataSource;

public class MultiRunner {
    public static void main(String... args) throws Exception {
        final StringBuilder results = new StringBuilder(128);
        results.append("#cxn   #txn\n");
        for (int c = 1; c <= 32768; c *= 2) {
            System.gc();
            final JPgBench b = new JPgBench();
            b.concurrency = c;
            b.connectStrategy = ConnectStrategy.HIKARI;
            b.rerun = true;
            PGSimpleDataSource ds = new PGSimpleDataSource();
            ds.setUrl(args[0]);
            JPgBench.findPgpass(ds);
            final long txns;
            try {
                txns = b.run(ds);
            } catch (Throwable e) {
                e.printStackTrace();
                System.exit(1);
                return;
            }
            results.append(String.format("%5d", c));
            results.append(' ');
            results.append(String.format("%7d", txns));
            results.append('\n');
            if (txns <= 0) {
                break;
            }
        }
        System.err.println(results.toString());
    }
}
