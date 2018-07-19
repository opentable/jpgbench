package com.opentable.bench;

import org.jdbi.v3.core.Jdbi;

public class JPgBench {
    public static void main(String... args) {
        final Jdbi db = Jdbi.create();
        db.useTransaction(h -> {
            h.createQuery("SELECT 1")
                .mapToMap().forEach(System.out::println);
        });
    }
}
