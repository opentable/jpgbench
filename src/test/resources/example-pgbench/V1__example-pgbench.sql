CREATE TABLE pgbench_history (
  tid bigint,
  bid bigint,
  aid bigint,
  delta int,
  mtime timestamp,
  filler char(22)
);

CREATE TABLE pgbench_tellers (
  tid bigint not null,
  bid bigint,
  tbalance int,
  filler char(84)
) WITH (fillfactor=100);

CREATE TABLE pgbench_accounts (
  aid bigint not null,
  bid bigint,
  abalance int,
  filler char(84)
) WITH (fillfactor=100);

CREATE TABLE pgbench_branches (
  bid bigint not null,
  bbalance int,
  filler char(88)
) WITH (fillfactor=100);
