CREATE TABLE pgbench_history (
  tid int,
  bid int,
  aid int,
  delta int,
  mtime timestamp,
  filler char(22)
);

CREATE TABLE pgbench_tellers (
  tid int not null,
  bid int,
  tbalance int,
  filler char(84)
) WITH (fillfactor=100);

CREATE TABLE pgbench_accounts (
  aid int not null,
  bid int,
  abalance int,
  filler char(84)
) WITH (fillfactor=100);

CREATE TABLE pgbench_branches (
  bid int not null,
  bbalance int,
  filler char(88)
) WITH (fillfactor=100);
