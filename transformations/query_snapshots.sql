SELECT snapshot_id, committed_at, operation FROM iceberg.bronze."orders$snapshots" ORDER BY committed_at ASC;

