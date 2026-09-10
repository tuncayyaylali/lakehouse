-- Apache Iceberg Metadata Snapshots Inspection
-- Query historical commits and snapshot IDs across Bronze tables

-- 1. Snapshot history for Excel Orders (Medallion Pipeline)
SELECT snapshot_id, committed_at, operation 
FROM iceberg.bronze."excel_orders$snapshots" 
ORDER BY committed_at DESC;

-- 2. Snapshot history for E-Commerce Orders
SELECT snapshot_id, committed_at, operation 
FROM iceberg.bronze."orders$snapshots" 
ORDER BY committed_at DESC;
