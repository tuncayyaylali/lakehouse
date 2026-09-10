-- 1. Initial State (10 Orders, $5,405.50 Total Amount)
SELECT '1. INITIAL STATE BEFORE INCIDENT' AS step, count(*) AS order_count, round(sum(amount), 2) AS total_amount 
FROM iceberg.bronze.orders;

-- 2. Accidental Delete Simulation (Deleting 4 orders in 'Electronics' category)
DELETE FROM iceberg.bronze.orders WHERE category = 'Electronics';

-- 3. Damaged State Post-Incident (Only 6 orders and $825.50 remaining!)
SELECT '2. DAMAGED STATE AFTER ACCIDENTAL DELETE' AS step, count(*) AS order_count, round(sum(amount), 2) AS total_amount 
FROM iceberg.bronze.orders;

-- 4. Iceberg Time Travel (Querying historical Snapshot 3972403013475769839 before incident)
SELECT '3. HISTORICAL SNAPSHOT VIA TIME TRAVEL' AS step, count(*) AS order_count, round(sum(amount), 2) AS total_amount 
FROM iceberg.bronze.orders FOR VERSION AS OF 3972403013475769839;

-- 5. Zero-Data-Loss Restoration from Snapshot (Disaster Recovery)
INSERT INTO iceberg.bronze.orders
SELECT * FROM iceberg.bronze.orders FOR VERSION AS OF 3972403013475769839
WHERE category = 'Electronics';

-- 6. Final Fully Restored State (Table is restored back to 10 orders and $5,405.50!)
SELECT '4. FINAL RESTORED STATE (ZERO DATA LOSS)' AS step, count(*) AS order_count, round(sum(amount), 2) AS total_amount 
FROM iceberg.bronze.orders;
