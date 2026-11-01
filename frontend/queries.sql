-- Representative queries. Run with:  mysql -u$MYSQL_USER -p $MYSQL_DATABASE < storage/queries.sql
-- (all statements are read-only except the clearly marked DML demo at the end, which rolls back)

-- Q1 SELECT + WHERE + ORDER BY: latest completed measured runs
SELECT experiment_id, name, algorithm, workers, created_at FROM experiments
WHERE source = 'measured_engine' AND status = 'COMPLETED' ORDER BY created_at DESC LIMIT 10;

-- Q2 JOIN: throughput of every transaction experiment
SELECT e.experiment_id, e.algorithm, e.workers, m.metric_value AS throughput_tps
FROM experiments e JOIN experiment_metrics m ON m.experiment_id = e.experiment_id
WHERE e.category = 'transaction' AND m.metric_name = 'throughput_tps' AND e.is_seed = 0
ORDER BY m.metric_value DESC;

-- Q3 AGGREGATE + GROUP BY + HAVING: mean throughput per lock policy
SELECT e.algorithm, COUNT(*) AS runs, ROUND(AVG(m.metric_value), 1) AS mean_tps, MAX(m.metric_value) AS best_tps
FROM experiments e JOIN experiment_metrics m ON m.experiment_id = e.experiment_id
WHERE m.metric_name = 'throughput_tps' AND e.is_seed = 0
GROUP BY e.algorithm HAVING COUNT(*) >= 1 ORDER BY mean_tps DESC;

-- Q4 SUBQUERY (scalar): runs faster than the overall average
SELECT e.experiment_id, e.algorithm, m.metric_value
FROM experiments e JOIN experiment_metrics m ON m.experiment_id = e.experiment_id
WHERE m.metric_name = 'throughput_tps'
  AND m.metric_value > (SELECT AVG(metric_value) FROM experiment_metrics WHERE metric_name = 'throughput_tps');

-- Q5 SUBQUERY (correlated, EXISTS): experiments that logged at least one deadlock event
SELECT e.experiment_id, e.name FROM experiments e
WHERE EXISTS (SELECT 1 FROM transaction_logs l WHERE l.experiment_id = e.experiment_id AND l.event_type = 'DEADLOCK_DETECTED');

-- Q6 LEFT JOIN: experiments without any metric (data-quality check)
SELECT e.experiment_id, e.name FROM experiments e
LEFT JOIN experiment_metrics m ON m.experiment_id = e.experiment_id WHERE m.metric_id IS NULL;

-- Q7 Window-style ranking without window functions (portable): best run per category
SELECT e.category, e.experiment_id, m.metric_value
FROM experiments e JOIN experiment_metrics m ON m.experiment_id = e.experiment_id
WHERE m.metric_name = 'throughput_tps'
  AND m.metric_value = (SELECT MAX(m2.metric_value) FROM experiment_metrics m2
                        JOIN experiments e2 ON e2.experiment_id = m2.experiment_id
                        WHERE e2.category = e.category AND m2.metric_name = 'throughput_tps');

-- Q8 VIEWS
SELECT * FROM v_experiment_summary ORDER BY created_at DESC LIMIT 10;
SELECT * FROM v_policy_comparison ORDER BY algorithm, workers;

-- Q9 STORED PROCEDURES
CALL sp_experiment_report(1);

-- Q10 DML demo (INSERT / UPDATE / DELETE, triggers, constraints) inside a rolled-back transaction
START TRANSACTION;
INSERT INTO experiments (name, category, source, algorithm, status) VALUES ('demo', 'transaction', 'measured_engine', 'wait_die', 'RUNNING');
SET @demo_id = LAST_INSERT_ID();
CALL sp_record_metric(@demo_id, 'throughput_tps', 123.4, 'txn/s');
CALL sp_record_metric(@demo_id, 'throughput_tps', 456.7, 'txn/s');   -- upsert: still ONE row
UPDATE experiments SET status = 'COMPLETED' WHERE experiment_id = @demo_id;
SELECT metric_name, metric_value FROM experiment_metrics WHERE experiment_id = @demo_id;
DELETE FROM experiments WHERE experiment_id = @demo_id;               -- cascades + audit trigger
SELECT COUNT(*) AS audit_rows FROM experiment_audit WHERE experiment_id = @demo_id;
ROLLBACK;
