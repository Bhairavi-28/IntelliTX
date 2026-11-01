-- Illustrative SEED rows (is_seed = 1). They are labelled as seeds, are excluded from dashboard
-- charts by default and are NOT benchmark results. Real data comes from running experiments.
INSERT INTO experiments (name, category, source, algorithm, workload, workers, txn_count, parameters, status, invariant_ok, is_seed)
VALUES ('[seed] example engine run', 'transaction', 'measured_engine', 'wait_die', 'ycsb', 4, 1000,
        JSON_OBJECT('note', 'illustrative row for SQL demos'), 'COMPLETED', 1, 1),
       ('[seed] example scheduling run', 'scheduling', 'simulator', 'FCFS', NULL, NULL, 3,
        JSON_OBJECT('note', 'textbook jobs P1=24 P2=3 P3=3'), 'COMPLETED', NULL, 1);

INSERT INTO experiment_metrics (experiment_id, metric_name, metric_value, unit)
SELECT experiment_id, 'throughput_tps', 1000, 'txn/s' FROM experiments WHERE name = '[seed] example engine run';
INSERT INTO experiment_metrics (experiment_id, metric_name, metric_value, unit)
SELECT experiment_id, 'committed', 1000, 'txn' FROM experiments WHERE name = '[seed] example engine run';
INSERT INTO experiment_metrics (experiment_id, metric_name, metric_value, unit)
SELECT experiment_id, 'avg_waiting', 17, 'time units' FROM experiments WHERE name = '[seed] example scheduling run';

INSERT INTO job_results (experiment_id, job_id, arrival, burst, priority, completion, turnaround, waiting, response)
SELECT experiment_id, 1, 0, 24, 1, 24, 24, 0, 0 FROM experiments WHERE name = '[seed] example scheduling run';
INSERT INTO job_results (experiment_id, job_id, arrival, burst, priority, completion, turnaround, waiting, response)
SELECT experiment_id, 2, 0, 3, 2, 27, 27, 24, 24 FROM experiments WHERE name = '[seed] example scheduling run';
INSERT INTO job_results (experiment_id, job_id, arrival, burst, priority, completion, turnaround, waiting, response)
SELECT experiment_id, 3, 0, 3, 3, 30, 30, 27, 27 FROM experiments WHERE name = '[seed] example scheduling run';

INSERT INTO transaction_logs (experiment_id, seq, txn_ref, event_type, detail)
SELECT experiment_id, 1, 1, 'COMMIT', 'seed example' FROM experiments WHERE name = '[seed] example engine run';
