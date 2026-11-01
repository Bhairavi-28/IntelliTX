-- IntelliTX relational schema (MySQL 8.0.16+; CHECK constraints are enforced from 8.0.16).
-- The script is re-runnable: it drops and recreates objects. Statements containing ';' inside a
-- body use DELIMITER $$ (handled by backend/database.py as well as the mysql client).

DROP VIEW IF EXISTS v_experiment_summary;
DROP VIEW IF EXISTS v_policy_comparison;
DROP PROCEDURE IF EXISTS sp_record_metric;
DROP PROCEDURE IF EXISTS sp_experiment_report;
DROP PROCEDURE IF EXISTS sp_purge_experiments;
DROP TABLE IF EXISTS experiment_audit;
DROP TABLE IF EXISTS job_results;
DROP TABLE IF EXISTS transaction_logs;
DROP TABLE IF EXISTS experiment_metrics;
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS experiments;

-- 1) experiments: one row per run (real-engine run, simulator run, SQL index experiment).
CREATE TABLE experiments (
  experiment_id INT UNSIGNED NOT NULL AUTO_INCREMENT,
  name          VARCHAR(120) NOT NULL,
  category      ENUM('transaction','deadlock','recovery','scheduling','disk','allocation',
                     'os_deadlock','sql_index') NOT NULL,
  -- measured_engine: real C++ engine / real MySQL timing; simulator: deterministic model.
  source        ENUM('measured_engine','simulator','measured_mysql') NOT NULL,
  algorithm     VARCHAR(40)  NOT NULL,            -- lock policy, scheduling algorithm, ...
  workload      VARCHAR(40)  NULL,
  workers       SMALLINT UNSIGNED NULL,
  txn_count     INT UNSIGNED NULL,
  parameters    JSON NULL,                        -- full input configuration (reproducibility)
  status        ENUM('RUNNING','COMPLETED','FAILED') NOT NULL DEFAULT 'RUNNING',
  invariant_ok  TINYINT(1) NULL,                  -- correctness check outcome, if applicable
  is_seed       TINYINT(1) NOT NULL DEFAULT 0,    -- 1 = illustrative seed row, hidden in charts
  error_message VARCHAR(500) NULL,
  created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (experiment_id),
  CONSTRAINT chk_exp_workers CHECK (workers IS NULL OR workers BETWEEN 1 AND 256),
  CONSTRAINT chk_exp_txn     CHECK (txn_count IS NULL OR txn_count > 0),
  CONSTRAINT chk_exp_name    CHECK (CHAR_LENGTH(TRIM(name)) > 0),
  INDEX idx_exp_cat_algo (category, algorithm),
  INDEX idx_exp_created (created_at)
) ENGINE=InnoDB;

-- 2) transactions: per-transaction samples of the REAL engine (latency measured start->commit).
CREATE TABLE transactions (
  txn_id        BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  experiment_id INT UNSIGNED NOT NULL,
  txn_ref       BIGINT UNSIGNED NOT NULL,          -- engine transaction id of the committing attempt
  attempts      SMALLINT UNSIGNED NOT NULL DEFAULT 1,  -- 1 = no abort/restart
  latency_us    DOUBLE NOT NULL,
  reads_done    SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  writes_done   SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  PRIMARY KEY (txn_id),
  UNIQUE KEY uq_txn_ref (experiment_id, txn_ref),
  CONSTRAINT fk_txn_exp FOREIGN KEY (experiment_id) REFERENCES experiments(experiment_id)
    ON DELETE CASCADE,
  CONSTRAINT chk_txn_attempts CHECK (attempts >= 1)
) ENGINE=InnoDB;

-- 3) job_results: per-job output of the CPU-scheduling simulator (SIMULATED values).
CREATE TABLE job_results (
  job_result_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  experiment_id INT UNSIGNED NOT NULL,
  job_id        INT NOT NULL,
  arrival       DOUBLE NOT NULL,
  burst         DOUBLE NOT NULL,
  priority      INT NOT NULL,
  completion    DOUBLE NOT NULL,
  turnaround    DOUBLE NOT NULL,
  waiting       DOUBLE NOT NULL,
  response      DOUBLE NOT NULL,
  PRIMARY KEY (job_result_id),
  UNIQUE KEY uq_job (experiment_id, job_id),
  CONSTRAINT fk_job_exp FOREIGN KEY (experiment_id) REFERENCES experiments(experiment_id)
    ON DELETE CASCADE,
  CONSTRAINT chk_job_burst CHECK (burst > 0),
  CONSTRAINT chk_job_times CHECK (completion >= arrival AND waiting >= -0.000001)
) ENGINE=InnoDB;

-- 4) transaction_logs: ordered event log (deadlock events, recovery checks, ...).
CREATE TABLE transaction_logs (
  log_id        BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  experiment_id INT UNSIGNED NOT NULL,
  seq           INT UNSIGNED NOT NULL,
  txn_ref       BIGINT UNSIGNED NULL,
  event_type    VARCHAR(30) NOT NULL,
  detail        VARCHAR(1000) NOT NULL,
  passed        TINYINT(1) NULL,                   -- for check-style events
  PRIMARY KEY (log_id),
  UNIQUE KEY uq_log_seq (experiment_id, seq),
  INDEX idx_log_event (event_type),
  CONSTRAINT fk_log_exp FOREIGN KEY (experiment_id) REFERENCES experiments(experiment_id)
    ON DELETE CASCADE
) ENGINE=InnoDB;

-- 5) experiment_metrics: name/value facts about a run (3NF: value depends on (experiment, name)).
CREATE TABLE experiment_metrics (
  metric_id     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  experiment_id INT UNSIGNED NOT NULL,
  metric_name   VARCHAR(60) NOT NULL,
  metric_value  DOUBLE NOT NULL,
  unit          VARCHAR(20) NOT NULL DEFAULT '',
  PRIMARY KEY (metric_id),
  UNIQUE KEY uq_metric (experiment_id, metric_name),
  INDEX idx_metric_name (metric_name),
  CONSTRAINT fk_metric_exp FOREIGN KEY (experiment_id) REFERENCES experiments(experiment_id)
    ON DELETE CASCADE
) ENGINE=InnoDB;

-- 6) experiment_audit: filled by a trigger when an experiment is deleted.
CREATE TABLE experiment_audit (
  audit_id      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  experiment_id INT UNSIGNED NOT NULL,          -- no FK on purpose: the parent row is gone
  name          VARCHAR(120) NOT NULL,
  action        VARCHAR(20) NOT NULL,
  logged_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (audit_id)
) ENGINE=InnoDB;

-- ---------------------------------------------------------------- views
CREATE VIEW v_experiment_summary AS
SELECT e.experiment_id, e.name, e.category, e.source, e.algorithm, e.workload, e.workers,
       e.txn_count, e.status, e.invariant_ok, e.is_seed, e.created_at,
       MAX(CASE WHEN m.metric_name = 'throughput_tps'   THEN m.metric_value END) AS throughput_tps,
       MAX(CASE WHEN m.metric_name = 'elapsed_ms'       THEN m.metric_value END) AS elapsed_ms,
       MAX(CASE WHEN m.metric_name = 'committed'        THEN m.metric_value END) AS committed,
       MAX(CASE WHEN m.metric_name = 'aborted_attempts' THEN m.metric_value END) AS aborted_attempts,
       MAX(CASE WHEN m.metric_name = 'avg_waiting'      THEN m.metric_value END) AS avg_waiting,
       MAX(CASE WHEN m.metric_name = 'avg_turnaround'   THEN m.metric_value END) AS avg_turnaround
FROM experiments e
LEFT JOIN experiment_metrics m ON m.experiment_id = e.experiment_id
GROUP BY e.experiment_id, e.name, e.category, e.source, e.algorithm, e.workload, e.workers,
         e.txn_count, e.status, e.invariant_ok, e.is_seed, e.created_at;

CREATE VIEW v_policy_comparison AS
SELECT algorithm, workers, COUNT(*) AS runs,
       AVG(throughput_tps) AS avg_throughput_tps,
       AVG(aborted_attempts / NULLIF(committed + aborted_attempts, 0)) AS avg_abort_rate
FROM v_experiment_summary
WHERE category = 'transaction' AND status = 'COMPLETED' AND is_seed = 0
GROUP BY algorithm, workers;

-- ---------------------------------------------------------------- triggers
DELIMITER $$
CREATE TRIGGER trg_exp_status_guard BEFORE UPDATE ON experiments
FOR EACH ROW
BEGIN
  IF OLD.status IN ('COMPLETED','FAILED') AND NEW.status = 'RUNNING' THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'a finished experiment cannot be reopened';
  END IF;
END$$

CREATE TRIGGER trg_exp_audit_delete AFTER DELETE ON experiments
FOR EACH ROW
BEGIN
  INSERT INTO experiment_audit (experiment_id, name, action) VALUES (OLD.experiment_id, OLD.name, 'DELETE');
END$$

CREATE TRIGGER trg_txn_latency_guard BEFORE INSERT ON transactions
FOR EACH ROW
BEGIN
  IF NEW.latency_us < 0 THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'latency cannot be negative';
  END IF;
END$$

-- ---------------------------------------------------------------- stored procedures
-- Upsert one metric (INSERT ... ON DUPLICATE KEY UPDATE).
CREATE PROCEDURE sp_record_metric(IN p_exp INT UNSIGNED, IN p_name VARCHAR(60),
                                  IN p_value DOUBLE, IN p_unit VARCHAR(20))
BEGIN
  INSERT INTO experiment_metrics (experiment_id, metric_name, metric_value, unit)
  VALUES (p_exp, p_name, p_value, p_unit)
  ON DUPLICATE KEY UPDATE metric_value = VALUES(metric_value), unit = VALUES(unit);
END$$

-- Two result sets: the experiment row, then its metrics.
CREATE PROCEDURE sp_experiment_report(IN p_exp INT UNSIGNED)
BEGIN
  SELECT experiment_id, name, category, source, algorithm, status, invariant_ok, created_at
  FROM experiments WHERE experiment_id = p_exp;
  SELECT metric_name, metric_value, unit FROM experiment_metrics
  WHERE experiment_id = p_exp ORDER BY metric_name;
END$$

-- Deletes non-seed experiments older than p_days days (children go via ON DELETE CASCADE).
CREATE PROCEDURE sp_purge_experiments(IN p_days INT, OUT p_deleted INT)
BEGIN
  IF p_days < 0 THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'p_days must be >= 0';
  END IF;
  DELETE FROM experiments
  WHERE is_seed = 0 AND created_at < (NOW() - INTERVAL p_days DAY);
  SET p_deleted = ROW_COUNT();
END$$
DELIMITER ;
