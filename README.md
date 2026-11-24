# IntelliTX

# IntelliTX — Multithreaded Transaction Management and OS Resource Scheduling Analysis System

A small, honest, fully runnable platform that combines

* a **C++17 transaction engine** (Strict 2PL, Wait-Die / NO-WAIT prevention, wait-for-graph deadlock *detection*, undo log, write-ahead log, checkpoints, crash recovery, worker pool, **four SQL isolation levels**),
* an **Isolation & Serializability Lab**: dirty read / non-repeatable read / lost update / phantom reproduced at READ UNCOMMITTED → SERIALIZABLE on the engine *and* on MySQL InnoDB, plus a **precedence-graph** checker (conflict + view serializability, recoverable / cascadeless / strict) for typed schedules and for real concurrent runs,
* **OS algorithm simulators** (CPU scheduling, disk scheduling, file allocation, Banker's algorithm and OS deadlock detection, semaphores / producer–consumer),
* **MySQL 8** for experiment storage and SQL/indexing experiments (7 related tables, 3 views, triggers, stored procedures),
* a **Python** integration layer and a **Streamlit** dashboard.

```
Streamlit UI (frontend/dashboard.py)
   ↓
Python backend (backend/)  ──────────────►  MySQL (storage/*.sql): experiments, results, history
   ↓ subprocess + JSON
C++ engine + simulators (engine/, binary build/itx_cli)
```

Inspired by the VLDB 2014 paper *“Staring into the Abyss”* (concurrency control on many cores). It does **not** reproduce that paper — see `docs/research_alignment.md`. Memory management / paging is intentionally excluded.

**What is measured vs simulated.** Transaction, deadlock and recovery experiments run the *real* engine and MySQL (measured). CPU scheduling, disk scheduling, file allocation and Banker's algorithm are *simulators* (deterministic models). The dashboard labels every page.

## 1. Install dependencies (Ubuntu / WSL)

```bash
sudo apt update
sudo apt install -y build-essential cmake python3 python3-pip python3-venv mysql-server
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## 2. Configure MySQL

```bash
sudo service mysql start            # WSL; on systemd Ubuntu: sudo systemctl start mysql
# edit the password in scripts/setup_mysql.sql first, then:
sudo mysql < scripts/setup_mysql.sql
cp .env.example .env                # put the SAME password in .env
```
`SET GLOBAL log_bin_trust_function_creators = 1` (in the script) is required once per server start so the schema's triggers can be created while binary logging is on. Add `log_bin_trust_function_creators=1` to the `[mysqld]` section of `/etc/mysql/mysql.conf.d/mysqld.cnf` to make it permanent.

## 3. Create the schema

```bash
python -m scripts.init_db           # drops and recreates the IntelliTX tables, loads 2 labelled seed rows
```

## 4. Build the C++ engine

```bash
./scripts/build.sh                  # = cmake -S . -B build && cmake --build build
```

## 5. Run the tests

```bash
./build/itx_tests                   # 90 C++ tests (locks, 2PL, Wait-Die, deadlocks, isolation levels, serializability, WAL/recovery, schedulers, sync)
./scripts/run_all_tests.sh          # C++ tests + pytest suite (uses database intellitx_test)
```
The Python tests recreate the schema, so they only run when `MYSQL_DATABASE` ends in `_test` (the script sets this). Details and recorded results: `docs/testing.md`.

## 6. Launch the dashboard

```bash
streamlit run frontend/dashboard.py     # http://localhost:8501
```

## 7. Run and compare experiments

* **Dashboard:** *Transaction Lab → Benchmark sweep* runs every policy at 1/2/4/8 workers; *Performance Analysis* plots the saved results; *Experiment History* filters runs and exports CSV.
* **Command line:** `python -m backend.run_benchmark --workers 1,2,4,8 --policies wait_die,no_wait,detect --txns 20000`
* **Engine directly:** `build/itx_cli bench --policy detect --workers 8 --txns 20000 --theta 0.9` (prints JSON). Other commands: `sched`, `disk`, `alloc`, `deadlock`, `recovery`, `osdeadlock`, `isolation`, `serial`, `serial-run`.
* **Everything together (recommended first run):** dashboard page **Integrated Pipeline**, or `build/itx_cli pipeline --txns 1000 --workers 8 --policy detect --admission SJF --checkpoint-ms 20 --dir /tmp/pl` (add `--crash-after 400` to kill the process mid-load and recover). 1000 transactions update the same accounts and one hot counter row at once through: scheduling policy → bounded queue → worker pool → Strict 2PL + deadlock handling → undo + write-ahead log + checkpoints → (crash) → recovery → correctness checks → MySQL.
* **Isolation levels and serializability:** dashboard page **Isolation & Serializability Lab**, or
  `build/itx_cli isolation --scenario all --level all` (the anomaly × isolation-level matrix),
  `build/itx_cli serial --schedule "R1(A) W2(A) R2(B) W1(B) C1 C2"` (precedence graph of a typed schedule),
  `build/itx_cli serial-run --level RC --txns 6` (graph of a real concurrent run).
* **SQL demos:** `mysql -u intellitx -p intellitx < storage/queries.sql`

## Repository map

| Path | Content |
|---|---|
| `engine/` | C++ engine, simulators, CLI; `engine/tests/` C++ tests |
| `backend/` | `database.py` (MySQL), `services/` (engine runner, experiments, SQL lab, queries), `run_benchmark.py` |
| `storage/` | `schema.sql`, `seed.sql`, `queries.sql` |
| `frontend/dashboard.py` | Streamlit app |
| `tests/` | pytest: CLI, MySQL constraints/triggers/procedures, integration, dashboard smoke tests |
| `docs/` | architecture, syllabus mapping, research alignment, testing, viva Q&A, presentation outline |
