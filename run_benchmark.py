"""CLI: run the worker-count x policy benchmark matrix and store every run in MySQL.

    python -m backend.run_benchmark --workers 1,2,4,8 --policies wait_die,no_wait,detect --txns 20000
"""
from __future__ import annotations

import argparse

from backend.services.experiments import run_transaction_experiment


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--workers", default="1,2,4,8")
    ap.add_argument("--policies", default="wait_die,no_wait,detect")
    ap.add_argument("--workload", default="ycsb", choices=["ycsb", "transfer"])
    ap.add_argument("--txns", type=int, default=20000)
    ap.add_argument("--records", type=int, default=1000)
    ap.add_argument("--theta", type=float, default=0.6)
    ap.add_argument("--write-ratio", type=float, default=0.5)
    ap.add_argument("--repeat", type=int, default=1)
    a = ap.parse_args()
    bad = 0
    print(f"{'policy':<10}{'workers':>8}{'tps':>12}{'committed':>11}{'aborts':>9}  invariant")
    for policy in a.policies.split(","):
        for w in (int(x) for x in a.workers.split(",")):
            for _ in range(a.repeat):
                r = run_transaction_experiment(policy=policy, workload=a.workload, workers=w, txns=a.txns,
                                               records=a.records, theta=a.theta, write_ratio=a.write_ratio)
                bad += 0 if r["invariant_ok"] else 1
                print(f"{policy:<10}{w:>8}{r['throughput_tps']:>12.0f}{r['committed']:>11}{r['aborted_attempts']:>9}  "
                      f"{'OK' if r['invariant_ok'] else 'VIOLATED'} (exp #{r['experiment_id']})")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
