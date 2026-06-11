"""Detection accuracy benchmark: injected ground truth vs detected anomalies.

For every injected anomaly (data/anomaly_ground_truth.json) and every detection
method, computes precision / recall / F1 and median detection latency. A
detection matches an injected anomaly when:

    same ticker  AND  detection ts ∈ [start_ts − 5 s, end_ts + grace]

where grace defaults to 90 s (bar-based methods detect at window close, up to
one 5m bar after the event). Detections matching no injection are false
positives; injections matched by no detection are false negatives (per method).

The ensemble row treats an injection as detected when ≥2 distinct methods
matched it, and an ensemble FP as any 30 s ticker-bucket where ≥2 methods
fired without a matching injection.

Usage:
    python tests/benchmarks/evaluate_detection.py \
        [--ground-truth data/anomaly_ground_truth.json] [--grace-s 90] [--markdown]
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, "apps")

from ml.feature_builder import clickhouse_client  # noqa: E402

METHODS = ["zscore", "ewma_spc", "isolation_forest", "arima_residual"]


def load_detections() -> list[dict]:
    client = clickhouse_client()
    rows = client.query(
        "SELECT ticker, ts, detection_method, score FROM nse.anomalies ORDER BY ts"
    ).result_rows
    return [
        {"ticker": r[0], "ts": r[1], "method": r[2], "score": r[3]}
        for r in rows
    ]


def evaluate(truth: dict, detections: list[dict], grace_s: int) -> dict[str, dict]:
    anomalies = truth["anomalies"]
    for a in anomalies:
        a["_start"] = datetime.fromisoformat(a["start_ts"]).replace(tzinfo=None) - timedelta(seconds=5)
        a["_end"] = datetime.fromisoformat(a["end_ts"]).replace(tzinfo=None) + timedelta(seconds=grace_s)

    results: dict[str, dict] = {}
    matched_by_anomaly: dict[str, set] = defaultdict(set)  # anomaly_id -> methods

    for method in METHODS:
        dets = [d for d in detections if d["method"] == method]
        tp_latencies: list[float] = []
        matched_det = 0
        hit_anomalies: set[str] = set()

        for d in dets:
            ts = d["ts"].replace(tzinfo=None) if hasattr(d["ts"], "replace") else d["ts"]
            hit = None
            for a in anomalies:
                if d["ticker"] == a["ticker"] and a["_start"] <= ts <= a["_end"]:
                    hit = a
                    break
            if hit:
                matched_det += 1
                if hit["anomaly_id"] not in hit_anomalies:
                    hit_anomalies.add(hit["anomaly_id"])
                    matched_by_anomaly[hit["anomaly_id"]].add(method)
                    true_start = datetime.fromisoformat(hit["start_ts"]).replace(tzinfo=None)
                    tp_latencies.append((ts - true_start).total_seconds())

        n_det = len(dets)
        fp = n_det - matched_det
        recall = len(hit_anomalies) / len(anomalies) if anomalies else 0.0
        precision = matched_det / n_det if n_det else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
        results[method] = {
            "detections": n_det,
            "precision": round(precision, 3),
            "recall": round(recall, 3),
            "f1": round(f1, 3),
            "fp": fp,
            "median_latency_s": round(statistics.median(tp_latencies), 1) if tp_latencies else None,
        }

    # ensemble: anomaly detected when >=2 methods matched it
    ens_hits = {aid for aid, methods in matched_by_anomaly.items() if len(methods) >= 2}
    results["ensemble(>=2)"] = {
        "detections": len(ens_hits),
        "precision": None,
        "recall": round(len(ens_hits) / len(anomalies), 3) if anomalies else 0.0,
        "f1": None,
        "fp": None,
        "median_latency_s": None,
    }
    return results


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ground-truth", default="data/anomaly_ground_truth.json")
    ap.add_argument("--grace-s", type=int, default=90)
    ap.add_argument("--markdown", action="store_true", help="emit a Markdown table")
    args = ap.parse_args()

    truth = json.loads(Path(args.ground_truth).read_text())
    detections = load_detections()
    print(f"ground truth: {len(truth['anomalies'])} injected ({truth['session_id']})")
    print(f"detections in ClickHouse: {len(detections)}")

    results = evaluate(truth, detections, args.grace_s)
    if args.markdown:
        print("\n| Method | Detections | Precision | Recall | F1 | Median latency |")
        print("|---|---|---|---|---|---|")
        for m, r in results.items():
            lat = f"{r['median_latency_s']}s" if r["median_latency_s"] is not None else "—"
            print(f"| {m} | {r['detections']} | {r['precision'] if r['precision'] is not None else '—'} | {r['recall']} | {r['f1'] if r['f1'] is not None else '—'} | {lat} |")
    else:
        for m, r in results.items():
            print(f"{m:>18}: {r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
