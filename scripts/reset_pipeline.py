"""Full pipeline reset: cancel Flink jobs, wipe topics + consumer groups,
recreate topics, resubmit jobs. Gives a pristine event-time state — required
between replay sessions because replaying the same trading date twice merges
into the same event-time windows (see docs/streaming-deep-dive.md).

Usage:
    python scripts/reset_pipeline.py [--ooo-seconds 125] [--jobs validate_enrich,window_bars]
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time

import requests

FLINK = "http://localhost:28088"
RP = ["docker", "exec", "streampulse-redpanda"]
JM = ["docker", "exec", "streampulse-flink-jm"]

ALL_JOBS = ["validate_enrich", "window_bars", "session_bars", "anomaly_online"]
GROUPS = ["flink-validate-enrich", "flink-window-bars", "flink-session-bars", "flink-anomaly-online"]


def sh(cmd: list[str], check: bool = False) -> str:
    r = subprocess.run(cmd, capture_output=True, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} -> {r.stderr.strip()}")
    return r.stdout.strip()


def cancel_all_jobs() -> None:
    jobs = requests.get(f"{FLINK}/jobs/overview", timeout=10).json()["jobs"]
    for j in jobs:
        if j["state"] in ("RUNNING", "RESTARTING", "CREATED"):
            print(f"cancelling {j['name']} ({j['jid']})")
            requests.patch(f"{FLINK}/jobs/{j['jid']}?mode=cancel", timeout=10)
    for _ in range(30):
        live = [
            j
            for j in requests.get(f"{FLINK}/jobs/overview", timeout=10).json()["jobs"]
            if j["state"] in ("RUNNING", "RESTARTING", "CANCELLING")
        ]
        if not live:
            return
        time.sleep(2)


def wipe_topics_and_groups() -> None:
    print(sh(RP + ["sh", "-c", "rpk topic delete -r 'nse\\..*' -X brokers=localhost:9092"]))
    for g in GROUPS:
        sh(RP + ["rpk", "group", "delete", g, "-X", "brokers=localhost:9092"])


def recreate_topics() -> None:
    sys.path.insert(0, "apps")
    from generator.kafka_sink import ensure_topics

    ensure_topics("localhost:29092")
    print("topics recreated")


def submit(job: str, ooo_seconds: int | None) -> None:
    cmd = JM + [
        "flink", "run",
        "-py", f"/opt/streampulse/flink/jobs/{job}.py",
        "--pyFiles", "/opt/streampulse/flink/jobs",
        "-d",
    ]
    if ooo_seconds is not None:
        cmd += ["--ooo-seconds", str(ooo_seconds)]
    out = sh(cmd)
    print(out.splitlines()[-1] if out else f"submitted {job}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ooo-seconds", type=int, default=None, help="watermark bound for replay speed")
    ap.add_argument("--jobs", default="validate_enrich,window_bars", help=f"comma list from {ALL_JOBS}")
    args = ap.parse_args()

    cancel_all_jobs()
    wipe_topics_and_groups()
    recreate_topics()
    for job in [j.strip() for j in args.jobs.split(",") if j.strip()]:
        submit(job, args.ooo_seconds)
    print("pipeline reset complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
