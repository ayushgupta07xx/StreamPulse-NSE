"""Sync source files into Helm chart files/ dirs (CI-friendly, no make)."""

import pathlib
import shutil


def sync(src: str, dst: str, pattern: str) -> None:
    d = pathlib.Path(dst)
    d.mkdir(parents=True, exist_ok=True)
    for f in pathlib.Path(src).glob(pattern):
        if f.is_file():
            shutil.copy2(f, d / f.name)


sync("clickhouse", "helm/charts/clickhouse/files", "*.sql")
sync("apps/flink/jobs", "helm/charts/flink/files/jobs", "*.py")
sync("apps/flink/jobs/common", "helm/charts/flink/files/common", "*.py")
sync("apps/flink/jobs/common/pb", "helm/charts/flink/files/common/pb", "*.py")
sync("observability/grafana/dashboards", "helm/charts/grafana/files/dashboards", "*.json")
sync("data", "helm/charts/flink/files/data", "*.csv")
print("helm chart files synced")
