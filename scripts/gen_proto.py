"""Generate Python bindings from schemas/protobuf/*.proto.

Two copies are emitted (same sync pattern as the Helm chart files):
- apps/generator/pb/          — producer side (host venv, protobuf 6.x)
- apps/flink/jobs/common/pb/  — consumer side (Flink image, protobuf 4.23.x)

protoc is pinned via grpcio-tools==1.56.2 (bundles protoc 23.x): gencode from
protoc 23 is the newest the Flink container's protobuf 4.23.4 runtime accepts,
and stays forward-compatible with the host's 6.x runtime.

Usage:
    pip install grpcio-tools==1.56.2 "setuptools<81"   # protoc.py needs pkg_resources
    python scripts/gen_proto.py
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROTO_DIR = ROOT / "schemas" / "protobuf"
GEN_TARGET = ROOT / "apps" / "generator" / "pb"
FLINK_TARGET = ROOT / "apps" / "flink" / "jobs" / "common" / "pb"

INIT_DOC = '"""Generated protobuf bindings — regenerate via scripts/gen_proto.py."""\n'


def main() -> int:
    protos = sorted(PROTO_DIR.glob("*.proto"))
    if not protos:
        print(f"no .proto files under {PROTO_DIR}", file=sys.stderr)
        return 1

    GEN_TARGET.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"-I{PROTO_DIR}",
        f"--python_out={GEN_TARGET}",
        *[str(p) for p in protos],
    ]
    subprocess.run(cmd, check=True)
    # explicit encoding: Windows write_text defaults to cp1252
    (GEN_TARGET / "__init__.py").write_text(INIT_DOC, encoding="utf-8")

    FLINK_TARGET.mkdir(parents=True, exist_ok=True)
    for f in GEN_TARGET.glob("*.py"):
        shutil.copy2(f, FLINK_TARGET / f.name)

    files = ", ".join(f.name for f in sorted(GEN_TARGET.glob("*_pb2.py")))
    print(f"generated {files} -> {GEN_TARGET.relative_to(ROOT)} + {FLINK_TARGET.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
