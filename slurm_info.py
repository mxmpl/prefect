#!/usr/bin/env python3
"""Dump a comprehensive, read-only snapshot of the SLURM cluster.

Runs a battery of SLURM query commands (sinfo, scontrol, sacctmgr, sshare,
sprio, squeue, ...) and prints one big report to stdout so it lands in the
Prefect flow-run logs (the flow runs with ``log_prints=True``).

When the Hyperloop env is present -- as injected by the ``python_launcher``
flow via ``hyperloop_config`` -- the same report is also shipped to a durable
S3 log at ``$LOG_DEST/<run-name>.log``, exactly like crawler.py. Outside a
Prefect run (e.g. run directly on a login node) it just prints.

Everything is best-effort: a missing binary, a non-zero exit, or a slow
command degrades to a note in its section instead of aborting the report.
"""

from __future__ import annotations

import getpass
import io
import os
import shutil
import socket
import subprocess
import sys
from datetime import datetime, timezone

# Durable-log plumbing is optional so this script also runs standalone.
try:
    from devai_prefect.utils.hyperloop_context import (
        hyperloop_file_transfer_context,
        setup_hyperloop_from_env,
    )
    from prefect.runtime import flow_run

    _HAS_PREFECT = True
except Exception:  # pragma: no cover - only when run outside the worker env
    _HAS_PREFECT = False

CMD_TIMEOUT = 60  # seconds per command
_BAR = "=" * 78


def _current_user() -> str:
    try:
        return getpass.getuser()
    except Exception:
        return os.environ.get("USER") or os.environ.get("LOGNAME") or "unknown"


def run(cmd: list[str]) -> str:
    """Run one query command, returning ``"$ cmd\\n<output>"`` (never raises)."""
    shown = " ".join(cmd)
    if shutil.which(cmd[0]) is None:
        return f"$ {shown}\n[skipped: '{cmd[0]}' not found on PATH]\n"
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=CMD_TIMEOUT)
    except subprocess.TimeoutExpired:
        return f"$ {shown}\n[timed out after {CMD_TIMEOUT}s]\n"
    except Exception as e:  # noqa: BLE001 - report anything, never abort
        return f"$ {shown}\n[error: {e}]\n"

    out = proc.stdout.rstrip("\n")
    err = proc.stderr.rstrip("\n")
    body = out
    if err:
        body = f"{body}\n[stderr] {err}" if body else f"[stderr] {err}"
    if not body:
        body = "[no output]"
    return f"$ {shown}\n{body}\n"


def section(title: str, *commands: list[str]) -> str:
    parts = [_BAR, f"# {title}", _BAR, ""]
    parts.extend(run(cmd) for cmd in commands)
    return "\n".join(parts)


def build_report(user: str) -> str:
    """Assemble the full cluster report for ``user``."""
    header = "\n".join(
        [
            _BAR,
            "# SLURM CLUSTER SNAPSHOT",
            _BAR,
            f"host:      {socket.gethostname()}",
            f"user:      {user}",
            f"generated: {datetime.now(timezone.utc).isoformat()}",
            "",
        ]
    )

    sections = [
        section(
            "VERSION & CONTROLLER CONFIG",
            ["scontrol", "--version"],
            # Full slurm.conf as the controller sees it: scheduler type,
            # PriorityType / PriorityWeight*, Def/MaxMemPer*, MaxJobCount, etc.
            ["scontrol", "show", "config"],
        ),
        section(
            "PARTITIONS",
            # Full definitions: TimeLimit, DefaultTime, MaxNodes, TRES,
            # AllowQos, AllowGroups, PriorityJobFactor, ...
            ["scontrol", "show", "partition"],
            # Compact: avail, timelimit, #nodes, state, CPUs(A/I/O/T), GRES.
            ["sinfo", "-o", "%P %.5a %.14l %.6D %.6t %C %G"],
        ),
        section(
            "NODES & RESOURCES",
            # Per-node: partition, state, #cpus, mem(MB), GRES (GPUs), free mem.
            ["sinfo", "-N", "-o", "%N %P %.6t %.4c %.9m %.22G %.8e"],
            # Partition-level node-state summary.
            ["sinfo", "-s"],
        ),
        section(
            "QOS (priority & limits)",
            # --parsable2 => every column, no truncation. This is where QoS
            # Priority, MaxWall, GrpTRES, MaxTRES, MaxTRESPerUser, MaxJobs*,
            # MaxSubmit*, and Flags live.
            ["sacctmgr", "--parsable2", "show", "qos"],
        ),
        section(
            "TRES (trackable resources)",
            ["sacctmgr", "--parsable2", "show", "tres"],
        ),
        section(
            f"YOUR ASSOCIATIONS & LIMITS (user={user})",
            [
                "sacctmgr",
                "--parsable2",
                "show",
                "assoc",
                f"user={user}",
                "format=Cluster,Account,User,Partition,QOS,DefaultQOS,"
                "GrpTRES,MaxTRES,MaxWall,MaxJobs,MaxSubmitJobs,Priority",
            ],
            ["sacctmgr", "--parsable2", "show", "user", user, "withassoc"],
        ),
        section(
            "FAIRSHARE & JOB PRIORITY",
            # Your fairshare / usage / effective share.
            ["sshare", "-U", "-u", user, "-l"],
            # Priority breakdown (age, fairshare, QOS, TRES, ...) for pending jobs.
            ["sprio", "-l"],
        ),
        section(
            "RESERVATIONS",
            ["scontrol", "show", "reservation"],
        ),
        section(
            f"CURRENT QUEUE (user={user})",
            ["squeue", "-u", user, "-o", "%.18i %.9P %.28j %.8T %.10M %.10l %.6D %R"],
        ),
    ]

    return header + "\n" + "\n".join(sections)


def main() -> int:
    user = os.environ.get("SLURM_INFO_USER") or _current_user()
    report = build_report(user)

    # Primary output: stdout -> Prefect flow-run logs (log_prints=True).
    print(report, flush=True)

    if not _HAS_PREFECT:
        return 0

    transfer = setup_hyperloop_from_env()
    log_dest = os.environ.get("LOG_DEST")
    if transfer is None or not log_dest:
        print("[hyperloop] no config / LOG_DEST in env; skipping durable S3 log", file=sys.stderr)
        return 0

    name = flow_run.get_name() or f"slurm-info-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}"
    dest_path = f"{log_dest}/{name}.log"
    with hyperloop_file_transfer_context(dest_path, transfer, mode="w") as f:
        assert isinstance(f, io.TextIOBase)
        f.write(report)
    print(f"[hyperloop] durable log written to {dest_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
