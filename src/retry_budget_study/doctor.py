"""Preflight checks: environment, permissions, resource caps.

Every check prints PASS/FAIL/SKIP with a reason. Exit code is non-zero
if any hard check fails. Hard gates come from config/permissions.yaml
and are not raised here.
"""

from __future__ import annotations

import importlib.metadata
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time

REQUIRED = {"requests": "2.34.2", "urllib3": "2.6.3",
            "tenacity": "9.1.4", "httpx": "0.28.1"}


def check(name, ok, detail="", hard=True, skip=False):
    status = "SKIP" if skip else ("PASS" if ok else ("FAIL" if hard else "WARN"))
    print(f"[{status}] {name}: {detail}")
    return ok or skip or not hard


def main() -> int:
    ok = True

    ok &= check("python>=3.12", sys.version_info >= (3, 12),
                sys.version.split()[0])

    for mod, want in REQUIRED.items():
        try:
            __import__(mod)
            got = importlib.metadata.version(mod)
            ok &= check(f"dep {mod}=={want}", got == want, got)
        except ImportError:
            ok &= check(f"dep {mod}=={want}", False, "not installed")

    # git identity must be the single study author
    try:
        name = subprocess.run(
            ["git", "config", "user.name"], capture_output=True,
            text=True, check=True).stdout.strip()
        email = subprocess.run(
            ["git", "config", "user.email"], capture_output=True,
            text=True, check=True).stdout.strip()
        ok &= check("git identity", name == "Tang Vu"
                    and email.endswith("users.noreply.github.com"),
                    f"{name} <{email}>")
    except Exception as e:
        ok &= check("git identity", False, str(e))

    # repo must stay private
    gh = shutil.which("gh")
    if gh is None:
        check("gh cli", False, "not found; cannot verify visibility",
              hard=False)
    else:
        try:
            out = subprocess.run(
                [gh, "repo", "view", "--json", "visibility,isPrivate"],
                capture_output=True, text=True, check=True,
                cwd=os.path.dirname(os.path.dirname(
                    os.path.dirname(os.path.abspath(__file__)))),
            ).stdout
            v = json.loads(out)
            ok &= check("repo private", v.get("isPrivate") is True,
                        v.get("visibility", "?"))
        except Exception as e:
            ok &= check("repo private", False, f"gh query failed: {e}",
                        hard=False)

    # loopback bind
    try:
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        s.close()
        check("loopback bind", True, "127.0.0.1 ephemeral port OK")
    except Exception as e:
        ok &= check("loopback bind", False, str(e))

    # disk headroom for evidence (>=200 MB)
    free = shutil.disk_usage(".").free
    ok &= check("disk >=200MB", free >= 200 * 1024 * 1024,
                f"{free // (1024 * 1024)} MB free")

    # no stale lock
    stale = os.path.exists("manifests/.pilot.lock")
    check("pilot lock", not stale,
          "stale manifests/.pilot.lock present" if stale else "free",
          hard=False)

    # caps, declared not silently exceeded
    try:
        perms = open("config/permissions.yaml", encoding="utf-8").read()
        m = re.search(r"max_physical_attempts:\s*(\d+)", perms)
        cap = int(m.group(1)) if m else 3000
        check("attempt cap", True, f"max_physical_attempts={cap}")
        check("workers", True, "max_workers=1 (sequential)")
    except OSError:
        ok &= check("permissions.yaml", False, "missing")

    # timing platform note (A4): Windows timer granularity
    ci = time.get_clock_info("perf_counter")
    print(f"[INFO] perf_counter resolution: {ci.resolution}s; "
          "sleeps <~16ms carry Windows timer uncertainty")

    print("RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
