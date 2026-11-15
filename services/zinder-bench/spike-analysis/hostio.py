#!/usr/bin/env python3
"""Every 10 s, log loadavg, IO PSI, and readable processes whose read+write bytes grew by >20 MB (latency attribution)."""
import datetime
import os
import time

OUT = "/home/ubuntu/.t3/worktrees/zinder/spike-in-process-serving/.tmp/spike-results/host-io.log"
prev = {}
while True:
    parts = []
    cur = {}
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open(f"/proc/{pid}/io") as f:
                fields = dict(line.split(":") for line in f.read().splitlines())
            total = int(fields["read_bytes"]) + int(fields["write_bytes"])
            cur[pid] = total
            if pid in prev and total - prev[pid] > 20_000_000:
                with open(f"/proc/{pid}/comm") as f:
                    parts.append(f"{f.read().strip()}[{pid}] +{(total - prev[pid]) // 1_000_000}MB")
        except Exception:
            pass
    prev = cur
    load = open("/proc/loadavg").read().split()[0]
    full = [l for l in open("/proc/pressure/io").read().splitlines() if l.startswith("full")][0].split()[1]
    with open(OUT, "a") as f:
        f.write(f"{datetime.datetime.utcnow().isoformat()}Z load={load} io_{full} {' | '.join(parts)}\n")
    time.sleep(10)
