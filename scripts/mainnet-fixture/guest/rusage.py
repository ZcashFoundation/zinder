#!/usr/bin/env python3
"""rusage.py <out.json> -- cmd...: run cmd, record wall time and rusage of it and its waited descendants."""
import json, os, subprocess, sys, time
out = sys.argv[1]; cmd = sys.argv[3:] if sys.argv[2] == "--" else sys.argv[2:]
t0 = time.time(); m0 = time.monotonic()
p = subprocess.Popen(cmd)
_, status, ru = os.wait4(p.pid, 0)
m1 = time.monotonic()
rc = os.waitstatus_to_exitcode(status)
json.dump({"command": cmd, "started_unix": t0, "wall_seconds": round(m1 - m0, 3), "exit_code": rc,
           "user_cpu_seconds": ru.ru_utime, "system_cpu_seconds": ru.ru_stime,
           "max_rss_bytes": ru.ru_maxrss * 1024, "block_input_ops": ru.ru_inblock, "block_output_ops": ru.ru_oublock},
          open(out, "w"), indent=1)
sys.exit(rc if rc >= 0 else 128 - rc)
