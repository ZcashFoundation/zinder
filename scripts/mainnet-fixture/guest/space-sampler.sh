#!/usr/bin/env bash
# space-sampler.sh <out.tsv> <dir>...: every 15 s, record filesystem used bytes and du of each dir
out=$1; shift
while true; do
  line="$(date -u +%FT%TZ)"; for m in /srv/work /srv/archive; do line+="	$(df -B1 --output=used $m 2>/dev/null | tail -1 | tr -d ' ')"; done
  for d in "$@"; do line+="	$(du -s -B1 "$d" 2>/dev/null | cut -f1)"; done
  echo "$line" >> "$out"; sleep 15
done
