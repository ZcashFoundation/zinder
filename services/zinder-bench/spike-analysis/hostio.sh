#!/usr/bin/env bash
# Logs per-10s write/read bytes of readable processes with notable IO, plus loadavg and PSI, for attributing latency outliers.
out=/home/ubuntu/.t3/worktrees/zinder/spike-in-process-serving/.tmp/spike-results/host-io.log
declare -A prev
while true; do
  ts=$(date -u +%FT%TZ); line="$ts load=$(cut -d' ' -f1 /proc/loadavg) io_full=$(awk '/full/{print $2}' /proc/pressure/io)"
  for p in /proc/[0-9]*; do
    pid=${p#/proc/}
    io=$(cat $p/io 2>/dev/null) || continue
    rb=$(awk '/^read_bytes/{print $2}' <<<"$io"); wb=$(awk '/^write_bytes/{print $2}' <<<"$io")
    key=$pid; cur=$((rb+wb)); old=${prev[$key]:-$cur}; prev[$key]=$cur
    d=$((cur-old)); if [ $d -gt 20000000 ]; then line="$line | $(cat $p/comm 2>/dev/null)[$pid] +$((d/1000000))MB"; fi
  done
  echo "$line" >> $out
  sleep 10
done
