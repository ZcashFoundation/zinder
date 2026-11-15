#!/usr/bin/env python3
"""Per-scenario CPU tables from collapsed perf samples of a spike replay.

Usage:
  analyze.py RUN_PREFIX REPLAY_LOG SAMPLES_JSONL CLOCK_OFFSETS SCENARIO_SPEC OUT_DIR

RUN_PREFIX.{stacks,samples}.tsv come from collapse.py. REPLAY_LOG is the
replay's stderr with one `spike_replay_progress` line per block
(`--progress-every 1`); each line marks the end of that block's commit.
CLOCK_OFFSETS holds CLOCK_REALTIME - CLOCK_MONOTONIC (seconds) measured
before and after the run. SCENARIO_SPEC is a JSON list of
{"name", "heights": [..], "phase": "<phase name>|commit"}.

A block's commit window is (end of previous block, end of this block] in
the perf clock. Only samples on the commit thread are attributed; that
thread is the one whose stacks contain the commit loop.
"""
import bisect
import collections
import datetime
import json
import re
import sys

PHASES = [
    # (phase, frame regex) in priority order; the first match wins. The spike
    # wrappers are inlined, so the production frames name the phases.
    ("canonical_prepare", re.compile(
        r"::append_anchor$|spike_next_append_settled_tip|spike_prepare_live_append|"
        r"PreparedLiveAppendCommit.*::new$|CanonicalLiveAppend.*::new$")),
    ("wallet_plan", re.compile(r"spike_plan_canonical_event_range|WalletTransitionPlanner")),
    ("canonical_put", re.compile(r"spike_put_prepared_live_append|PreparedLiveAppendCommit::put_into")),
    ("write_and_readback", re.compile(r"spike_commit_prepared_live_append|write_prepared_live_append")),
    ("wallet_adopt", re.compile(r"spike_complete_planned_transition|complete_planned_transition|finish_publication")),
    ("samples_and_log", re.compile(r"serde_json|tracing_subscriber|tracing_core|BufWriter|serde_core|itoa")),
]
COMMIT_THREAD = re.compile(r"replay::commit_loop|Committer::commit_append")
DERIVE_THREAD = re.compile(r"spike_derive_live_block|pipeline::derive_block|pipeline::spawn_pipeline")

ROOT = r"incrementalmerkletree::frontier::Frontier<H,_>::root"
ORCHARD_COMBINE = r"MerkleHashOrchard as incrementalmerkletree::Hashable>::combine"
# Each group is a list of conditions on one sample's stack; all must hold.
# A plain pattern needs some frame to match; a "!" pattern needs none to.
GROUPS = collections.OrderedDict([
    ("tree: Frontier::root, all pools", [ROOT]),
    ("tree: Frontier::root, Orchard-type pools (Orchard, Ironwood)", [ROOT, ORCHARD_COMBINE]),
    ("tree: Frontier::root, Sapling", [ROOT, "!" + ORCHARD_COMBINE]),
    ("tree: Sinsemilla hashing (MerkleHashOrchard::combine)", [ORCHARD_COMBINE]),
    ("tree: Pedersen hashing (sapling_crypto::pedersen_hash)", [r"sapling_crypto::pedersen_hash"]),
    ("tree: Frontier::append (new commitments)", [r"incrementalmerkletree::frontier::Frontier<H,_>::append|NonEmptyFrontier<H>::append"]),
    ("tree: validate_frontier, incl. its root", [r"CommitmentTreeFrontier::from_canonical_final_state"]),
    ("tree: validate_frontier, excl. root", [r"CommitmentTreeFrontier::from_canonical_final_state", "!" + ROOT]),
    ("tree: via append_anchor (tip checkpoint decode)", [ROOT, r"::append_anchor$"]),
    ("tree: via from_validated_frontiers (seed)", [ROOT, r"CommitmentTreeAccumulator::from_validated_frontiers"]),
    ("tree: via validated_frontiers (snapshot)", [ROOT, r"CommitmentTreeAccumulator::validated_frontiers"]),
    ("compact block: decode", [r"decode_compact_block_artifact|compact_block_from_message|compact_transaction_data_from_message|CompactBlock.*Message>::(decode|merge)"]),
    ("compact block: build and encode", [r"compact_block_message|artifact_builder::compact_|encode_compact_block|CompactBlock.*Message>::encode"]),
    ("canonical replay record: decode", [r"decode_canonical_block_replay|decode_persisted_replay"]),
    ("canonical replay record: encode", [r"encode_canonical_block_replay|canonical_block_replay::encode_payload"]),
    ("hash: SHA-256", [r"sha2::|(?i:sha256)"]),
    ("hash: BLAKE2", [r"(?i)blake2"]),
    ("rocksdb get: all", [r"rocksdb::DBImpl::Get|rocksdb::DBImpl::GetImpl|rocksdb_get|DBCommon<.*>::get_cf_opt|DBCommon<.*>::get_pinned_cf_opt"]),
    ("rocksdb write batch build (put/delete)", [r"rocksdb::WriteBatch(Internal)?::(Put|Delete)|rocksdb_writebatch_(put|delete)"]),
    ("wallet: require_publishable_undo_suffix", [r"require_publishable_undo_suffix"]),
    ("wallet: undo suffix, RocksDB get", [r"require_publishable_undo_suffix", r"rocksdb::DBImpl::Get|rocksdb_get|DBCommon<.*>::get_cf_opt"]),
    ("wallet: undo suffix, WalletReorgUndo::decode", [r"require_publishable_undo_suffix", r"WalletReorgUndo::decode"]),
    ("wallet: load_undo (all callers)", [r"WalletTransitionPlanner::load_undo"]),
    ("wallet: WalletReorgUndo::decode (all callers)", [r"WalletReorgUndo::decode"]),
    ("wallet: advance_settled_tip", [r"WalletTransitionPlanner::advance_settled_tip"]),
    ("wallet: apply_block_rows", [r"WalletTransitionPlanner::apply_block_rows"]),
    ("wallet: apply_block_rows, RocksDB get", [r"WalletTransitionPlanner::apply_block_rows", r"rocksdb::DBImpl::Get|rocksdb_get|DBCommon<.*>::get_cf_opt"]),
    ("wallet: accumulator and digest folds", [r"zinder_wallet_projection::.*(?i:accumulat|digest)|WalletProjectionDigestBuilder|WalletProjectionAccumulator"]),
    ("wallet: overlay map (BTreeMap/HashMap ops)", [r"WalletTransitionPlanner", r"alloc::collections::btree|hashbrown::|std::collections::hash"]),
    ("alloc: malloc/free family", [r"^(malloc|free|realloc|calloc|cfree|__libc_malloc|__libc_free|__libc_realloc|__libc_calloc|posix_memalign|aligned_alloc)$|__rust_alloc|__rust_dealloc|__rust_realloc|__rdl_|operator new|operator delete|alloc::raw_vec|RawVecInner|^\[libc\] malloc internals"]),
    ("libc: memcpy/memmove", [r"^\[libc\] memcpy/memmove|__memcpy|__memmove|^memcpy$|^memmove$"]),
    ("libc: memcmp (key comparisons)", [r"^\[libc\] memcmp|__memcmp|^memcmp$|^bcmp$"]),
    ("libc: memset", [r"^\[libc\] memset|__memset|^memset$"]),
    ("libc: still unresolved leaf", ["@leaf:\\[unknown:libc\\.so\\.6\\]"]),
    ("kernel: page faults", [r"asm_exc_page_fault_\[k\]"]),
    ("serialization: zinder codecs, excl. tree roots", [r"^<?zinder_[a-z_]+::.*::(encode|decode)[a-z_]*$|::encode_[a-z_]+$|::decode_[a-z_]+$", "!" + ROOT]),
    ("serialization: prost", [r"^prost::|<prost::|as prost::message::Message>"]),
    ("serialization: serde", [r"serde"]),
    ("kernel: any", [r"_\[k\]$"]),
])


def compile_group(conditions: list) -> list:
    compiled = []
    for condition in conditions:
        if condition.startswith("@leaf:"):
            compiled.append(("leaf", re.compile(condition[len("@leaf:"):])))
        elif condition.startswith("!"):
            compiled.append(("none", re.compile(condition[1:])))
        else:
            compiled.append(("any", re.compile(condition)))
    return compiled


def group_matches(compiled: list, frames: list) -> bool:
    for kind, pattern in compiled:
        if kind == "leaf":
            if not pattern.search(frames[-1]):
                return False
        elif kind == "none":
            if any(pattern.search(frame) for frame in frames):
                return False
        elif not any(pattern.search(frame) for frame in frames):
            return False
    return True


GROUP_RE = {name: compile_group(conditions) for name, conditions in GROUPS.items()}

# Exclusive partition: each sample goes to the first category whose
# conditions hold, so the shares sum to 100%.
PARTITION = [
    ("RocksDB point reads (get path, incl. value copy)", GROUPS["rocksdb get: all"]),
    ("hashing: commitment-tree roots and appends", [ROOT + r"|incrementalmerkletree::frontier::Frontier<H,_>::append"]),
    ("hashing: BLAKE2 (wallet accumulator, UTXO-set commitment)", [r"(?i)blake2"]),
    ("hashing: SHA-256", [r"sha2::|(?i:sha256)"]),
    ("undo row decode and key-order validation", [r"WalletReorgUndo::decode"]),
    ("allocation (malloc/free family)", GROUPS["alloc: malloc/free family"]),
    ("memcpy/memmove/memcmp (outside the above)", [r"^\[libc\] mem"]),
    ("wallet digest lane folds (u16 add/sub, outside BLAKE2)", [r"WalletProjectionDigestBuilder::(append_row|remove_row)|WalletTransitionPlanner::commit_(add|remove)_unspent|WalletProjectionAccumulator"]),
    ("column-family handle lookup (cf_handle on every read)", [r"DBCommon<.*>::cf_handle"]),
    ("RocksDB write batch build", GROUPS["rocksdb write batch build (put/delete)"]),
    ("planner overlay maps (BTreeMap/HashMap)", GROUPS["wallet: overlay map (BTreeMap/HashMap ops)"]),
    ("kernel (other)", [r"_\[k\]$"]),
    ("planner function self time (raw, load_undo, suffix loop)", ["@leaf:WalletTransitionPlanner::|spike_plan_canonical_event_range"]),
]
PARTITION_RE = [(name, compile_group(conditions)) for name, conditions in PARTITION]

ROCKSDB_GET_SUB = collections.OrderedDict([
    # Each get-path sample goes to the leaf-most frame that matches one of
    # these, so the parts sum to the whole get path.
    ("memtable lookup", r"rocksdb::MemTable::Get|MemTable::GetFromTable|InlineSkipList|SkipListRep::Get|MemTable::KeyComparator|MemTableListVersion"),
    ("SST: block cache (LRU lookup/insert/release)", r"LRUCacheShard::|ShardedCache<.*>::(Lookup|Insert|Release)|LRUHandleTable|RetrieveBlock|MaybeReadBlockAndLoadToCache|GetDataBlockFromCache|LookupFromCache"),
    ("SST: block read (BlockFetcher, pread)", r"BlockFetcher|RandomAccessFileReader::Read|PosixRandomAccessFile::Read|__libc_pread|pread64_\[k\]|ReadBlockContents"),
    ("SST: decompression (Snappy)", r"snappy::|DecompressSerializedBlock|DecompressBlockData|Decompressor|UncompressBlock"),
    ("SST: block checksum (XXH3)", r"VerifyBlockChecksum|ComputeBuiltinChecksum|XXH3|crc32c"),
    ("SST: filter", r"FilterBlockReader|FullFilter|Bloom|Ribbon|KeyMayMatch|FilterBits|IsFilterSkipped"),
    ("SST: index and data block seek", r"IndexBlockIter|DataBlockIter|BlockIter<|BinarySeek|NewDataBlockIterator|ParseNextIndexKey"),
    ("Version/TableCache/GetContext (incl. value save)", r"Version::Get|TableCache::Get|TableCache::FindTable|GetContext|SaveValue|BlockBasedTable::Get$"),
    ("Rust get_cf: copy of the pinned value into a Vec", r"DBCommon<.*>::get_cf_opt|DBCommon<.*>::get_pinned_cf_opt|rocksdb_get_pinned_cf|rocksdb_pinnableslice"),
])
ROCKSDB_SUB_RE = {name: re.compile(pattern) for name, pattern in ROCKSDB_GET_SUB.items()}
HASH_RE = {"SHA-256": re.compile(r"sha2::|(?i:sha256)"), "BLAKE2": re.compile(r"(?i)blake2")}
CALLER_RE = re.compile(r"^(<)?zinder_|^(<)?zcash_|^(<)?orchard|^(<)?sapling_crypto|^(<)?zebra_")
CRC_RE = {"crc32c_3way (SSE4.2 crc32 + PCLMULQDQ)": re.compile(r"crc32c_3way"),
          "Fast_CRC32 (SSE4.2 crc32 only)": re.compile(r"Fast_CRC32"),
          "Slow_CRC32 / software table": re.compile(r"Slow_CRC32|crc32c::ExtendImpl<.*Slow")}

TS = re.compile(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+)Z .*spike_replay_progress.* height=(\d+)")


def parse_ts(text: str) -> float:
    whole, frac = text.split(".")
    base = datetime.datetime.strptime(whole, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc)
    return base.timestamp() + float("0." + frac)


def load_ends(log_path: str, offset: float) -> dict:
    ends = {}
    with open(log_path) as handle:
        for line in handle:
            match = TS.match(line)
            if match:
                ends[int(match.group(2))] = parse_ts(match.group(1)) - offset
    return ends


def classify_phase(frames: list) -> str:
    for phase, pattern in PHASES:
        for frame in frames:
            if pattern.search(frame):
                return phase
    return "other_commit_thread"


def main() -> None:
    prefix, log_path, samples_path, offsets_path, spec_path, out_dir = sys.argv[1:7]
    offsets = [float(x) for x in open(offsets_path).read().split()]
    offset = sum(offsets) / len(offsets)
    ends = load_ends(log_path, offset)
    heights = sorted(ends)
    end_times = [ends[h] for h in heights]
    blocks = {}
    with open(samples_path) as handle:
        for line in handle:
            row = json.loads(line)
            blocks[row["height"]] = row

    stacks = {}
    with open(prefix + ".stacks.tsv") as handle:
        for line in handle:
            sid, stack = line.rstrip("\n").split("\t", 1)
            stacks[int(sid)] = stack.split(";")

    # Commit thread: the tid with the most commit-loop samples. Derive
    # samples come from the pipeline's blocking-pool threads.
    per_tid = collections.Counter()
    commit_like = {sid for sid, frames in stacks.items() if any(COMMIT_THREAD.search(f) for f in frames)}
    derive_like = {sid for sid, frames in stacks.items() if any(DERIVE_THREAD.search(f) for f in frames)}
    rows = []
    with open(prefix + ".samples.tsv") as handle:
        for line in handle:
            time_s, tid, comm, sid = line.rstrip("\n").split("\t")
            sid = int(sid)
            rows.append((float(time_s), int(tid), comm, sid))
            if sid in commit_like:
                per_tid[int(tid)] += 1
    commit_tid = per_tid.most_common(1)[0][0]

    # Attribute samples to heights by commit window.
    by_height = {"commit": collections.defaultdict(list), "derive": collections.defaultdict(list)}
    thread_total = 0
    unattributed = 0
    derive_total = 0
    for time_s, tid, comm, sid in rows:
        if tid == commit_tid:
            kind = "commit"
            thread_total += 1
        elif sid in derive_like:
            kind = "derive"
            derive_total += 1
        else:
            continue
        index = bisect.bisect_left(end_times, time_s)
        if index == 0 or index >= len(heights):
            if kind == "commit":
                unattributed += 1
            continue
        by_height[kind][heights[index]].append(sid)

    spec = json.load(open(spec_path))
    summary = {
        "commit_tid": commit_tid,
        "commit_thread_samples": thread_total,
        "derive_samples": derive_total,
        "unattributed_commit_thread_samples": unattributed,
        "clock_offset_seconds": offsets,
        "logged_blocks": len(heights),
        "first_logged_height": heights[0],
        "last_logged_height": heights[-1],
        "total_samples": len(rows),
        "scenarios": [],
    }
    for scenario in spec:
        summary["scenarios"].append(
            run_scenario(scenario, stacks, by_height[scenario.get("thread", "commit")], blocks, out_dir))
    json.dump(summary, open(f"{out_dir}/summary.json", "w"), indent=1)


def pct(part: int, whole: int) -> float:
    return round(100.0 * part / whole, 2) if whole else 0.0


def run_scenario(scenario: dict, stacks: dict, by_height: dict, blocks: dict, out_dir: str) -> dict:
    name = scenario["name"]
    want_phase = scenario["phase"]
    heights = [h for h in scenario["heights"] if h in by_height or h in blocks]
    phase_counts = collections.Counter()
    base = collections.Counter()  # sid -> samples in the scenario base set
    for height in heights:
        for sid in by_height.get(height, []):
            phase = classify_phase(stacks[sid]) if want_phase != "derive" else "derive"
            phase_counts[phase] += 1
            if want_phase in ("commit", "derive") or phase == want_phase:
                base[sid] += 1
    base_total = sum(base.values())
    commit_total = sum(phase_counts.values())

    # Measured wall time of the same phase from the per-block samples file.
    phase_field = {
        "canonical_prepare": "canonical_prepare_ms",
        "wallet_plan": "wallet_plan_ms",
        "commit": "commit_ms",
        "derive": "derive_ms",
    }.get(want_phase)
    wall_ms = sum(blocks[h][phase_field] for h in heights if h in blocks) if phase_field else None

    self_counts = collections.Counter()
    incl_counts = collections.Counter()
    group_counts = collections.Counter()
    rocks_sub = collections.Counter()
    hash_sites = {key: collections.Counter() for key in HASH_RE}
    crc_counts = collections.Counter()
    partition = collections.Counter()
    with open(f"{out_dir}/{name}.folded", "w") as folded:
        for sid, count in base.most_common():
            frames = stacks[sid]
            folded.write(";".join(frames) + f" {count}\n")
            self_counts[frames[-1]] += count
            for frame in set(frames):
                incl_counts[frame] += count
            for group, compiled in GROUP_RE.items():
                if group_matches(compiled, frames):
                    group_counts[group] += count
            if group_matches(GROUP_RE["rocksdb get: all"], frames):
                # Leaf-most recognizable frame decides, so the parts sum to the whole.
                matched = None
                for frame in reversed(frames):
                    for sub, pattern in ROCKSDB_SUB_RE.items():
                        if pattern.search(frame):
                            matched = sub
                            break
                    if matched:
                        break
                rocks_sub[matched or "(rest of get path)"] += count
            for key, pattern in HASH_RE.items():
                positions = [i for i, frame in enumerate(frames) if pattern.search(frame)]
                if positions:
                    caller = "(no zinder/zcash caller)"
                    for frame in reversed(frames[:positions[0]]):
                        if CALLER_RE.search(frame) and not pattern.search(frame):
                            caller = frame
                            break
                    hash_sites[key][caller] += count
            for part, compiled in PARTITION_RE:
                if group_matches(compiled, frames):
                    partition[part] += count
                    break
            else:
                partition["everything else"] += count
            for key, pattern in CRC_RE.items():
                if any(pattern.search(frame) for frame in frames):
                    crc_counts[key] += count

    scaffold = re.compile(
        r"^\[unknown:libc|^\[libc\]|thread_start|^tokio::|^<tokio::|std::sys::backtrace|FnOnce::call_once|"
        r"core::ops::function|replay::commit_loop|Committer::commit_append|BlockingTask|Harness<|Core<T,S>::poll|"
        r"blocking::pool|pipeline::spawn_pipeline|derive_block|run_replay")
    incl_counts = collections.Counter({frame: count for frame, count in incl_counts.items()
                                       if not (count >= 0.999 * base_total and scaffold.search(frame))})

    def top(counter: collections.Counter) -> list:
        return [{"frame": frame, "samples": count, "pct": pct(count, base_total)}
                for frame, count in counter.most_common(25)]

    result = {
        "name": name,
        "phase": want_phase,
        "blocks": len(heights),
        "height_min": min(heights) if heights else None,
        "height_max": max(heights) if heights else None,
        "commit_thread_samples": commit_total,
        "commit_thread_phase_pct": {phase: pct(count, commit_total) for phase, count in phase_counts.most_common()},
        "base_samples": base_total,
        "base_on_cpu_seconds_at_999hz": round(base_total / 999.0, 3),
        "base_wall_seconds_from_block_samples": round(wall_ms / 1000.0, 3) if wall_ms is not None else None,
        "partition_pct": {part: pct(count, base_total) for part, count in partition.most_common()},
        "groups_inclusive_pct": {group: pct(count, base_total) for group, count in group_counts.items()},
        "rocksdb_get_breakdown_pct_of_base": {sub: pct(count, base_total) for sub, count in rocks_sub.most_common()},
        "hash_call_sites_pct_of_base": {key: {site: pct(count, base_total) for site, count in sites.most_common(8)} for key, sites in hash_sites.items()},
        "crc32c_paths_samples": dict(crc_counts),
        "top_self": top(self_counts),
        "top_inclusive": top(incl_counts),
    }
    json.dump(result, open(f"{out_dir}/{name}.json", "w"), indent=1)
    return {k: v for k, v in result.items() if k not in ("top_self", "top_inclusive")}


if __name__ == "__main__":
    main()
