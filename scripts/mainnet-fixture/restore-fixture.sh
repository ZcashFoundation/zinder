#!/usr/bin/env bash
# Restore a benchmark fixture onto a fresh GCP host and stop at a serving
# wallet reader. See docs/runbooks/mainnet-benchmark-fixture.md.
#
# The host gets one disk per fixture component, created from that component's
# snapshot with the selected performance profile, no service account, and no
# external address. The stage tarball (Zinder binaries, the node binary, small
# tools, and the guest scripts) is fetched once from a private bucket with a
# short-lived access token that is removed right after.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: restore-fixture.sh --fixture FILE --stage gs://BUCKET/PATH.tar.zst --name VM [options]

Options:
  --profile fast|modest   Disk profile for component disks: fast is 12,000 IOPS
                          and 375 MiB/s, modest is 3,000 IOPS and 140 MiB/s
                          (default fast).
  --verify-hashes MODE    none, stores, or all: check every listed file against
                          the component listing named in the fixture (default stores).
  --project ID            GCP project (default zfnd-dev-zinder).
  --zone ZONE             GCP zone (default us-east1-c).
  --subnet PATH           Subnetwork self link or path
                          (default projects/zfnd-dev-net-spoke-0/regions/us-east1/subnetworks/dev-default-ue1).
  --machine-type TYPE     Default c4-standard-16, with one thread per core.
  --hours N               Host deletes itself after N hours (default 12, at most 36).
  --receipt FILE          Where to write the JSON receipt (default ./restore-receipt-VM.json).
  -h, --help              Show this help.
EOF
}

fixture="" stage="" name="" profile=fast verify=stores
project=zfnd-dev-zinder zone=us-east1-c machine=c4-standard-16 hours=12 receipt=""
subnet=projects/zfnd-dev-net-spoke-0/regions/us-east1/subnetworks/dev-default-ue1
while [[ $# -gt 0 ]]; do
  case $1 in
    --fixture) fixture=$2; shift 2 ;;
    --stage) stage=$2; shift 2 ;;
    --name) name=$2; shift 2 ;;
    --profile) profile=$2; shift 2 ;;
    --verify-hashes) verify=$2; shift 2 ;;
    --project) project=$2; shift 2 ;;
    --zone) zone=$2; shift 2 ;;
    --subnet) subnet=$2; shift 2 ;;
    --machine-type) machine=$2; shift 2 ;;
    --hours) hours=$2; shift 2 ;;
    --receipt) receipt=$2; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done
die() { echo "restore-fixture: $*" >&2; exit 1; }
[[ -n $fixture && -n $stage && -n $name ]] || { usage >&2; exit 2; }
[[ -f $fixture ]] || die "fixture file $fixture not found"
[[ $name =~ ^[a-z][a-z0-9-]{0,61}$ ]] || die "--name must be a lowercase GCP instance name"
case $profile in
  fast) iops=12000 throughput=375 ;;
  modest) iops=3000 throughput=140 ;;
  *) die "--profile must be fast or modest" ;;
esac
case $verify in none|stores|all) ;; *) die "--verify-hashes must be none, stores, or all" ;; esac
if ! [[ $hours =~ ^[0-9]+$ ]] || (( hours < 1 || hours > 36 )); then die "--hours must be 1 to 36"; fi
receipt=${receipt:-./restore-receipt-$name.json}
schema=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/fixture.schema.json
for tool in gcloud jq; do command -v "$tool" >/dev/null || die "missing $tool"; done

# IAP connections drop now and then. Retry when ssh itself fails (exit 255);
# standard input is buffered so a retry resends it.
gssh() {
  local input rc attempt
  input=$(mktemp)
  cat > "$input"
  for attempt in 1 2 3 4 5; do
    gcloud compute ssh "$name" --project "$project" --zone "$zone" --tunnel-through-iap --quiet \
      --command "$1" < "$input" && rc=0 || rc=$?
    [[ $rc -ne 255 ]] && break
    echo "ssh attempt $attempt failed; retrying" >&2
    sleep 10
  done
  rm -f "$input"
  return "$rc"
}
ts() { date -u +%FT%T.%3NZ; }
declare -A at
mark() { at[$1]=$(ts); echo "$(date -u +%T) $1" >&2; }

mark preflight
jq -e '.format_version == 1 and (.components | length) > 0 and .chain.visible_block_hash and
  ([.components[].kind] | index("node-state") and index("stores"))' "$fixture" >/dev/null \
  || die "fixture must have format_version 1, a chain fence, and node-state and stores components"
# Check everything the guest needs before paying for a host.
jq -e --arg verify "$verify" '
  (.network | type == "string") and (.chain.visible_height | type == "number") and
  all(.components[]; (.name | test("^[a-z][a-z0-9-]*$")) and (.snapshot | type == "string")
    and (.disk_size_gb | type == "number") and (.mount_path | startswith("/"))) and
  ([.components[] | select(.kind == "stores")] | length == 1 and
    (.[0].paths.canonical | type == "string") and (.[0].paths.wallet | type == "string")) and
  ([.components[] | select(.kind == "node-state")] | length == 1) and
  ($verify == "none" or all(.components[] | select($verify == "all" or .kind == "stores");
    (.files_listing | type == "string") and (.files_sha256 | test("^[0-9a-f]{64}$"))))' "$fixture" >/dev/null \
  || die "fixture is missing fields the restore needs (component names, snapshots, sizes, mount paths, store paths, or listings for --verify-hashes $verify)"
if command -v python3 >/dev/null && python3 -c 'import jsonschema' 2>/dev/null; then
  python3 -c 'import json, sys, jsonschema
jsonschema.Draft202012Validator(json.load(open(sys.argv[1]))).validate(json.load(open(sys.argv[2])))' \
    "$schema" "$fixture" || die "fixture does not match fixture.schema.json"
fi
while read -r snap; do
  status=$(gcloud compute snapshots describe "$snap" --project "$project" --format='value(status)' 2>/dev/null) \
    || die "snapshot $snap not found in $project"
  [[ $status == READY ]] || die "snapshot $snap is $status"
done < <(jq -r '.components[].snapshot' "$fixture")
stage_sha=$(gcloud storage cat "$stage.sha256" | awk '{print $1}')
[[ $stage_sha =~ ^[0-9a-f]{64}$ ]] || die "no SHA-256 at $stage.sha256"

mark create_host
labels="purpose=benchmark-fixture,issue=476,fixture=$(jq -r .fixture_id "$fixture" | tr '.' '-' | cut -c1-63)"
disk_args=()
while read -r c; do
  comp=$(jq -r .name <<< "$c") snap=$(jq -r .snapshot <<< "$c") size=$(jq -r .disk_size_gb <<< "$c")
  disk_args+=("--create-disk=name=$name-$comp,auto-delete=yes,size=$size,type=hyperdisk-balanced,provisioned-iops=$iops,provisioned-throughput=$throughput,source-snapshot=$snap,device-name=$comp,interface=NVME")
done < <(jq -c '.components[]' "$fixture")
image=$(gcloud compute images describe-from-family ubuntu-2404-lts-amd64 --project ubuntu-os-cloud --format='value(selfLink)')
gcloud compute instances create "$name" --project "$project" --zone "$zone" \
  --machine-type "$machine" --threads-per-core 1 \
  --network-interface="subnet=$subnet,nic-type=GVNIC,no-address" \
  --no-service-account --no-scopes --metadata enable-oslogin=TRUE --labels "$labels" \
  --provisioning-model STANDARD --instance-termination-action DELETE \
  --termination-time "$(date -u -d "+$hours hours" +%FT%TZ)" \
  --create-disk="name=$name-boot,boot=yes,auto-delete=yes,size=64,type=hyperdisk-balanced,provisioned-iops=3000,provisioned-throughput=140,image=$image,device-name=boot,interface=NVME" \
  "${disk_args[@]}" >&2
mark host_running
for _ in $(seq 1 60); do gssh true < /dev/null 2>/dev/null && break; sleep 10; done
gssh true < /dev/null

mark stage
gcloud auth print-access-token | gssh 'sudo install -m 0600 /dev/stdin /root/.gcs-token'
gssh "trap 'sudo rm -f /root/.gcs-token' EXIT; set -e
  sudo gcloud storage cp --access-token-file=/root/.gcs-token '$stage' /tmp/zfx-stage.tar.zst >/dev/null 2>&1; sudo rm -f /root/.gcs-token
  echo '$stage_sha  /tmp/zfx-stage.tar.zst' | sha256sum --quiet -c
  zstd -dc /tmp/zfx-stage.tar.zst | sudo tar -x -C /tmp" < /dev/null
gssh 'sudo tee /tmp/zfx-stage/fixture.json >/dev/null' < "$fixture"

mark guest_restore
# The guest restore runs as its own unit, so a dropped ssh session cannot stop it.
gssh "sudo systemd-run --unit zfx-restore-guest --collect bash /tmp/zfx-stage/guest/restore-guest.sh /tmp/zfx-stage/fixture.json $verify /var/log/zfx/restore-receipt.json" < /dev/null
until gssh 'sudo test -s /var/log/zfx/restore-receipt.json || ! systemctl is-active -q zfx-restore-guest' < /dev/null; do sleep 20; done
gssh 'sudo test -s /var/log/zfx/restore-receipt.json' < /dev/null \
  || { gssh 'sudo journalctl -u zfx-restore-guest -o cat --no-pager | tail -20' < /dev/null >&2; die "guest restore failed"; }
mark finished
guest=$(gssh 'sudo cat /var/log/zfx/restore-receipt.json' < /dev/null)

steps=$(for k in "${!at[@]}"; do printf '%s\t%s\n' "$k" "${at[$k]}"; done | jq -Rn '[inputs | split("\t") | {(.[0]): .[1]}] | add')
jq -n --argjson guest "$guest" --argjson steps "$steps" --arg name "$name" --arg project "$project" \
  --arg zone "$zone" --arg machine "$machine" --arg profile "$profile" --argjson iops $iops \
  --argjson throughput $throughput --arg stage "$stage" --arg stage_sha "$stage_sha" \
  --arg fixture_sha "$(sha256sum "$fixture" | cut -d' ' -f1)" \
  '{host: {name: $name, project: $project, zone: $zone, machine_type: $machine, threads_per_core: 1},
    profile: {name: $profile, component_disk_iops: $iops, component_disk_throughput_mib_s: $throughput},
    fixture_sha256: $fixture_sha, stage: {object: $stage, sha256: $stage_sha},
    workstation_steps_utc: $steps,
    nothing_to_reader_seconds: ((($steps.finished | sub("\\.[0-9]+Z$"; "Z") | fromdate) - ($steps.preflight | sub("\\.[0-9]+Z$"; "Z") | fromdate))),
    guest: $guest}' > "$receipt"
echo "receipt: $receipt" >&2
jq '{nothing_to_reader_seconds, served_tip: .guest.served_tip, wallet_sample_matches_fixture: .guest.wallet_sample_matches_fixture}' "$receipt"
