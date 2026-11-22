# shellcheck shell=bash
# objstore.sh: sourced by offhost-upload.sh and offhost-download.sh.
# With R2_ACCOUNT_ID, R2_BUCKET, R2_ACCESS_KEY_ID, and R2_SECRET_ACCESS_KEY set,
# objects go to R2 through the AWS CLI v2, in the zcash-fleet-ops style: a
# private config file, credentials exported only inside a subshell, and request
# checksums only when required. Otherwise they go to Cloud Storage with
# `gcloud storage` and the access token in /root/.gcs-token.
# Object URLs are written as store://<key>, relative to the bucket.
if [[ -n ${R2_ACCOUNT_ID:-} ]]; then
  for v in R2_BUCKET R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY; do [[ -n ${!v:-} ]] || { echo "missing $v" >&2; exit 1; }; done
  R2_ENDPOINT=${R2_ENDPOINT:-https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com}
  _cfg=$(mktemp); trap 'rm -f "$_cfg"' EXIT
  printf '[default]\nretry_mode = standard\nmax_attempts = 10\ns3 =\n    max_concurrent_requests = %s\n    multipart_chunksize = %s\n' \
    "${R2_MAX_CONCURRENT_REQUESTS:-6}" "${R2_MULTIPART_CHUNKSIZE:-256MB}" > "$_cfg"
  r2_aws() (
    unset AWS_PROFILE AWS_DEFAULT_PROFILE AWS_SESSION_TOKEN
    export AWS_CONFIG_FILE=$_cfg AWS_SHARED_CREDENTIALS_FILE=/dev/null AWS_DEFAULT_REGION=auto
    export AWS_ACCESS_KEY_ID=$R2_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$R2_SECRET_ACCESS_KEY
    export AWS_REQUEST_CHECKSUM_CALCULATION=when_required
    exec aws "$@" --endpoint-url "$R2_ENDPOINT"
  )
  obj_url() { echo "s3://$R2_BUCKET/$1"; }
  # obj_exists returns 0 when the object exists and 1 when it does not; any
  # other failure stops the script, so a credential error never reads as absent.
  obj_exists() {
    local out
    out=$(r2_aws s3api head-object --bucket "$R2_BUCKET" --key "$1" 2>&1) && return 0
    grep -q -E '404|Not Found' <<< "$out" && return 1
    echo "cannot check $1: $out" >&2; exit 1
  }
  obj_put() { r2_aws s3 cp --only-show-errors "$1" "$(obj_url "$2")"; }
  obj_get() { r2_aws s3 cp --only-show-errors "$(obj_url "$1")" "$2"; }
  obj_put_dir() { r2_aws s3 cp --only-show-errors --recursive "$1" "$(obj_url "$2")"; }
  obj_get_dir() { r2_aws s3 cp --only-show-errors --recursive "$(obj_url "$1")" "$2"; }
else
  : "${GCS_BUCKET:?set GCS_BUCKET or the R2_* variables}"
  _g() { gcloud storage --access-token-file=/root/.gcs-token "$@"; }
  obj_url() { echo "gs://$GCS_BUCKET/$1"; }
  obj_exists() {
    local out
    out=$(_g ls "$(obj_url "$1")" 2>&1) && return 0
    grep -q 'matched no objects' <<< "$out" && return 1
    echo "cannot check $1: $out" >&2; exit 1
  }
  obj_put() { _g cp "$1" "$(obj_url "$2")"; }
  obj_get() { _g cp "$(obj_url "$1")" "$2"; }
  obj_put_dir() { _g cp -r "$1/*" "$(obj_url "$2")"; }
  obj_get_dir() { mkdir -p "$2"; _g cp -r "$(obj_url "$1")*" "$2/"; }
fi
