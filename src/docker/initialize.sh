#!/bin/bash
set -e
set -m

# Example outputs:
# - Cluster is not running:
# ERROR: Unable to connect to host at http://127.0.0.1:8091
# - Cluster is running, but not initialized:
# ERROR: "unknown pool"
# - Cluster is initialized and nodes are healthy:
# ns_1@127.0.0.1 127.0.0.1:8091 healthy active
function cluster_status() {
  set +e # Disable exit-on-error to return the error.
  couchbase-cli server-list \
    -c 127.0.0.1 \
    -u $COUCHBASE_USERNAME \
    -p $COUCHBASE_PASSWORD
  set -e
}

function cluster_health() {
  set +e # Disable exit-on-error to return empty string on error.
  couchbase-cli server-info \
    -c 127.0.0.1 \
    -u $COUCHBASE_USERNAME \
    -p $COUCHBASE_PASSWORD \
    | grep -Po 'unhealthy|healthy'
  set -e
}

function running() {
  local status
  status=$(cluster_status)
  echo $status

  if [[ "$status" == *"127.0.0.1:8091 healthy active"* ]] || [[ "$status" == *"unknown pool"* ]]; then
    echo "> Cluster is running"
    return 0
  fi

  echo "> Cluster is NOT running"
  return 1
}

function healthy() {
  local health
  health=$(cluster_health)
  echo "STATUS: $health"

  if [[ "$health" == "healthy" ]]; then
    echo "> Cluster is healthy"
    return 0
  fi

  echo "> Cluster is NOT healthy"
  return 1
}

function initialized() {
  local status
  status=$(cluster_status)
  echo $status

  if [[ "$status" == *"127.0.0.1:8091 healthy active"* ]]; then
    echo "> Cluster is initialized"
    return 0
  fi

  echo "> Cluster is NOT initialized"
  return 1
}

# Retries $1 times the remaining of the command, with 5s interval.
# Example: retry 5 times to run "curl localhost:8091"
# $ retry 5 curl localhost:8091
function retry() {
  set +e
  for (( i=0; i<"$1"; i++)); do
    if ${@:2}; then
      return 0
    fi
    echo "> Retrying..."
    sleep 5
  done
  return $?
  set -e
}

# Start server as usual in the background and poll it until healthy
/entrypoint.sh couchbase-server &
until running && healthy; do
  sleep 10
done

# Initialize cluster, node and buckets if needed
if ! initialized; then
  echo "> Initializing cluster"
  retry 5 couchbase-cli cluster-init -c 127.0.0.1 \
    --cluster-username $COUCHBASE_USERNAME \
    --cluster-password $COUCHBASE_PASSWORD \
    --services data,index,query \
    --cluster-ramsize 256 \
    --cluster-index-ramsize 256

  echo "> Initializing node"
  retry 5 couchbase-cli node-init -c 127.0.0.1 \
    -u $COUCHBASE_USERNAME \
    -p $COUCHBASE_PASSWORD \
    --node-init-data-path /opt/couchbase/var/lib/couchbase/data \
    --node-init-index-path /opt/couchbase/var/lib/couchbase/index \
    --node-init-analytics-path /opt/couchbase/var/lib/couchbase/analytics
fi

# After initialization, nodes may appear unhealthy for a while.
# So we wait again...
until initialized; do
  sleep 5
done

echo "> Creating missing buckets"

existing_buckets=$(couchbase-cli bucket-list -c 127.0.0.1 \
  -u $COUCHBASE_USERNAME \
  -p $COUCHBASE_PASSWORD)

(
  # Run in a subshell to isolate change to IFS
  IFS=","
  for bucket in $COUCHBASE_BUCKETS; do
    if ! $(echo $existing_buckets | grep -q "^$bucket$"); then
      echo "> Creating bucket '$bucket'"
      couchbase-cli bucket-create -c 127.0.0.1 \
        -u $COUCHBASE_USERNAME \
        -p $COUCHBASE_PASSWORD \
        --bucket=$bucket \
        --bucket-type=couchbase \
        --bucket-ramsize=256 \
        --wait

      # Before creating the primary index on the bucket, we need to wait a little
      # bit for the index service to be available
      sleep 30

      echo "> Creating primary index on $bucket"
      cbq -e couchbase://127.0.0.1 -u $COUCHBASE_USERNAME -p $COUCHBASE_PASSWORD \
        -s "CREATE PRIMARY INDEX ON $bucket"
    else
      echo "> Bucket '$bucket' already exists"
    fi
  done
)

fg 1
