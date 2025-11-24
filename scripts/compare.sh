#!/usr/bin/env bash
set -euo pipefail

# Simple side-by-side runner for Ferric vs VMCache under the same env.
# Usage: scripts/compare.sh [--workload tpcc|rndread] [--threads N] [--datasize N] [--runfor N] [--physgb N] [--virtgb N] [--bgwrite 0|1]
# Outputs CSVs: compare-ferric.csv and compare-vmcache.csv in the repo root.

workload="tpcc"
threads=1
datasize=10
runfor=30
physgb=4
virtgb=16
bgwrite=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workload) workload="$2"; shift 2 ;;
    --threads) threads="$2"; shift 2 ;;
    --datasize) datasize="$2"; shift 2 ;;
    --runfor) runfor="$2"; shift 2 ;;
    --physgb) physgb="$2"; shift 2 ;;
    --virtgb) virtgb="$2"; shift 2 ;;
    --bgwrite) bgwrite="$2"; shift 2 ;;
    *) echo "unknown arg: $1"; exit 1 ;;
  esac
done

if [[ "$workload" != "tpcc" && "$workload" != "rndread" ]]; then
  echo "workload must be tpcc or rndread" >&2
  exit 1
fi

pushd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)" >/dev/null

FERRIC_OUT="compare-ferric.csv"
VMCACHE_OUT="compare-vmcache.csv"

echo "[ferric] running workload=${workload} threads=${threads} datasize=${datasize} runfor=${runfor} physgb=${physgb} virtgb=${virtgb} bgwrite=${bgwrite}"
RNDREAD=0
if [[ "$workload" == "rndread" ]]; then
  RNDREAD=1
fi
STATS_INTERVAL_SECS=1 RNDREAD=${RNDREAD} THREADS=${threads} DATASIZE=${datasize} RUNFOR=${runfor} PHYSGB=${physgb} VIRTGB=${virtgb} BGWRITE=${bgwrite} \
  cargo run --quiet --release --bin bench >"${FERRIC_OUT}"
echo "[ferric] wrote ${FERRIC_OUT}"

if [[ ! -d .vmcache_upstream ]]; then
  echo "[vmcache] .vmcache_upstream not found; skip" >&2
  exit 0
fi

echo "[vmcache] building"
make -C .vmcache_upstream -s
echo "[vmcache] running workload=${workload}"
RNDREAD=${RNDREAD} THREADS=${threads} DATASIZE=${datasize} RUNFOR=${runfor} PHYSGB=${physgb} VIRTGB=${virtgb} \
  ./.vmcache_upstream/vmcache >"${VMCACHE_OUT}"
echo "[vmcache] wrote ${VMCACHE_OUT}"

echo "Done. Compare ${FERRIC_OUT} vs ${VMCACHE_OUT} (ts,tx,rmb,wmb,system,threads,datasize,workload,batch)."

popd >/dev/null
