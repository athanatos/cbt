#!/bin/bash

CEPH_DIR=~/ceph
CBT_DIR=~/cbt
CAB_DIR=${CBT_DIR}/tools/crimson
SJUST_DIR=~/cbt/sjust_folio_testing
BENCH_CONF=${SJUST_DIR}/bench_config.yaml

OUTPUT_DIR=~/output/

cd ${CAB_DIR}
${CAB_DIR}/crimson_auto_bench.py --run --x client --config ${BENCH_CONF}
