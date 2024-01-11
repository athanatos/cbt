#!/bin/bash

CEPH_DIR=~/ceph
CBT_DIR=~/cbt
SJUST_DIR=~/cbt/sjust_folio_testing
BENCH_CONF=${SJUST_DIR}/bench_config.yaml

OUTPUT_DIR=~/output/

${CBT_DIR}/tools/crimson/crimson_auto_bench.py --run --x clients --config ${BENCH_CONF}
