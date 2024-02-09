#!/bin/bash

CEPH_DIR=~/ceph
CBT_DIR=~/cbt
CAB_DIR=${CBT_DIR}/tools/crimson
SJUST_DIR=~/cbt/sjust_folio_testing
BENCH_CONF=${SJUST_DIR}/bench_config.yaml

OUTPUT_DIR=~/output/

#cd ${CEPH_DIR}
#git fetch origin
#git reset --hard origin/sjust/integration/wip-crimson-testing

cd ${CEPH_DIR}/build
ninja -j50 all tests

${CAB_DIR}/crimson_auto_bench.py --run --x smp --config ${BENCH_CONF}
#${CAB_DIR}/crimson_bench_tool.py --client 4 --thread 2 --bench-taskset 16-31 --time 60 --block-size 4096 --dev /dev/nvme0n1 --fio-rbd-rand-write 1
