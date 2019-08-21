import copy
import itertools

import settings
from benchmark.radosbench import Radosbench
from benchmark.fio import Fio
from benchmark.rbdfio import RbdFio
from benchmark.rawfio import RawFio
from benchmark.kvmrbdfio import KvmRbdFio
from benchmark.librbdfio import LibrbdFio
from benchmark.nullbench import Nullbench
from benchmark.cosbench import Cosbench
from benchmark.cephtestrados import CephTestRados
from benchmark.getput import Getput

def get_all(cluster, iteration):
    for benchmark, config in sorted(settings.benchmarks.iteritems()):
        constructor = get_constructor(benchmark)
        default = {"benchmark": benchmark,
                   "iteration": iteration}
        for current in constructor.generate_all_configs(config):
            current.update(default)
            yield constructor(cluster, benchmark, current)


def get_object(cluster, benchmark, bconfig):
    if benchmark == "nullbench":
        return Nullbench
    if benchmark == "radosbench":
        return Radosbench
    if benchmark == "fio":
        return Fio
    if benchmark == "rbdfio":
        return RbdFio
    if benchmark == "kvmrbdfio":
        return KvmRbdFio
    if benchmark == "rawfio":
        return RawFio
    if benchmark == 'librbdfio':
        return LibrbdFio
    if benchmark == 'cosbench':
        return Cosbench
    if benchmark == 'cephtestrados':
        return CephTestRados
    if benchmark == 'getput':
        return Getput
