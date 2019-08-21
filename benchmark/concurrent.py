import copy
import threading
import logging
import itertools

from cluster.ceph import Ceph
import benchmarkfactory
from benchmark import Benchmark

logger = logging.getLogger('cbt')

class Concurrent(Benchmark):
    def __init__(self, cluster, config):
        super(Concurrent, self).__init__(cluster, config)

        def merge_config(shared, specific, i):
            ret = copy.deepcopy(shared)
            ret.update(specific)
            ret['iteration'] = config['iteration']
            ret['concurrent_id'] = i
            return ret

        self.benchmarks = [
            benchmarkfactory.get_object(c.get('benchmark'))(
                cluster, merge_config(config.get('shared', {}),
                                      c.get('config', {}),
                                      i))
            for i, c in zip(
                    itertools.count(0), config.get('benchmarks', []))]

    def initialize(self):
        super(Concurrent, self).initialize()
        [x.initialize() for x in self.benchmarks]

    def run(self):
        super(Concurrent, self).run()

        threads = [
            threading.Thread(target=(lambda _x: lambda: _x.run())(x))
            for x in self.benchmarks]
        [x.start() for x in threads]
        [x.join() for x in threads]

    @staticmethod
    def generate_all_configs(config):
        """
        Takes the set of configs for each benchmark generated
        via Benchmark.generate_all_configs and emits the product
        of those streams.
        """
        def subconfigs(c):
            name = c.get('benchmark')
            obj = benchmarkfactory.get_object(name)
            return ({ 'benchmark': name, 'config': x }
                    for x in obj.generate_all_configs(c.get('config', {})))
                    
        subconfig_generators = itertools.product(
            *map(subconfigs, config.get('benchmarks', [])))
        shared_generator = Benchmark.generate_all_configs(config.get('shared', {}))
        return ( {'shared': s, 'benchmarks': bs} for s, bs in
                 itertools.product(shared_generator, subconfig_generators))
