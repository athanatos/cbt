import subprocess
import logging

import settings
import common
import monitoring
import hashlib
import os
import yaml
import itertools
import copy
import json

logger = logging.getLogger('cbt')

class Benchmark(object):
    def __init__(self, cluster, config):
        self.config = config
        self.cluster = cluster
#        self.cluster = Ceph(settings.cluster)
        self.concurrent_id = config.get('concurrent_id', None)
        concurrent_id_segment = ''
        if self.concurrent_id is not None:
            concurrent_id_segment = '%04d/' % (self.concurrent_id,)

        self.archive_dir = "%s/%s/%08d/%s%s%s" % (
            settings.cluster.get('archive_dir'),
            "results",
            config.get('iteration'),
            concurrent_id_segment,
            "id",
            hash(json.dumps(
                self.config, sort_keys=True)))

        self.run_dir = "%s/%08d/%s%s" % (
            settings.cluster.get('tmp_dir'),
            config.get('iteration'),
            concurrent_id_segment,
            self.getclass())
        self.osd_ra = config.get('osd_ra', None)
        self.cmd_path = ''
        self.valgrind = config.get('valgrind', None)
        self.cmd_path_full = '' 
        self.log_iops = config.get('log_iops', True)
        self.log_bw = config.get('log_bw', True)
        self.log_lat = config.get('log_lat', True)
        if self.valgrind is not None:
            self.cmd_path_full = common.setup_valgrind(self.valgrind, self.getclass(), self.run_dir)

        self.osd_ra_changed = False
        if self.osd_ra:
            self.osd_ra_changed = True
        else:
            self.osd_ra = common.get_osd_ra()

    def cleandir(self):
        # Wipe and create the run directory
        common.clean_remote_dir(self.run_dir)
        common.make_remote_dir(self.run_dir)

    def getclass(self):
        return self.__class__.__name__

    def initialize(self):
        if not self.cluster.use_existing and not self.cluster.initialized:
            self.cluster.cleanup()
            self.cluster.initialize()
        self.cleanup()

    def initialize_endpoints(self):
        pass

    def run(self):
        if self.osd_ra and self.osd_ra_changed:
            logger.info('Setting OSD Read Ahead to: %s', self.osd_ra)
            self.cluster.set_osd_param('read_ahead_kb', self.osd_ra)

        logger.debug('Cleaning existing temporary run directory: %s', self.run_dir)
        common.pdsh(settings.getnodes('clients', 'osds', 'mons', 'rgws'), 'sudo rm -rf %s' % self.run_dir).communicate()
        if self.valgrind is not None:
            logger.debug('Adding valgrind to the command path.')
            self.cmd_path_full = common.setup_valgrind(self.valgrind, self.getclass(), self.run_dir)
        # Set the full command path
        self.cmd_path_full += self.cmd_path

        # Store the parameters of the test run
        config_file = os.path.join(self.archive_dir, 'benchmark_config.yaml')
        if not os.path.exists(self.archive_dir):
            os.makedirs(self.archive_dir)
        if not os.path.exists(config_file):
            config_dict = dict(cluster=self.config)
            with open(config_file, 'w') as fd:
                yaml.dump(config_dict, fd, default_flow_style=False)

    def exists(self):
        return False

    def cleanup(self):
        pass

    def dropcaches(self):
        nodes = settings.getnodes('clients', 'osds') 

        common.pdsh(nodes, 'sync').communicate()
        common.pdsh(nodes, 'echo 3 | sudo tee /proc/sys/vm/drop_caches').communicate()

    def __str__(self):
        return str(self.config)

    @staticmethod
    def generate_all_configs(config):
        """
        return all parameter combinations for config
        config: dict - list of params
        iterate over all top-level lists in config
        """
        cycle_over_lists = []
        cycle_over_names = []
        default = {}

        for param, value in config.iteritems():
            if isinstance(value, list):
                cycle_over_lists.append(value)
                cycle_over_names.append(param)
            else:
                default[param] = value

        for permutation in itertools.product(*cycle_over_lists):
            current = copy.deepcopy(default)
            current.update(zip(cycle_over_names, permutation))
            yield current
