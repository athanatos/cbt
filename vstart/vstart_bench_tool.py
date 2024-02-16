#!/usr/bin/env python3

import argparse
import copy
import os
import threading
import time
import re
import subprocess
import yaml
from subprocess import Popen, PIPE

"""
General tool for running short tests against single vstart OSDs

Config file should have the form:

base:
  cluster:
    type: vstart
    ...
overlay:
  product:
  -  

"""

class Cluster:
    def make(conf):
        ctype = conf.get('type', 'vstart')
        if ctype == 'vstart':
            return VStartCluster(conf)
        else:
            raise Exception("unrecognized cluster.type {}".format(ctype))

    def start(self): pass
    def stop(self): pass

class VStartCluster(Cluster):
    def __init__(self, conf):
        self.conf = conf
        if 'source_directory' not in self.conf:
            raise Exception('VStartCluster: must specify cluster.source_directory')
        self.base_dir = self.conf['source_directory']
        self.build_dir = os.path.join(self.base_dir, 'build')
        self.bin_dir = os.path.join(self.base_dir, 'bin')
        self.command_timeout = int(self.conf.get('command_timeout', 120))
        self.crimson = self.conf.get('crimson', False)
        if not isinstance(self.crimson, bool):
            raise Exception(
                "VStartCluster: crimson must be boolean (defaults to False), {} found"
                .format(self.crimson))

    def get_args(self):
        ret = [
            '--require-osd-and-client-version', 'squid',
            '--without-dashboard',
            '-X',
            '--redirect-output',
            '-n', '--no-restart'
        ]
        if self.crimson:
            ret += ['--crimson']

    def get_env(self):
        return {
            'MDS': 0,
            'MGR': 1,
            'OSD': 1,
            'MON': 1
        }

    def start(self):
        startup_process = subprocess.run(
            [ '../src/vstart.sh'] + self.get_args(),
            env = self.get_env(),
            cwd = self.build_dir,
            shell = True,
            timeout = self.command_timeout)
        if startup_process.return_code != 0:
            raise Exception(
                "VStartCluster.start startup process exited with code {}"
                .format(startup_process.return_code))
        

    def stop(self): pass
        stop_process = subprocess.run(
            [ '../src/stop.sh'],
            cwd = self.build_dir,
            shell = True,
            timeout = self.command_timeout)
        if stop_process.return_code != 0:
            raise Exception(
                "VStartCluster.stop stop process exited with code {}"
                .format(stop_process.return_code))
            

def recursive_merge(d1, d2):
    if d1 is None:
        return copy.deepcopy(d2)
    elif d2 is None:
        return copy.deepcopy(d1)
    elif not isinstance(d2, dict) or not isinstance(d1, dict):
        return copy.deepcopy(d2)
    elif len(d2) == 0:
        return copy.deepcopy(d1)
    elif len(d1) == 0:
        return copy.deepcopy(d2)
    else:
        return {
            k: recursive_merge(d1.get(k), d2.get(k)) \
            for k in set(d1) | set(d2)
        }

def generate_configs(conf):
    """
    Takes input config and generates resulting config set
    """
    base = conf.get('base', {})

    for name, override in conf.get('overrides', {'default': {}}).items():
        yield name, override, base, recursive_merge(base, override)

def read_configs(path):
    with open(path) as f:
        return generate_configs(yaml.safe_load(f))

def main():
    parser = argparse.ArgumentParser(
        prog='vstart_bench_tool',
        description='Benchmarks RADOS via vstart')
    parser.add_argument(
        '-c', '--config', help='path to yaml config file', required=True
    )
    args = parser.parse_args()

    for name, override, base, config in read_configs(args.config):
        print(yaml.dump(config))
        cluster = Cluster.make(config.get('cluster', {}))


if __name__ == "__main__":
    main()
    
