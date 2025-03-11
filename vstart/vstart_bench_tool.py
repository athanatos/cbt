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
import logging
import sys
import time

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

logging.basicConfig(level=logging.DEBUG, stream=sys.stderr)
logger = logging.getLogger(__name__)

def get_merged_env(env):
    return recursive_merge(dict(os.environ), env)

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

def get_git_version(path):
    return str(
        subprocess.check_output(
            ['git', 'rev-parse', 'HEAD'],
            cwd = path
        ).strip(), 'utf-8')

def set_process_cpu_mask(pid, cpumask):
    logger.getChild('set_process_cpu_mask').debug(
        f"setting {pid} to {cpumask}")
    subprocess.check_output(
        ['taskset', '-a', '-c', '-p', cpumask, pid])

def set_attr_from_config(self, defaults, conf):
    self.conf = {}
    for k in conf:
        if k not in defaults:
            raise Exception(
                "{}: unknown config key {}",
                self.__class__.__name__,
                k);
    for k, v in defaults.items():
        if k in conf:
            setattr(self, k, conf[k])
            self.conf[k] = conf[k]
        elif v is None:
            raise Exception(
                "{}: missing required key {}",
                self.__class__.__name__,
                k);
        else:
            setattr(self, k, v)
            self.conf[k] = v


class Cluster:
    def make(conf):
        ctype = conf.get('type', 'vstart')
        cluster_conf = copy.deepcopy(conf)
        del cluster_conf['type']
        if ctype == 'vstart':
            return VStartCluster(cluster_conf)
        else:
            raise Exception(f"unrecognized cluster.type {ctype}")

    class Handle:
        def get_conf_directory(self): pass

        def get_bin_directory(self): pass

        def get_ceph_bin(self):
            return os.path.join(self.get_bin_directory(), 'ceph')

        def get_rbd_bin(self):
            return os.path.join(self.get_bin_directory(), 'rbd')

        def cluster_cmd(self, cmd, positional, named):
            self.logger.getChild('cluster_cmd').info(
                f"({cmd} {positional} {named})")
            subprocess.check_output(
                ([cmd] + \
                 [str(x) for x in positional] +
                 [f"--{k}={v}" for k, v in named.items()]),
                cwd = self.get_conf_directory()
            )

        def ceph_cmd(self, *args, **kwargs):
            self.cluster_cmd(self.get_ceph_bin(), *args, **kwargs)

        def rbd_cmd(self, *args, **kwargs):
            self.cluster_cmd(self.get_rbd_bin(), *args, **kwargs)

        def create_pool(self, name, size, pg_num):
            self.ceph_cmd(
                ['osd', 'pool', 'create', name, pg_num, pg_num, '--size', size]
                , {})
                            
        def create_rbd_image(self, pool, name, size):
            self.rbd_cmd(
                ['create', name],
                {
                    'size': size,
                    'image-format': '2',
                    'rbd_default_features': '3',
                    'pool': pool
                })

            # prepopulate
            self.rbd_cmd([
                'bench',
                f'{pool}/{name}',
            ], {
                'io-type': 'write',
                'io-size': '64K',
                'io-threads': '8',
                'io-pattern': 'seq',
                'io-total': size
            })

    def get_handle(self): pass

    def start(self): pass

    def stop(self): pass
    def get_output(self): pass


class VStartCluster(Cluster):
    def __init__(self, conf):
        self.logger = logger.getChild(type(self).__name__)
        set_attr_from_config(
            self,
            {
                'source_directory': None,
                'command_timeout': 120,
                'crimson': False,
                'cpuset_base': 0,
                'osd_cores': 8,
                'cpuset': '',
                'seastore': False,
                'osd_devices': [],
                'num_osds': 1,
                'osd_options': {},
                'startup_timeout': 60
            },
            conf)
        self.build_directory = os.path.join(self.source_directory, 'build')
        self.bin_directory = os.path.join(self.build_directory, 'bin')
        self.output = {
            'conf': self.conf
        }
        if self.cpuset == '':
            self.cpuset = "{}-{}".format(
                self.cpuset_base,
                self.cpuset_base + (self.osd_cores * self.num_osds)
            )
        self.logger.getChild('__init__').info(
            f"self.cpuset_base={self.cpuset_base}, self.cpuset={self.cpuset}")

    def get_output(self):
        return self.output

    def get_args(self):
        ret = [
            '--without-dashboard',
            '-X',
            '--redirect-output',
            '-n', '--no-restart'
        ]
        if self.crimson:
            ret += ['--crimson']
            ret += ["--crimson-smp", "{}".format(self.osd_cores)]
            if self.seastore:
                ret += ['--seastore']
                if self.osd_devices is not []:
                    ret += ['--seastore-devs', ','.join(self.osd_devices)]
        else:
            if self.osd_devices is not []:
                ret += ['--bluestore-devs', ','.join(self.osd_devices)]
        osd_options = ' '.join([f'--{k}={v}' for k, v in self.osd_options.items()])
        if osd_options != '':
            ret += ['--osd-args', osd_options]
        return ret

    def get_env(self):
        return {
            'MDS': '0',
            'MGR': '1',
            'OSD': str(self.num_osds),
            'MON': '1'
        }

    class Handle(Cluster.Handle):
        def __init__(self, parent):
            self.logger = logger.getChild(f"{type(parent).__name__}.{type(self).__name__}")
            self.conf_directory = parent.build_directory
            self.bin_directory = parent.bin_directory

        def get_conf_directory(self):
            return self.conf_directory

        def get_bin_directory(self):
            return self.bin_directory

    def get_handle(self):
        return VStartCluster.Handle(self)

    def get_out_dir(self):
        return os.path.join(self.build_directory, 'out')

    def set_osd_cpumask(self):
        def osd_pid_file(osdid):
            return os.path.join(
                self.get_out_dir(),
                f'osd.{osdid}.pid')

        def get_pid(osdid):
            with open(osd_pid_file(osdid)) as f:
                return f.read().strip()

        def get_cpumask(base, cores, osdid):
            return f"{base + (osdid * cores)}-{base + ((osdid + 1) * cores)}"

        for osdid in range(self.num_osds):
            set_process_cpu_mask(
                get_pid(osdid),
                get_cpumask(self.cpuset_base, self.osd_cores, osdid))

    def start(self):
        time_start = time.time()
        self.output['git_sha1'] = get_git_version(self.source_directory)
        self.stop()
        cmdline = [ '../src/vstart.sh'] + self.get_args()
        self.logger.getChild('start').info(
            " ".join(cmdline))
        startup_process = subprocess.run(
            cmdline,
            env = get_merged_env(self.get_env()),
            cwd = self.build_directory,
            timeout = self.command_timeout)
        if startup_process.returncode != 0:
            raise Exception(
                f"VStartCluster.start startup process exited with code {startup_process.returncode}")

        time.sleep(1)
        while time.time() < (time_start + self.startup_timeout):
            try:
                self.set_osd_cpumask()
                break
            except Exception as e:
                if time.time() < (time_start + self.startup_timeout):
                    continue
                else:
                    raise e
        
    def stop(self):
        subprocess.run([ 'pkill', '-9', 'crimson-osd'])
        subprocess.run([ 'pkill', '-9', 'ceph-osd'])
        stop_process = subprocess.run(
            [ '../src/stop.sh'],
            cwd = self.build_directory,
            shell = True,
            timeout = self.command_timeout)
        if stop_process.returncode != 0:
            raise Exception(
                f"VStartCluster.stop stop process exited with code {stop_process.returncode}")
        stop_process = subprocess.run(
            [ '../src/stop.sh', '--crimson'],
            cwd = self.build_directory,
            shell = True,
            timeout = self.command_timeout)
        if stop_process.returncode != 0:
            raise Exception(
                f"VStartCluster.stop stop process exited with code {stop_process.returncode}")

class Workload:
    def start(self): pass
    def join(self): pass
    def get_output(self): pass

    def make(conf, handle):
        wtype = conf.get('type', None)
        workload_conf = copy.deepcopy(conf)
        del workload_conf['type']
        if wtype == 'fio_rbd':
            return FioRBD(workload_conf, handle)
        else:
            raise Exception(f"unrecognized cluster.type {wtype}")

class FioRBD(Workload):
    """
    Example config:

    workload:
        type: FioRBD
        bin: fio
        num_pgs: 32
        fio_args:
            iodepth: 32
            rw: randread
            bs: 4096
            numjobs: 4 
            runtime: 120
    """
    def __init__(self, conf, cluster_handle):
        self.logger = logger.getChild(type(self).__name__)
        set_attr_from_config(
            self,
            {
                'bin': 'fio',
                'fio_args': {},
                'cpuset': '',
                'timeout_ratio': 2,
                'rbd_name': 'test_rbd',
                'pool_name': 'test_pool',
                'num_pgs': 32,
                'pool_size': 1,
                'rbd_size': '1G'
            },
            conf)
        self.fio_args['ioengine'] = 'rbd'
        self.fio_args['output-format'] = 'json'
        self.fio_args['direct'] = '1'
        self.fio_args['group_reporting'] = None
        self.fio_args['name'] = 'fio'
        self.fio_args['pool'] = self.pool_name
        self.fio_args['rbdname'] = self.rbd_name
        self.cluster_handle = cluster_handle
        self.conf['fio_args'] = self.fio_args
        try:
            self.timeout = int(self.fio_args['runtime']) * self.timeout_ratio
        except:
            raise Exception("FioRBD: workload.fio_args.runtime required")
        self.output = {
            'conf': self.conf
        }

    def get_output(self):
        return self.output

    def get_fio_args(self):
        def get_arg(x):
            k, v = x
            if v is None:
                return f"-{k}"
            else:
                return f"-{k}={v}"
        return [get_arg(x) for x in self.fio_args.items()]

    def start(self):
        self.cluster_handle.create_pool(self.pool_name, self.pool_size, self.num_pgs)
        self.cluster_handle.create_rbd_image(self.pool_name, self.rbd_name, self.rbd_size)
        args = [self.bin] + self.get_fio_args()
        env = get_merged_env({})
        self.logger.getChild('start').debug("args={}, env={}".format(args, env))
        self.process = subprocess.Popen(
            args, env = env,
            cwd = self.cluster_handle.get_conf_directory(),
            stdout = subprocess.PIPE)

    def join(self):
            self.process.wait(self.timeout)
            self.output['results'] = yaml.safe_load(self.process.stdout)

def main():
    parser = argparse.ArgumentParser(
        prog='vstart_bench_tool',
        description='Benchmarks RADOS via vstart')
    parser.add_argument(
        '-c', '--config', help='path to yaml config file', required=True
    )
    parser.add_argument(
        '-o', '--output', help='path for results', required=False
    )
    args = parser.parse_args()

    outputs = []
    for name, override, base, config in read_configs(args.config):
        output = {}
        print(yaml.dump(config))
        cluster = Cluster.make(config.get('cluster', {}))
        cluster.start()
        
        workload = Workload.make(config.get('workload', {}), cluster.get_handle())
        workload.start()
        workload.join()

        cluster.stop()
        output['name'] = name
        output['cluster'] = cluster.get_output()
        output['workload'] = workload.get_output()
        outputs.append(output)
    results = yaml.dump(outputs)
    print(results)
    if args.output:
        with open(args.output, 'w') as f:
            f.write(results)


if __name__ == "__main__":
    main()
    
