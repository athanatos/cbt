#!/usr/bin/env python3

import argparse
import copy
import os
import threading
import time
import re
import statistics
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

def kill_test_procs():
    subprocess.run([ 'pkill', '-9', 'crimson-osd'])
    subprocess.run([ 'pkill', '-9', 'ceph-osd'])
    subprocess.run([ 'pkill', '-9', 'ceph-mon'])
    subprocess.run([ 'pkill', '-9', 'fio'])
    subprocess.run([ 'pkill', '-9', 'rbd'])

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
            return subprocess.check_output(
                ([cmd] + \
                 [str(x) for x in positional] +
                 [f"--{k}={v}" for k, v in named.items()]),
                cwd = self.get_conf_directory()
            )

        def ceph_cmd(self, positional, named):
            return self.cluster_cmd(self.get_ceph_bin(), positional, named)

        def ceph_status(self):
            return yaml.safe_load(
                self.ceph_cmd(['status'], {'format': 'json'}))

        def rbd_cmd(self, *args, **kwargs):
            return self.cluster_cmd(self.get_rbd_bin(), *args, **kwargs)

        def create_pool(self, name, size, pg_num):
            self.ceph_cmd(
                ['osd', 'pool', 'create', name, pg_num, pg_num],
                {'size': size})
                            
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
                'setcpumask': False,
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
                'startup_timeout': 60,
                'crimson_balance_cpu': ''
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
                if self.osd_devices != []:
                    ret += ['--seastore-devs', ','.join(self.osd_devices)]
            if self.crimson_balance_cpu != '':
                ret += ['--crimson-balance-cpu', self.crimson_balance_cpu]
        else:
            if self.osd_devices != []:
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
            self.parent = parent

        def get_conf_directory(self):
            return self.parent.build_directory

        def get_bin_directory(self):
            return self.parent.bin_directory

        def get_osd_pid(self, osdid):
            return self.parent.get_osd_pid(osdid)

        def get_osds(self):
            return range(self.parent.num_osds)

        def run_osd_asok(self, osd, in_args):
            args = [self.get_ceph_bin(), 'daemon', f"osd.{osd}"] + in_args
            self.logger.getChild('run_osd_asok').info(f"osd.{osd} args {' '.join(args)}")
            return subprocess.Popen(
                args,
                cwd = self.get_conf_directory(),
                stdout = subprocess.PIPE)

        def run_osd_asok_decode(self, osd, args):
            self.logger.getChild('run_osd_asok').info(f"osd.{osd} args {' '.join(args)}")
            process = self.run_osd_asok(osd, args)
            ret = yaml.safe_load(process.stdout)
            process.wait()
            self.logger.getChild('run_osd_asok').info(f"osd.{osd} args {' '.join(args)} wait complete")
            return ret

    def get_handle(self):
        return VStartCluster.Handle(self)

    def get_out_dir(self):
        return os.path.join(self.build_directory, 'out')

    def get_osd_pid(self, osdid):
        def osd_pid_file(osdid):
            return os.path.join(
                self.get_out_dir(),
                f'osd.{osdid}.pid')
        with open(osd_pid_file(osdid)) as f:
            return f.read().strip()


    def set_osd_cpumask(self):
        def get_cpumask(base, cores, osdid):
            return f"{base + (osdid * cores)}-{base + ((osdid + 1) * cores)}"

        for osdid in range(self.num_osds):
            set_process_cpu_mask(
                self.get_osd_pid(osdid),
                get_cpumask(self.cpuset_base, self.osd_cores, osdid))


    def is_cluster_unhealthy(self):
        """
        If unhealthy, returns reason.
        If healthy, returns None
        """
        status = self.get_handle().ceph_status()

        osds_up = status['osdmap']['num_up_osds']
        unclean_pgs = list(filter(
            lambda x: x['state_name'] != 'active+clean',
            status['pgmap']['pgs_by_state']))

        if osds_up == self.num_osds and len(unclean_pgs) == 0:
            return None
        else:
            unclean_pgs_str = ', '.join(
                [f"{x['state_name']}:{x['count']}" for x in unclean_pgs])
            return f"{osds_up}/{self.num_osds} up, unclean_pgs: [{unclean_pgs_str}]"
        

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
        if self.setcpumask:
            while time.time() < (time_start + self.startup_timeout):
                try:
                    self.set_osd_cpumask()
                    break
                except Exception as e:
                    if time.time() < (time_start + self.startup_timeout):
                        continue
                    else:
                        raise e

        reason = None
        while time.time() < (time_start + self.startup_timeout):
            reason = self.is_cluster_unhealthy()
            if reason is None:
                break

        if reason != None:
            raise Exception(f"VStartCluster.start cluster still unhealthy: {reason}")
        
    def stop(self):
        kill_test_procs()
        stop_process = subprocess.run(
            [ '../src/stop.sh', '--crimson'],
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
    def prepare(self): pass
    def start(self): pass
    def join(self): pass
    def get_output(self): pass
    def get_summary(self): pass
    def get_estimated_runtime(self): pass

    def make(conf, handle):
        wtype = conf.get('type', None)
        workload_conf = copy.deepcopy(conf)
        del workload_conf['type']
        if wtype == 'fio_rbd':
            return FioRBD(workload_conf, handle)
        elif wtype == 'repeat':
            return Repeat(workload_conf, handle)
        else:
            raise Exception(f"unrecognized cluster.type {wtype}")


class FioRBD(Workload):
    """
    Example config:

    workload:
        type: FioRBD
        bin: fio
        num_pgs: 32
        num_clients: 4
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
                'rbd_size': '1G',
                'num_clients': 1
            },
            conf)
        self.fio_args['ioengine'] = 'rbd'
        self.fio_args['output-format'] = 'json'
        self.fio_args['direct'] = '1'
        self.fio_args['group_reporting'] = None
        self.fio_args['name'] = 'fio'
        self.fio_args['pool'] = self.pool_name
        self.cluster_handle = cluster_handle
        self.conf['fio_args'] = self.fio_args
        try:
            self.timeout = int(self.fio_args['runtime']) * self.timeout_ratio
        except:
            raise Exception("FioRBD: workload.fio_args.runtime required")

    def get_estimated_runtime(self):
        return self.fio_args['runtime']

    def get_output(self):
        return self.output

    def get_rbd_name(self, clientid):
        return f"{self.rbd_name}-{clientid}"

    def get_fio_args(self, clientid):
        args = copy.deepcopy(self.fio_args)
        args['rbdname'] = self.get_rbd_name(clientid)
        return args

    def get_summary(self):
        def summarize(f, c):
            return c(map(f, range(self.num_clients)))
            
        res = self.output['results']

        return {
            'write_iops': summarize(
                lambda cid: res[cid]['jobs'][0]['write']['iops'],
                sum),
            'write_lat_ms': summarize(
                lambda cid: res[cid]['jobs'][0]['write']['lat_ns']['mean'] / 1000000,
                statistics.mean),
            'read_iops': summarize(
                lambda cid: res[cid]['jobs'][0]['read']['iops'],
                sum),
            'read_lat_ms': summarize(
                lambda cid: res[cid]['jobs'][0]['read']['lat_ns']['mean'] / 1000000,
                statistics.mean)
        }

    def prepare(self):
        logger = self.logger.getChild('prepare')
        logger.debug(
            f"creating pool {self.pool_name} size " +
            f"{self.pool_size} pgs {self.num_pgs}")
        self.cluster_handle.create_pool(
            self.pool_name, self.pool_size, self.num_pgs)
        logger.debug(
            f"created pool {self.pool_name} size " +
            f"{self.pool_size} pgs {self.num_pgs}")

        def make_image(clientid):
            name = self.get_rbd_name(clientid)
            logger.debug(f"creating image {name} size {self.rbd_size}")
            self.cluster_handle.create_rbd_image(
                self.pool_name, name, self.rbd_size)
            logger.debug(f"created image {name} size {self.rbd_size}")
        threads = [
            threading.Thread(target=(lambda y: lambda: make_image(y))(x))
            for x in range(self.num_clients)
        ]
        [t.start() for t in threads]
        [t.join() for t in threads]
        logger.debug("prepare complete")

    def start(self):
        def get_fio_arg_list(fio_args):
            def get_arg(x):
                k, v = x
                if v is None:
                    return f"-{k}"
                else:
                    return f"-{k}={v}"
            return [get_arg(x) for x in fio_args.items()]

        def get_fio_proc(clientid):
            args = [self.bin] + get_fio_arg_list(self.get_fio_args(clientid))
            env = get_merged_env({})
            self.logger.getChild('start').debug(f"args={args}")
            return subprocess.Popen(
                args, env = env,
                cwd = self.cluster_handle.get_conf_directory(),
                stdout = subprocess.PIPE)

        self.procs = [get_fio_proc(x) for x in range(self.num_clients)]

    def join(self):
        [x.wait() for x in self.procs]
        self.output = {
            'conf': self.conf
        }
        self.output['results'] = [yaml.safe_load(x.stdout) for x in self.procs]


class Repeat(Workload):
    """
    Example config:

    workload:
        type: repeat
        runs: 3
        workload:
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
                'workload': None,
                'runs': None,
            },
            conf)
        self.workload_obj = Workload.make(self.workload, cluster_handle)
        self.conf = conf

    def get_estimated_runtime(self):
        return self.workload_obj.get_estimated_runtime() * self.runs

    def prepare(self):
        self.workload_obj.prepare()

    def run(self):
        self.output = {
            'workload': self.workload,
            'runs': self.runs,
            'results': []
        }
        self.summaries = []
        for _ in range(self.runs):
            self.workload_obj.start()
            self.workload_obj.join()
            self.output['results'].append(
                self.workload_obj.get_output()
            )
            self.summaries.append(self.workload_obj.get_summary())

    def start(self):
        self.thread = threading.Thread(
            target=self.run,
        )
        self.thread.start()

    def join(self):
        self.thread.join()

    def get_output(self):
        return self.output

    def get_summary(self):
        keys = set()
        for summary in self.summaries:
            keys |= set(summary.keys())
        ret = {}
        for key in keys:
            vals = [s[key] for s in self.summaries]
            ret[f"{key}_mean"] = statistics.mean(vals)
            ret[f"{key}_median"] = statistics.median(vals)
            ret[f"{key}_stddev"] = statistics.stdev(vals)
        return ret


class PerfMonitor:
    def start(self):
        pass

    def join(self):
        pass

    def make(conf, *args):
        wtype = conf.get('type', None)
        confcopy = copy.deepcopy(conf)
        del confcopy['type']
        if wtype == 'perf':
            return Perf(confcopy, *args)
        elif wtype == 'counters':
            return Counters(confcopy, *args)
        else:
            raise Exception(f"unrecognized cluster.type {wtype}")


class Perf(PerfMonitor):
    """
    perfmonitors:
    - type: perf
    """
    def __init__(self, conf, handle, output_path, name):
        self.logger = logger.getChild(type(self).__name__)
        self.conf = conf
        self.handle = handle
        self.output_path = output_path
        self.name = name

    def get_filename(self, osd):
        return os.path.join(
            f"{self.name}-{osd}-perf.data")

    def start(self):
        self.processes = {}
        for osd in self.handle.get_osds():
            args = [
                'perf', 'record', '-g', '--call-graph', 'lbr',
                '-p', self.handle.get_osd_pid(osd),
                '-o', self.get_filename(osd),
                '--', 'sleep', '10'
            ]
            self.logger.getChild('start').info(
                f"starting perf for osd {osd}: {' '.join(args)}")

            self.processes[osd] = subprocess.Popen(
                args,
                cwd = self.output_path,
            )

    def join(self):
        for _, proc in self.processes.items():
            proc.wait(10)


class Counters(PerfMonitor):
    """
    perfmonitors:
    - type: counters 
    """
    def __init__(self, conf, handle, output_path, name):
        self.logger = logger.getChild(type(self).__name__)
        self.conf = conf
        self.handle = handle
        self.output_path = output_path
        self.name = name

    def get_filename(self):
        return os.path.join(
            self.output_path,
            f"{self.name}-counters.yaml")

    def start(self):
        ret = []
        for osd in self.handle.get_osds():
            self.logger.getChild('start').info(
                f"about to dump metrics for osd {osd}")
            val = {'osd': osd}
            val['perfcounters_dump'] = self.handle.run_osd_asok_decode(
                osd, ['perfcounters_dump'])
            val['dump_metrics'] = self.handle.run_osd_asok_decode(
                osd, ['dump_metrics'])
            ret.append(val)
        with open(self.get_filename(), 'w') as f:
            f.write(yaml.dump(ret))

    def join(self):
        pass

def main():
    kill_test_procs()
    parser = argparse.ArgumentParser(
        prog='vstart_bench_tool',
        description='Benchmarks RADOS via vstart')
    parser.add_argument(
        '-c', '--config', help='path to yaml config file', required=True
    )
    parser.add_argument(
        '-o', '--output', help='path for results', required=False
    )
    parser.add_argument(
        '-p', '--perf-dir', help='path for perf output', required=False
    )
    args = parser.parse_args()

    if not args.output and args.perf_dir:
        args.output = os.path.join(args.perf_dir, 'summary.yaml')

    if args.perf_dir:
        os.mkdir(args.perf_dir)

    outputs = []
    for name, override, base, config in read_configs(args.config):
        output = {}
        print(yaml.dump(config))
        cluster = Cluster.make(config.get('cluster', {}))
        cluster.start()
        
        workload = Workload.make(config.get('workload', {}), cluster.get_handle())
        workload.prepare()
        workload.start()

        est_completion = time.monotonic() + workload.get_estimated_runtime()
        perfmonitors = [
            PerfMonitor.make(i, cluster.get_handle(), args.perf_dir, name)
            for i in config.get('perfmonitors', [])
        ]
        if perfmonitors and not args.perf_dir:
            raise Exception("Must specify -p, --perf-dir for PerfMonitors")
        for perfmonitor in perfmonitors:
            perfmonitor.start()
        time.sleep(max(0, est_completion - time.monotonic()))
        for perfmonitor in perfmonitors:
            perfmonitor.join()

        workload.join()

        cluster.stop()
        output['name'] = name
        output['cluster'] = cluster.get_output()
        output['workload_raw'] = workload.get_output()
        output['workload_summary'] = workload.get_summary()
        outputs.append(output)
    results = yaml.dump(outputs)
    print(results)
    if args.output:
        with open(args.output, 'w') as f:
            f.write(results)


if __name__ == "__main__":
    main()
    
