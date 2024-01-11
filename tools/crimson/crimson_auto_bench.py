#!/usr/bin/env python3
import yaml
import argparse
import os
import pandas as pd
import matplotlib.pyplot as plt

''' directory structure:
    autobench.{date}/rep:{repeat_id}/test:{test_id}
    test:{test_id} is the output directory from crimson_bench_tool.

    graphic directory:
    graphic.autobench.{date}/test:{test_id}
'''

no_value_attributes= ['crimson', 'output_horizontal', 'perf', 'perf_record']

# transfer to crimson_bench_tool param
def trans(param):
    res = f"--{param}"
    res = res.replace('_', '-')
    return res

def prefix_match(prefix, target):
    if target[:len(prefix)] == prefix:
        return True
    else:
        return False

def read_config(config_file, x = None, comp = None):
    f = open(config_file, 'r')
    _config = yaml.safe_load_all(f.read())
    f.close()

    configs = list()
    for config in _config:
        configs.append(config)

    if x:
        for config in configs:
            if args.x not in config:
                raise Exception("Error: x param should exist in config yaml file")
            if len(config[args.x]) <= 1:
                raise Exception("Error: cannot use single param as x axis")
    if comp:
        if not x:
            raise Exception("Error: must input --x when using --comp")
        same_x = configs[0][x]
        for config in configs:
            if config[x] != same_x:
                raise Exception("Error: x must be the same "\
                                "between different test in --comp mode")
    return configs

def do_bench(configs, repeat):
    # all files' root for this auto bench test
    root = ''
    with os.popen("date +%Y%m%d.%H%M%S") as date:
        line = date.readline()
        res = line.split()[0]
        root = f"autobench.{res}"
    os.makedirs(root)

    # do bench
    for repeat_id in range(repeat):
        repeat_path = f"{root}/rep:{repeat_id}"
        os.makedirs(repeat_path)
        for test_id, test_config in enumerate(configs):
            command = "./crimson_bench_tool.py"
            for key in test_config:
                if key in no_value_attributes:
                    command += f" {trans(key)}"
                else:
                    command += f" {trans(key)} {test_config[key]}"
            test_path_prefix = f"./{repeat_path}/test:{test_id}"
            command += f" --log {test_path_prefix}"
            os.system(command)
    return root

def read_results(root):
    root_files = os.listdir(root)
    failure_log = 'failure_log.txt'
    failure_path = f"{root}/{failure_log}"
    first = False
    if failure_log not in root_files:
        first = True
        os.system(f"touch {failure_path}")

    repeat = len(os.listdir(root)) - 1
    # read root directory to get results
    results = dict()
    # results - repeat_results - test_results - (tester_id, all results)
    for repeat_id in range(repeat):
        repeat_path = f"{root}/rep:{repeat_id}"
        repeat_results = dict()
        tests = os.listdir(repeat_path)
        tests.sort()
        for test_id, test_name in enumerate(tests):
            test_path = f"{repeat_path}/{test_name}"
            test_results = dict()
            files = os.listdir(test_path)
            if '__failed__' not in files:
                result_path = f"{test_path}/result.csv"
                res = pd.read_csv(result_path)
                for case in range(len(res)):
                    test_results[case] = res.iloc[case]
                repeat_results[test_id] = test_results
            else:
                repeat_results[test_id] = False
                if first:
                    os.system(f"echo \"test failed in rep:{repeat_id},"\
                              f" test:{test_id}\" >> {failure_path}")

        results[repeat_id] = repeat_results
    return results

def print_results(results):
    for repeat_id in results:
        for test_id in results[repeat_id]:
            print(f"repeat_id:{repeat_id}, test_id:{test_id}")
            print(results[repeat_id][test_id])

def delete_and_create_at_local(path):
    files = os.listdir('.')
    if path not in files:
        os.makedirs(path)
    else:
        os.system(f"rm -rf {path}")
        os.makedirs(path)

def adjust_results(results, y):
    '''
        transfer to [[[r1t1c1y, r2t1c1y, r3t1c1y], [r1t1c2y, r2t1c2y, r3t1c3y], ...], => pics1
                     [[r1t2c1y, r2t2c1y, r3t2c1y], [r1t2c2y, r2t2c2y, r3t2c3y], ...],  => pics2
                     ...]
        r1t1c1y means repeat 1, test 1, case 1, y
        t - c - r
        one pics for one test case, correspoding to one config block in yaml file
        the most inner list represent multiple repeat target y for one tester(case) in one test
    '''
    test_num = len(results[0])
    all_test_res = list()
    for test_id in range(test_num):
        case_num = 0
        for repeat_id in results:
            if results[repeat_id][test_id]:
                _case_num = len(results[repeat_id][test_id])
                if case_num != 0 and _case_num != case_num:
                    raise Exception("Error: cases num changed\
                                    between different repeat for one same test")
                case_num = _case_num
        if case_num == 0:
            # all repeat of this test failed
            all_test_res.append([])
            continue

        test_res = list()
        for case_id in range(case_num):
            case_res = list()
            for repeat_id in results:
                if results[repeat_id][test_id] != False:
                    match_count = 0
                    case = results[repeat_id][test_id][case_id]
                    for y_f in case.keys().to_list():
                        if prefix_match(y, y_f):
                            y_res = results[repeat_id][test_id][case_id][y_f]
                            match_count += 1
                    if match_count > 1:
                        raise Exception(f"Error: {y} matches multiple y indexes"\
                                        f", please input full y index name")
                    if match_count == 0:
                        raise Exception(f"Error: {y} didn't match any y index")
                    case_res.append(y_res)
                else:
                    pass
            test_res.append(case_res)
        all_test_res.append(test_res)
    return all_test_res

def draw(analysed_results, configs, x, y, res_path, comp):
    for test_id, test in enumerate(analysed_results):
        if test == []:
            print('failed happen')
            continue
        x_data = configs[test_id][x].split()
        y_data = test
        y_data_mean = list()
        for items in y_data:
            y_data_mean.append(sum(items)/len(items))

        df = pd.DataFrame({f'{x}':[], f'{y}':[]})
        for x_id, x_content in enumerate(x_data):
            for y_content in y_data[x_id]:
                df.loc[len(df.index)] = {f'{x}': x_content, f'{y}' : y_content}

        plt.title(f"{x}-{y}".lower())
        plt.xlabel(f"{x}")
        plt.ylabel(f"{y}")
        plt.plot(f'{x}', f'{y}', data=df, linestyle='none',\
                 marker='o', label=f'test_{test_id}')
        plt.plot(x_data, y_data_mean, linestyle='-', label=f'test_{test_id} mean')
        plt.legend()
        # TODO: additional information to graphics
        if not comp:
            plt.savefig(f'{res_path}/test:{test_id}_x:{x}_y:{y}.png'.lower())
            plt.close()

        # raw data to csv
        df.to_csv(f'{res_path}/test:{test_id}_x:{x}_y:{y}.csv'.lower())
        # average to csv
        df_avg = pd.DataFrame({f'{x}':[], f'{y}_avg':[]})
        for x_id, x_content in enumerate(x_data):
            df_avg.loc[len(df_avg.index)] = \
                {f'{x}': x_content, f'{y}_avg' : y_data_mean[x_id]}
        df_avg.to_csv(f'{res_path}/test:{test_id}_x:{x}_y:{y}_avg.csv'.lower())
    if comp:
        plt.savefig(f'{res_path}/test_x:{x}_y:{y}.png'.lower())
        plt.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--run',
                        action='store_true',
                        help= "do bench, analyse results and draw pictures,\
                            require --x, --y")
    parser.add_argument('--bench',
                        action='store_true',
                        help= "only do bench")
    parser.add_argument('--ana',
                        type=str,
                        default=None,
                        help= "the root directory storing the raw bench data, to"\
                            " adjust results and draw pictures."\
                            " require --x, --y, config file should match the results.")
    parser.add_argument('--comp',
                        action='store_true',
                        help= "will merge multiple test into one graphics")
    parser.add_argument('--clean',
                        action='store_true',
                        help= "remove all history graphic and bench results")

    parser.add_argument('--config',
                        type=str,
                        default="bench_config.yaml",
                        help="bench config yaml file path, bench_config.yaml by default")

    parser.add_argument('--repeat',
                        type=int,
                        default=1,
                        help="repeat time for every tests, default to be 1")
    parser.add_argument('--x',
                        type=str,
                        help="x axis of result graphics, the main variable in the target"\
                            " graphics to draw x-y graphics. required when --ana or --run."\
                            " x can be smp, client, thread, osd_op_num_shards, etc. all the"\
                            " parameters that can be multiple in the crimson bench tool can be x.")
    parser.add_argument('--y',
                        nargs='+',
                        type=str,
                        default=["IOPS"],
                        help="the label name of y asix of the result graphics, IOPS by default")

    args = parser.parse_args()
    res_path_prefix = 'graphic'
    files = os.listdir('.')

    _ana = 0
    if args.ana:
        _ana = 1
    if args.run + args.bench + _ana +args.clean != 1:
        raise Exception("Error: should run in one of run/bench/ana/clean")
    if args.run:
        if not args.x:
            raise Exception("Error should input --x to run")
        configs = read_config(args.config, x=args.x, comp=args.comp)
        root = do_bench(configs, args.repeat)
        results = read_results(root)
        print(root)
        res_path = f"{res_path_prefix}.{root}"
        delete_and_create_at_local(res_path)
        for y in args.y:
            analysed_results = adjust_results(results, y)
            draw(analysed_results, configs, args.x, y, res_path, args.comp)

    if args.bench:
        configs = read_config(args.config)
        root = do_bench(configs, args.repeat)
        print(root)

    if args.ana:
        if not args.x:
            raise Exception("Error: should input --x to analyse")
        configs = read_config(args.config, x=args.x, comp=args.comp)
        results = read_results(args.ana)
        res_path = f"{res_path_prefix}.{args.ana}"
        delete_and_create_at_local(res_path)
        for y in args.y:
            analysed_results = adjust_results(results, y)
            draw(analysed_results, configs, args.x, y, res_path, args.comp)

    if args.clean:
        os.system("rm -rf autobench.*")
        os.system("rm -rf graphic.autobench.*")
