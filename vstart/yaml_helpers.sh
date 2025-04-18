PATH=${PATH}:/home/sjust/bin/

function yaml_validate {
  python -c 'import sys, yaml, json; yaml.safe_load(sys.stdin.read())'
}

function yaml2json {
  python -c 'import sys, yaml, json; print(json.dumps(yaml.safe_load(sys.stdin.read())))'
}

function yaml2json_pretty {
  python -c 'import sys, yaml, json; print(json.dumps(yaml.safe_load(sys.stdin.read()), indent=2, sort_keys=False))'
}

function json_validate {
  python -c 'import sys, yaml, json; json.loads(sys.stdin.read())'
}

function json2yaml {
  python -c 'import sys, yaml, json; print(yaml.dump(json.loads(sys.stdin.read())))'
}

function get_results {
        cat $1 | yaml2json | jq '.[] | {name: .name, git_sha1: .cluster.git_sha1, result: .workload_summary}'
}
