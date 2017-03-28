import glob
import os
import re


METRIC_PATH_PATTERN = re.compile("\A.*\/(.+)\/(.+)\/(.+)\/(.+)\/(.+)\Z")


def load_results(sc, results_dir, metric):
    metric_paths = fetch_metric_paths(results_dir, metric)
    rdd = sc.parallelize(metric_paths)
    rdd = rdd.flatMap(process_metric_file)
    return rdd


def fetch_metric_paths(results_dir, metric):
    return glob.glob(os.path.join(results_dir, '*', '*', '*', '*', metric))


def process_metric_file(metric_path):
    path_data = extract_path_data(metric_path)
    experiment = path_data.group(2)

    entries = {}
    with open(metric_path) as metric_file:
        for line in metric_file:
            data = line.split(',')
            timestamp, value = int(data[0]), float(data[1])
            entries[timestamp] = value

    min_timestamp = min(entries.keys())

    entries_kv = []
    for timestamp, value in entries.iteritems():
        second = timestamp - min_timestamp
        entries_kv.append(((experiment, second), value))
    return entries_kv


def extract_path_data(metric_path):
    path_data = METRIC_PATH_PATTERN.match(metric_path)
    if not path_data:
        raise ValueError('Invalid metric path', metric_path)
    return path_data
