import re, sys, os, glob
import matplotlib.pyplot as plt

from pyspark import SparkContext
from spark_utils import average_by_key
from operator import add

from IPython import embed

APP_NAME = "EMAS - Reproductions"

PATH_REGEX = re.compile('\A.+\/(.+)\/(.+)\/(.+)\/(.+)\/(.+)\/(.+)\Z')
ENTRY_REGEX = re.compile('(.+)\s+(.+)\s+\[(.+)\]\s+(.+)\s+<MEASUREMENT-(\d+)>\s+<STEP-(\d+)>\s+\[(.+)\]')
METRIC_ENTRY_REGEX = re.compile('{(\w+),([\d\-\.]+)}')


def process(sc, logs_dir):
    log_paths = fetch_log_paths(logs_dir)
    rdd = sc.parallelize(log_paths)
    rdd = rdd.flatMap(parse_log_file)
    rdd = rdd.filter(lambda ((_nodes_count, _experiment, measurement), _value): 50 < measurement < 250)
    rdd = rdd.map(lambda ((nodes_count, experiment, _measurement), value): ((nodes_count, experiment), value))
    rdd = rdd.reduceByKey(add)
    rdd = rdd.map(lambda ((nodes_count, _experiment), value): (nodes_count, value / 200))
    rdd = average_by_key(rdd)
    rdd = rdd.sortByKey()
    return zip(*rdd.collect())


def fetch_log_paths(root_dir):
    return glob.glob(os.path.join(root_dir, '*', '*', '*', 'console.log'))


def parse_log_file(log_path):
    path_data = PATH_REGEX.match(log_path)
    nodes_count = int(path_data.group(3))
    experiment = path_data.group(4)

    with open(log_path) as log_file:
        for line in log_file:
            match = ENTRY_REGEX.match(line)
            if not match:
                continue
            measurement = int(match.group(5))
            raw_metrics = match.group(7)
            metrics = extract_metrics(raw_metrics)
            key = (nodes_count, experiment, measurement)
            yield (key, metrics['reproduction'])


def extract_metrics(raw_metrics):
    metrics = {}
    for (metric, value) in re.findall(METRIC_ENTRY_REGEX, raw_metrics):
        metrics[metric] = float(value)
    return metrics


def plot(data_points):
    print data_points
    x, y = data_points
    plt.plot(x, y)
    plt.scatter(x, y)

    plt.title('EMAS - Reproductions')
    plt.xlabel('Nodes count')
    plt.ylabel('Reproductions per second')
    # plt.axis([0, 85, -1000, 0])
    # plt.yscale('symlog', linthreshy=0.01)
    plt.grid(True)
    plt.legend(loc='lower right', title='Nodes count')
    plt.show()


if __name__ == '__main__':
    logs_dir = sys.argv[1]
    sc = SparkContext("local", APP_NAME)
    data_points = process(sc, logs_dir)
    plot(data_points)
