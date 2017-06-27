import re, sys, os, glob
import matplotlib.pyplot as plt

from pyspark import SparkContext
from spark_utils import average_by_key

APP_NAME = "EMAS - Best fitness"

PATH_REGEX = re.compile('\A.+\/(.+)\/(.+)\/(.+)\/(.+)\/(.+)\/(.+)\Z')
ENTRY_REGEX = re.compile('(.+)\s+(.+)\s+\[(.+)\]\s+(.+)\s+<MEASUREMENT-(\d+)>\s+<STEP-(\d+)>\s+\[(.+)\]')
METRIC_ENTRY_REGEX = re.compile('{(\w+),([\d\-\.]+)}')


def process(sc, series_dir):
    log_paths = fetch_log_paths(series_dir)
    rdd = sc.parallelize(log_paths)
    rdd = rdd.flatMap(parse_log_file)
    rdd = average_by_key(rdd)
    rdd = rdd.map(lambda ((_experiment, measurement), value): (measurement, value))
    rdd = average_by_key(rdd)
    rdd = rdd.sortByKey()
    return zip(*rdd.collect())


def fetch_log_paths(root_dir):
    return glob.glob(os.path.join(root_dir, '*', '*', 'console.log'))


def parse_log_file(log_path):
    path_data = PATH_REGEX.match(log_path)
    experiment = path_data.group(3)
    with open(log_path) as log_file:
        for line in log_file:
            match = ENTRY_REGEX.match(line)
            if not match:
                continue
            measurement, raw_metrics = match.group(5, 7)
            metrics = extract_metrics(raw_metrics)
            yield ((experiment, int(measurement)), metrics['total_energy'])


def extract_metrics(raw_metrics):
    metrics = {}
    for (metric, value) in re.findall(METRIC_ENTRY_REGEX, raw_metrics):
        metrics[metric] = float(value)
    return metrics


def plot(data_sets):
    print data_sets
    for series, data_points in data_sets.iteritems():
        x, y = data_points
        plt.plot(x, y, label=series)

    plt.title('EMAS - Energy')
    plt.xlabel('Time [s]')
    plt.ylabel('Total energy')
    # plt.axis([0, 85, -1000, 0])
    # plt.yscale('symlog', linthreshy=0.01)
    plt.grid(True)
    plt.legend(loc='lower right', title='Nodes count')
    plt.show()


if __name__ == '__main__':
    logs_dir = sys.argv[1]
    sc = SparkContext("local", APP_NAME)

    data_sets = {}
    for series in os.listdir(logs_dir):
        series_dir = os.path.join(logs_dir, series)
        data_sets[series] = process(sc, series_dir)

    plot(data_sets)
