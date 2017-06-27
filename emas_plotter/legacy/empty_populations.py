import sys
import os
import matplotlib.pyplot as plt

from operator import add
from pyspark import SparkContext
from spark_utils import average_by_key
from results_loader import load_results


APP_NAME = 'EMAS - Empty populations'


def aggregate(rdd):
    rdd = rdd.map(lambda (key, value): (key, 1) if value < 10 else (key, 0))
    rdd = rdd.reduceByKey(add)
    rdd = rdd.map(lambda ((_experiment, second), value): (second, value))
    rdd = average_by_key(rdd)
    rdd = rdd.map(lambda (second, value): (second, int(value)))
    rdd = rdd.sortByKey()
    data = rdd.collect()
    return zip(*data)


def plot(data_sets):
    for series, data_points in data_sets.iteritems():
        x, y = data_points
        plt.plot(x, y, label=series)

    plt.title('Empty populations (agents count < 10)')
    plt.xlabel('Time [s]')
    plt.ylabel('Number of empty populations')
    plt.axis([0, 85, -10, 250])
    plt.grid(True)
    plt.legend(loc='lower right')
    plt.show()


if __name__ == '__main__':
    results_dir = sys.argv[1]

    sc = SparkContext("local", APP_NAME)

    data_sets = {}
    for series in os.listdir(results_dir):
        series_dir = os.path.join(results_dir, series)
        rdd = load_results(sc, series_dir, 'agents_count')
        data_points = aggregate(rdd)
        data_sets[series] = data_points

    plot(data_sets)
