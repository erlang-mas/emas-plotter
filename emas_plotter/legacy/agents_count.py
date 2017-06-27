import sys
import os
import matplotlib.pyplot as plt

from operator import add
from pyspark import SparkContext
from spark_utils import average_by_key
from results_loader import load_results


APP_NAME = 'EMAS - Agents count'


def aggregate(rdd, op=None):
    rdd = rdd.reduceByKey(op) if op else average_by_key(rdd)
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

    plt.xlabel('Time [s]')
    plt.ylabel('Agents count')
    # plt.xlim(5, 85)
    plt.grid(True)
    plt.legend(loc='lower right')

if __name__ == '__main__':
    results_dir = sys.argv[1]

    sc = SparkContext("local", APP_NAME)

    plt.figure(1)

    plt.subplot(221)
    plt.title('Min agents count')
    plt.ylim(-10, 150)

    data_sets = {}
    for series in os.listdir(results_dir):
        series_dir = os.path.join(results_dir, series)
        rdd = load_results(sc, series_dir, 'agents_count')
        data_points = aggregate(rdd, min)
        data_sets[series] = data_points
    plot(data_sets)

    plt.subplot(222)
    plt.title('Max agents count')

    data_sets = {}
    for series in os.listdir(results_dir):
        series_dir = os.path.join(results_dir, series)
        rdd = load_results(sc, series_dir, 'agents_count')
        data_points = aggregate(rdd, max)
        data_sets[series] = data_points
    plot(data_sets)

    plt.subplot(223)
    plt.title('Average agents count')
    plt.ylim(-10, 200)

    data_sets = {}
    for series in os.listdir(results_dir):
        series_dir = os.path.join(results_dir, series)
        rdd = load_results(sc, series_dir, 'agents_count')
        data_points = aggregate(rdd)
        data_sets[series] = data_points
    plot(data_sets)

    plt.subplot(224)
    plt.title('Total agents count')

    data_sets = {}
    for series in os.listdir(results_dir):
        series_dir = os.path.join(results_dir, series)
        rdd = load_results(sc, series_dir, 'agents_count')
        data_points = aggregate(rdd, add)
        data_sets[series] = data_points
    plot(data_sets)

    plt.show()
