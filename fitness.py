import sys
import matplotlib.pyplot as plt

from pyspark import SparkContext
from spark_utils import average_by_key
from results_loader import load_results


APP_NAME = 'EMAS - Best fitness'


def aggregate(rdd):
    rdd = rdd.reduceByKey(min)
    rdd = rdd.map(lambda ((_experiment, second), value): (second, value))
    rdd = average_by_key(rdd)
    rdd = rdd.map(lambda (second, value): (second, int(value)))
    rdd = rdd.sortByKey()
    data = rdd.collect()
    return zip(*data)


def plot(data_points):
    x, y = data_points
    plt.plot(x, y)
    plt.title('Best fitness')
    plt.xlabel('Time [s]')
    plt.ylabel('Best fitness')
    plt.axis([0, 85, -1000, 0])
    plt.grid(True)
    plt.show()


if __name__ == '__main__':
    results_dir = sys.argv[1]

    sc = SparkContext("local", APP_NAME)

    rdd = load_results(sc, results_dir, 'fitness')

    data_points = aggregate(rdd)

    plot(data_points)
