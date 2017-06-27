import sys
import os

from pyspark import SparkContext

from emas_plotter import loaders
from emas_plotter import aggregators
from emas_plotter import plotters


if __name__ == '__main__':
    logs_dir = sys.argv[1]

    data_series = {}
    for series in os.listdir(logs_dir):
        series_dir = os.path.join(logs_dir, series)

        loader = loaders.MetricLoader(series_dir, 'best_fitness')
        rdd = loader.load()

        aggregator = aggregators.BestFitnessAggregator()
        data_series[series] = aggregator.aggregate(rdd)

    print data_series

    plotter = plotters.BestFitnessPlotter()
    plotter.plot(data_series)
