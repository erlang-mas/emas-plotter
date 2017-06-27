from emas_plotter.aggregators.base import BaseAggregator
from emas_plotter.aggregators import utils


class BestFitnessAggregator(BaseAggregator):

    def aggregate(self, rdd):
        rdd = rdd.reduceByKey(max)
        rdd = rdd.map(lambda ((exp, measurement), val): (measurement, val))
        rdd = self._average_by_key(rdd)
        rdd = rdd.sortByKey()
        return zip(*rdd.collect())
