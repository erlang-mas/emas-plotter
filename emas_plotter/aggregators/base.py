class BaseAggregator(object):

    def aggregate(self, rdd):
        raise NotImplementedError()

    def _average_by_key(self, rdd):
        sum_count = rdd.combineByKey(
            lambda value: (value, 1),
            lambda x, value: (x[0] + value, x[1] + 1),
            lambda x, y: (x[0] + y[0], x[1] + y[1]))
        return sum_count.map(
            lambda (key, (value_sum, count)): (key, value_sum / count))
