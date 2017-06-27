def average_by_key(rdd):
    sum_count = rdd.combineByKey(lambda value: (value, 1),
                                  lambda x, value: (x[0] + value, x[1] + 1),
                                  lambda x, y: (x[0] + y[0], x[1] + y[1]))
    return sum_count.map(lambda (key, (value_sum, count)): (key, value_sum / count))


def combine_lists_by_key(rdd):
    def create_combiner(val):
        return [val]
    def merge_value(acc, val):
        acc.append(val)
        return acc
    def merge_combiners(acc1, acc2):
        return acc1 + acc2
    return rdd.combineByKey(create_combiner, merge_value, merge_combiners)


def combine_dicts_by_key(rdd):
    def create_combiner((key, val)):
        return {key: val}
    def merge_value(acc, (key, val)):
        acc[key] = val
        return acc
    def merge_combiners(acc1, acc2):
        return acc1.update(acc2)
    return rdd.combineByKey(create_combiner, merge_value, merge_combiners)
