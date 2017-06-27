import re
import sys
import os
import matplotlib.pyplot as plt

from pyspark import SparkContext


APP_NAME = "EMAS - Population"

METRIC_ENTERY_REGEX = re.compile("\d{4}-\d{2}-\d{2}\s+(\d{2}:\d{2}:\d{2})\.\d{3}\s+\[\w+]\s+<\d+\.(\d+)\.\d+>\s+\[(.*)]")
METRIC_REGEX = re.compile("{(\w+),(\d+)}")

COLORS = ['#2645A3', '#B0044B', '#2F7038', '#F0B800', '#FF1E00', '#039C99', '#7F21CC']


def process(sc, log_path, output_dir=None):
    rdd = sc.textFile(log_path)
    rdd = rdd.flatMap(process_entry)
    rdd = combine_lists_by_key(rdd)
    rdd = rdd.map(lambda (key, val): (key, map(list, zip(*val))))
    rdd = rdd.map(lambda ((population, metric), values): (population, (metric, values)))
    rdd = combine_dicts_by_key(rdd)
    data_sets = rdd.collectAsMap()
    plot(data_sets, output_dir)


def plot(data_sets, output_dir=None):
    for population, metrics in data_sets.items():
        fig = plt.figure()
        ax = fig.add_subplot(111)
        ax.grid(True)

        axes = [ax, ax.twinx(), ax.twinx()]
        fig.subplots_adjust(right=0.75)

        axes[2].spines['right'].set_position(('axes', 1.2))
        axes[2].set_frame_on(True)
        axes[2].patch.set_visible(False)

        axes[0].set_xlabel('Epoch [s]')

        axes[0].set_ylabel('Behaviours count')
        axes[1].set_ylabel('Migrations count')
        axes[2].set_ylabel('Agents count')

        for idx, metric in enumerate(metrics):
            if metric not in ['total_energy']:
                continue
            x, y = metrics[metric]
            axis = axes[0]
            # if metric in ['migration', 'node_migration']:
            #     axis = axes[1]
            # elif metric in ['received_agents', 'agents_count']:
            #     axis = axes[2]
            axis.plot(x, y, label=metric, color=COLORS[idx % len(COLORS)])

        axes[0].legend(loc='lower left')
        axes[1].legend(loc='lower center')
        axes[2].legend(loc='lower right')

        if output_dir:
            fig.savefig(os.path.join(output_dir, population))
        plt.show()


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


def process_entry(entry):
    entry_data = METRIC_ENTERY_REGEX.match(entry)
    if not entry_data:
        return
    raw_time, population, raw_metrics = entry_data.group(1, 2, 3)
    epoch = normalize_time(raw_time)
    metrics = normalize_metrics(raw_metrics)
    for metric, value in metrics.iteritems():
        yield ((population, metric), (epoch, value))


def normalize_time(raw_time):
    hours, minutes, seconds = map(int, raw_time.split(':'))
    return hours * 60 * 60 + minutes * 60 + seconds


def normalize_metrics(raw_metrics):
    metrics = {}
    for (metric, value) in re.findall(METRIC_REGEX, raw_metrics):
        metrics[metric] = float(value)
    return metrics


if __name__ == '__main__':
    log_path = sys.argv[1]

    sc = SparkContext("local", APP_NAME)

    process(sc, log_path)
