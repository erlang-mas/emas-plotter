import os
import re

from emas_plotter.spark import sc
from emas_plotter.loaders.base import BaseLoader
from emas_plotter.loaders import constants


class MetricLoader(BaseLoader):

    def __init__(self, series_dir, metric):
        super(MetricLoader, self).__init__(series_dir)
        self.metric = metric

    def load(self):
        log_paths = self._fetch_log_paths()
        rdd = sc.parallelize(log_paths)
        rdd = rdd.flatMap(self._parse_log_file)
        rdd.cache()
        return rdd

    def _fetch_log_paths(self):
        paths = []
        for root, _dirs, filenames in os.walk(self.series_dir):
            for filename in filenames:
                if filename == 'console.log':
                    paths.append(os.path.join(root, filename))
        return paths

    def _parse_log_file(self, path):
        path_data = self._extract_path_data(path)
        with open(path) as log_file:
            for entry in log_file:
                entry_data = self._extract_entry_data(entry)
                if not entry_data:
                    continue
                key = (path_data['experiment'], entry_data['measurement'])
                yield (key, entry_data['metrics'][self.metric])

    def _extract_path_data(self, path):
        match = constants.PATH_REGEX.match(path)
        if not match:
            msg = "Path '{}' does not match required pattern.".format(path)
            raise ValueError(msg)
        return {'experiment': match.group(2)}

    def _extract_entry_data(self, entry):
        match = constants.ENTRY_REGEX.match(entry)
        if not match:
            return
        raw_metrics = match.group(7)
        metrics = self._extract_metrics(raw_metrics)
        return {
            'measurement': int(match.group(5)),
            'metrics': metrics
        }

    def _extract_metrics(self, raw_metrics):
        metrics = {}
        for (metric, value) in re.findall(constants.METRIC_REGEX, raw_metrics):
            metrics[metric] = float(value)
        return metrics
