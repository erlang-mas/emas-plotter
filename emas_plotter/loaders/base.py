class BaseLoader(object):

    def __init__(self, series_dir):
        self.series_dir = series_dir

    def load(self):
        raise NotImplementedError()
