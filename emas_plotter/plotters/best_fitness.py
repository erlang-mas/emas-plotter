import matplotlib.pyplot as plt

from emas_plotter.plotters.base import BasePlotter


class BestFitnessPlotter(BasePlotter):

    def plot(self, data_series):
        for series, data_points in data_series.iteritems():
            x, y = data_points
            plt.plot(x, y, label=series)

        plt.title('EMAS - Best fitness')
        plt.xlabel('Time [s]')
        plt.ylabel('Best fitness')
        # plt.axis([0, 85, -1000, 0])
        # plt.yscale('symlog', linthreshy=0.01)
        plt.grid(True)
        plt.legend(loc='lower right', title='Nodes count')
        plt.show()
