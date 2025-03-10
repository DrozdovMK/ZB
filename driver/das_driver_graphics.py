import multiprocessing as mp
import matplotlib.pyplot as plt
import os

try:
    if os.environ['username'] == 'dock':
        import matplotlib
        matplotlib.use('webagg')
        matplotlib.rcParams['webagg.address'] = "0.0.0.0"
except Exception:
    pass


class ProcessPlotter:
    def __init__(self):
        pass

    def run(self):
        while self.pipe.poll():
            data_0 = self.pipe.recv()
            self.ax.cla()
            self.ax.set_ylim(0, 16_000)
            self.ax.plot(data_0.flatten(), label='ADC_0')

        self.fig.canvas.draw()

    def __call__(self, pipe):
        self.pipe = pipe
        self.fig, self.ax = plt.subplots(figsize=(12, 8), tight_layout=True)
        timer = self.fig.canvas.new_timer(interval=100)
        timer.add_callback(self.run)
        timer.start()
        plt.show()


class ADCPlot:
    def __init__(self):
        print('Graphics:', plt.get_backend())
        self.plot_pipe, plotter_pipe = mp.Pipe()
        self.plotter = ProcessPlotter()
        self.plot_process = mp.Process(target=self.plotter, args=(plotter_pipe,), daemon=True)
        self.plot_process.start()

    def plot(self, data):
        self.plot_pipe.send(data)
