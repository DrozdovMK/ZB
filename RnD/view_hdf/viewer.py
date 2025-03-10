import sys
import h5py
import numpy as np
from PyQt5 import QtWidgets

from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar
import matplotlib.colors as mcolors
import json

class PlotCanvas(FigureCanvas):
    def __init__(self, parent=None, width=5, height=4, dpi=100):
        self.fig, self.ax = plt.subplots(1, 2, figsize=(width, height), dpi=dpi)
        super().__init__(self.fig)
        self.setParent(parent)
        
        self.ax[0].set_xlabel('Time, ms')
        self.ax[0].set_ylabel('Amplitude')
        self.ax[0].set_ylim([0, 16384])
        self.ax[1].set_xlabel('Classes')
        self.ax[1].set_title('Probability_distribution')
        self.ax[1].set_ylim([0, 1])
        
        self.draw()

    def plot(self, data, prob, title):
        self.ax[0].clear()
        self.ax[0].plot(data)
        self.ax[0].set_title(title)
        self.ax[0].set_xlabel('Time, ms')
        self.ax[0].set_ylabel('Amplitude')
        self.ax[0].set_ylim([0, 16384])
        
        self.ax[1].clear()
        self.ax[1].bar(prob.keys(), prob.values(), alpha = 1, edgecolor='black')
        self.ax[1].set_xlabel('Classes')
        self.ax[1].set_title('Probability_distribution')
        self.ax[1].set_ylim([0, 1])

        self.draw()
    
        


class HDF5Viewer(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        
        self.setWindowTitle('HDF5 Graph Viewer')

        self.plot_main = PlotCanvas(self, width=8, height=5)
        self.toolbar = NavigationToolbar(self.plot_main, self)
        # Create navigation buttons
        self.file_select_button = QtWidgets.QPushButton("Select the hdf5 file")
        
        self.nextButton = QtWidgets.QPushButton('Next', self)
        self.prevButton = QtWidgets.QPushButton('Previous', self)

        self.nextButton.clicked.connect(self.nextDataset)
        self.prevButton.clicked.connect(self.prevDataset)
        self.file_status = QtWidgets.QLabel("no file")
        self.file_select_button.clicked.connect(self.load_dataset)

        # Layout management
        layout = QtWidgets.QVBoxLayout()
        graph_layout = QtWidgets.QHBoxLayout()
        layout.addWidget(self.toolbar)
        graph_layout.addWidget(self.plot_main)
        layout.addLayout(graph_layout)
        
        layout.addWidget(self.file_select_button)
        layout.addWidget(self.file_status)
        
        button_layout = QtWidgets.QHBoxLayout()
        button_layout.addWidget(self.prevButton)
        button_layout.addWidget(self.nextButton)
        layout.addLayout(button_layout)
        
        container = QtWidgets.QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)
        
        self.hdf5_file = None
        self.show()

    def updatePlot(self):
        dataset_name = self.dataset_names[self.current_index]
        print(dataset_name)
        dataset = self.hdf5_file[dataset_name]
        
        # Assume the dataset is 1-dimensional for simplicity
        name = f"{dataset_name}"
        datetime, probs =  dataset.attrs["datetime"], dataset.attrs["probabilities"]

        probs = json.loads(json.loads(probs))
        
        self.plot_main.plot(data = np.array(dataset), 
                            prob = probs,
                            title =  name + "\n " + datetime)
        
    def load_dataset(self):
        options = QtWidgets.QFileDialog.Options()
        options |= QtWidgets.QFileDialog.DontUseNativeDialog
        
        hdf5_file, _ = QtWidgets.QFileDialog.getOpenFileName(
            self, 
            "Выберите файл HDF5", 
            "", 
            "HDF5 Files (*.h5 *.hdf5);;All Files (*)", 
            options=options
        )
        if self.hdf5_file is None:
            self.hdf5_file =  h5py.File(hdf5_file, "r")
        else:
            self.hdf5_file.close()
            self.hdf5_file =  h5py.File(hdf5_file, "r")
        self.file_status.setText(hdf5_file)
        self.dataset_names = sorted(self.hdf5_file.keys())
        self.current_index = 0
        self.updatePlot()

    def nextDataset(self):
        if self.current_index < len(self.dataset_names) - 1:
            self.current_index += 1
            self.updatePlot()

    def prevDataset(self):
        if self.current_index > 0:
            self.current_index -= 1
            self.updatePlot()
    
    def closeEvent(self, event):
        if self.hdf5_file is not None:
            self.hdf5_file.close()  # Закрываем файл, если он открыт
            self.hdf5_file = None
            print("Файл закрыт")
        event.accept()  # Прием события закрытия


def main():
    app = QtWidgets.QApplication(sys.argv)
    viewer = HDF5Viewer()
    # viewer.hdf5_file.close()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()