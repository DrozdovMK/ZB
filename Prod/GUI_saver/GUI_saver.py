script_folder  = '../../scripts/'
import sys
sys.path.append(script_folder)

import crop
import h5py
import os
import numpy as np
import pandas as pd
import pyqtgraph as pg
from PyQt5.QtWidgets import (QApplication, QMainWindow, QGridLayout, QWidget, QFrame,
                             QVBoxLayout, QPushButton, QLineEdit, QLabel, QFormLayout, QSizePolicy)
import signal_processing as sp


class ExampleApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('Datasets_loader')
        self.setGeometry(400, 200, 900, 700)
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        grid_layout = QGridLayout()
        central_widget.setLayout(grid_layout)
        
        # Создаем четыре фрейма для каждой части окна
        frame1 = QFrame()
        frame1.setFrameShape(QFrame.StyledPanel)
        frame2 = QFrame()
        frame2.setFrameShape(QFrame.StyledPanel)
        frame3 = QFrame()
        frame3.setFrameShape(QFrame.StyledPanel)
        frame4 = QFrame()
        frame4.setFrameShape(QFrame.StyledPanel)
        
        # Размещаем фреймы в сетке
        grid_layout.addWidget(frame1, 0, 0)  # Позиция графика сырого сигнала
        grid_layout.addWidget(frame2, 0, 1)  # Позиция кнопок сохранения
        grid_layout.addWidget(frame3, 1, 1)  # Позиция задаваемых параметров
        grid_layout.addWidget(frame4, 1, 0)  # Позиция графика после фильтра
        
        #Рамещаем виджеты в ячейках сетки
        frame1_layout = QVBoxLayout() # Вертикальная компоновка 
        frame2_layout = QFormLayout() # Компоновка в формате ('текст', QLineEdit())
        frame3_layout = QFormLayout() # Компоновка в формате ('текст', QLineEdit())
        frame4_layout = QVBoxLayout() # Вертикальная компоновка 
        
        frame1.setLayout(frame1_layout)
        frame2.setLayout(frame2_layout)
        frame3.setLayout(frame3_layout)
        frame4.setLayout(frame4_layout)
                         
        # Виджеты построения графиков
        self.graphwidget_raw = pg.PlotWidget(title = 'Сырой сигнал')
        self.graphwidget_filtered = pg.PlotWidget(title = 'Сигнал после фильтра')
        
        # Сохранение файла и состояние системы
        self.savepath_directory = QLineEdit('../data/hdf5_adaptive/')
        self.button_save = QPushButton("Сохранить в файл")
        self.button_save.setStyleSheet("background-color: green; color: white")
        self.button_save.clicked.connect(lambda: self.save_or_discard_file('save'))
        self.button_discard = QPushButton("Пропустить")
        self.button_discard.setStyleSheet("background-color: red; color: white")
        self.button_discard.clicked.connect(lambda: self.save_or_discard_file('discard'))
        self.alarm_state = QLabel('0')
        self.system_state = QLabel('Нет тревоги')
        
        self.warning_label = QLabel()
        self.count_records = QLabel()
        
        # Параметры
        self.start_programm_button = QPushButton('Старт')
        self.start_programm_button.setStyleSheet("background-color: white; color: green")
        self.is_timer_running = False
        self.record_status = QLabel('Запись не идет')

        self.input_cooling_time = QLineEdit('2')
        self.cooling_time = int(self.input_cooling_time.text())
        
        self.input_indent_signal = QLineEdit('500')
        self.indent = int(self.input_indent_signal.text())
        
        self.input_detector_threshold = QLineEdit('150')
        self.detector_threshold = int(self.input_detector_threshold.text())

        self.input_interaction = QLineEdit('Hit')
        
        self.input_std = QLineEdit('32')
        self.std_window = int(self.input_std.text())
        
        
        self.current_data = []
        self.stored_signal = []

        
        frame1_layout.addWidget(self.graphwidget_raw)
        
        frame2_layout.addRow('Cохраняемый файл', self.savepath_directory)
        frame2_layout.addWidget(self.button_save)
        frame2_layout.addWidget(self.button_discard)
        frame2_layout.addRow('Состояние записи:', self.system_state)
        frame2_layout.addRow('Наличие тревоги:', self.alarm_state)
        frame2_layout.addRow('Записано в файл:', self.count_records)
        frame2_layout.addRow(self.warning_label)
        
        
        frame3_layout.addRow(self.record_status, self.start_programm_button)
        frame3_layout.addRow('Время охлаждения, кол-во фреймов: ', self.input_cooling_time)
        frame3_layout.addRow('Длительность отступа, мс: ', self.input_indent_signal)
        frame3_layout.addRow('Порогвое значение отступа: ', self.input_detector_threshold)
        frame3_layout.addRow('Тип воздействия: ', self.input_interaction)
        frame3_layout.addRow('Окно СКО: ', self.input_std)
        
        frame4_layout.addWidget(self.graphwidget_filtered)

        # Связываем нажатие кнопки с выполнением действия
        self.start_programm_button.clicked.connect(self.start_timer)
        # Запускаем таймер для генерации данных
        self.timer = pg.QtCore.QTimer()
        self.timer.timeout.connect(self.get_data)
        self.is_timer_running = False
        
    def start_timer(self):
        if not self.is_timer_running:
            
            self.cropper = crop.DataStruct(detector_th = int(self.input_detector_threshold.text()),
                              mstd_th =  int(self.input_detector_threshold.text()),
                              mstd_wdw = int(self.input_std.text()) ,
                              indent = int(self.input_indent_signal.text()),
                              N_wait_frames = int(self.input_cooling_time.text()))
            
            
            
            self.timer.start(10)  # Запускаем таймер с интервалом 1000 мс (1 секунда)
            self.is_timer_running = True
            self.record_status.setText('Запись идёт')
            self.start_programm_button.setText('Стоп')
            self.start_programm_button.setStyleSheet("background-color: white; color: red")
        else:
            self.timer.stop()  # Останавливаем таймер
            self.count_records.setText(str(self.count_objects_in_directory()))
            self.is_timer_running = False
            self.record_status.setText('Запись остановлена')
            self.start_programm_button.setText('Старт')
            self.start_programm_button.setStyleSheet("background-color: white; color: green")
    def count_objects_in_directory(self):
        count = {}
        for file in os.listdir(self.savepath_directory.text()):
            filename = os.path.join(self.savepath_directory.text(), file)
            with h5py.File(filename, 'r') as hdf5_file:
                count[file.split('.')[0]] = len(hdf5_file)
        return count
        
        
        
        
        

    def save_or_discard_file(self, button_id):
        filename = os.path.join(self.savepath_directory.text(), self.input_interaction.text() + '.hdf5')
        with h5py.File(filename, mode = 'a') as self.hdf5_file: 
            if button_id == 'save':
                current_index = len(self.hdf5_file)
                
                self.hdf5_file.create_dataset(str(current_index), data = self.stored_signal)
                self.graphwidget_raw.clear()
                self.graphwidget_filtered.clear()
                self.stored_signal = []
                self.warning_label.setText('Сигнал записан')
                self.warning_label.setStyleSheet("color: green")
                #self.start_timer()
            
                # Здесь можно добавить соответствующее действие для кнопки 1
            
            elif button_id == 'discard':
                # skip saving
                self.graphwidget_raw.clear()
                self.graphwidget_filtered.clear()
                self.stored_signal = []
                self.warning_label.setText('Сигнал пропущен')
                self.warning_label.setStyleSheet("color: yellow")
                #self.start_timer()
            
    
    def get_data(self):
        buffer_data = sys.stdin.buffer.read(80000)
        self.current_data = sp.central_chl(np.frombuffer(buffer_data))
        
        self.stored_signal = self.cropper(self.current_data)
        if self.stored_signal is None:
            pass
        else:
            self.system_state.setText('Длина набранного сигнала: ' + str(len(self.stored_signal)))
            self.plot_graph()
        
        

    def plot_graph(self):
        self.graphwidget_raw.clear()
        self.graphwidget_raw.plot(self.stored_signal)
        self.graphwidget_filtered.clear()
        self.graphwidget_filtered.plot(sp.moving_std(self.stored_signal, int(self.input_std.text())))
        self.start_timer()
    
        

if __name__ == '__main__':
    
    app = QApplication(sys.argv)
    ex = ExampleApp()
    ex.show()
    sys.exit(app.exec_())
    
    
    
