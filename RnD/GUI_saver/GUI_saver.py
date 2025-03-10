script_folder_demostend  = '../../scripts/' # for demostend
script_folder_local  = 'scripts/' # for laptop
import sys
sys.path.append(script_folder_demostend)
sys.path.append(script_folder_local)
import crop
import h5py
import os
import numpy as np
import pandas as pd
import pyqtgraph as pg
from PyQt5.QtWidgets import (QApplication, QMainWindow, QGridLayout, QWidget, QFrame,QFileDialog,QHBoxLayout,
                             QVBoxLayout, QPushButton, QLineEdit, QLabel, QFormLayout, QSizePolicy)
import signal_processing as sp


class saver(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('Datasets_saver')
        # self.setGeometry(400, 200, 900, 700)
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        grid_layout = QGridLayout()
        central_widget.setLayout(grid_layout)
        
        # Создаем четыре фрейма для каждой части окна
        frame1 = QFrame()
        frame1.setFrameShape(QFrame.StyledPanel)
        frame2 = QFrame()
        frame2.setFrameShape(QFrame.StyledPanel)
        
        
        # Размещаем фреймы в сетке
        grid_layout.addWidget(frame1, 0, 0)  # Позиция графика сырого сигнала
        grid_layout.addWidget(frame2, 0, 1)  # Позиция кнопок сохранения

        frame1_layout = QVBoxLayout() # Вертикальная компоновка 
        frame2_layout = QVBoxLayout() # Вертикальная компоновка 
        frame1.setLayout(frame1_layout)
        frame2.setLayout(frame2_layout)
        
        frame1.setFixedSize(700, 400)  # Фиксированный размер для первого фрейма

        frame2.setFixedSize(400, 400)  # Фиксированный размер для второго фрейма
        
        # Виджеты построения графиков
        self.graphwidget_raw = pg.PlotWidget(title = 'Сырой сигнал')
        self.graphwidget_raw.setYRange(0, 16536)
        
        # Сохранение файла и состояние системы
        
        self.savepath_directory = QLabel()
        self.savepath_directory_info = QLabel('Директория для сохранения датасета не выбрана')
        self.savepath_directory_info.setWordWrap(True)
        self.button_savepath_directory  = QPushButton("Выбрать директорию")
        self.button_savepath_directory.clicked.connect(self.open_directory_dialog) #Связь кноки с функцией
        self.savepath_layout = QVBoxLayout() # layout в формате "текст" что-то
        self.savepath_layout.addWidget(self.button_savepath_directory)
        self.savepath_layout.addWidget(self.savepath_directory_info)
        
        self.input_interaction_layout = QHBoxLayout()
        self.input_interaction = QLineEdit('hit')
        self.input_interaction_layout.addWidget(QLabel("Введите имя воздействия") )
        self.input_interaction_layout.addWidget(self.input_interaction)
        
        self.button_save = QPushButton("Сохранить сигнал")
        self.button_save.setStyleSheet("background-color: green; color: white")
        self.button_save.clicked.connect(lambda: self.save_or_discard_file('save'))
        self.button_discard = QPushButton("Пропустить")
        self.button_discard.clicked.connect(lambda: self.save_or_discard_file('discard'))
        self.button_discard.setStyleSheet("background-color: red; color: white")
        
        self.button_layout = QHBoxLayout() # Создаем горизонтальный layout
        self.button_layout.addWidget(self.button_save) # Добавляем кнопку сохранения файла
        self.button_layout.addWidget(self.button_discard) # Добавляем кнопку пропуска файла
        self.button_layout.setSpacing(0) # Убираем пробелы между кнопками
        
        
        
        self.alarm_state = QLabel('0')
        self.system_state = QLabel('0')
        
        self.warning_label = QLabel()
        self.count_records = QLabel()
        self.savepath_directory.setWordWrap(True)
        
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
        self.input_std = QLineEdit('32')
        self.std_window = int(self.input_std.text())
        self.current_data = None
        self.stored_signal = None

        self.parameters_layout = QFormLayout()
        # self.parameters_layout.setFrameShape(QFrame.StyledPanel)
        self.parameters_layout.addRow(self.record_status, self.start_programm_button)
        self.parameters_layout.addRow('Время охлаждения детектора, сек: ', self.input_cooling_time)
        # self.parameters_layout.addRow('Длительность отступа, мс: ', self.input_indent_signal)
        self.parameters_layout.addRow('Порогвое значение детектора: ', self.input_detector_threshold)
        self.parameters_layout.addRow('Длина набранного сигнала: ' , self.system_state)
        self.parameters_layout.addRow(self.warning_label)
        
        # self.parameters_layout.addRow('Окно СКО: ', self.input_std)
        
        frame1_layout.addWidget(self.graphwidget_raw)
        
        frame2_layout.addLayout(self.input_interaction_layout)
        frame2_layout.addLayout(self.savepath_layout)
        frame2_layout.addLayout(self.button_layout)
        frame2_layout.addLayout(self.parameters_layout)
        # frame2_layout.addRow('Состояние записи:', self.system_state)
        # frame2_layout.addRow('Наличие тревоги:', self.alarm_state)
        # frame2_layout.addRow('Записано в файл:', self.count_records)
        # frame2_layout.addRow(self.warning_label)
        
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
            self.is_timer_running = False
            self.record_status.setText('Запись остановлена')
            self.start_programm_button.setText('Старт')
            self.start_programm_button.setStyleSheet("background-color: white; color: green")
    def count_objects_in_directory(self):
        count = {}
        files_hdf5 = [file for file in os.listdir(self.savepath_directory.text()) if file.endswith('.hdf5')]
        for file in files_hdf5:
            filename = os.path.join(self.savepath_directory.text(), file)
            with h5py.File(filename, 'r') as hdf5_file:
                count[file.split('.')[0]] = len(hdf5_file)
        return count
        
        
        
    def open_directory_dialog(self):
        # Открываем диалог выбора директории
        directory = QFileDialog.getExistingDirectory(self, "Выберите директорию")
        if directory:
            # Отображаем выбранный путь
            self.savepath_directory.setText(directory)
            self.savepath_directory_info.setText(
                'Директория для сохранения файлов: \n{} \nСохранено: {}'.format(directory,str(self.count_objects_in_directory())))
        

    def save_or_discard_file(self, button_id):
        filename = os.path.join(self.savepath_directory.text(), self.input_interaction.text() + '.hdf5')
        if button_id == 'discard':
            self.graphwidget_raw.clear()
            self.stored_signal = None
            self.warning_label.setText('Сигнал пропущен')
            self.warning_label.setStyleSheet("color: yellow") 
        
        elif button_id == 'save':
            if self.savepath_directory.text() is None or self.savepath_directory.text() == '':
                    self.warning_label.setText('Директория не выбрана')
                    self.warning_label.setStyleSheet("color: red")
            elif self.stored_signal is None:
                    self.warning_label.setText('Сигнал еще не записался')
                    self.warning_label.setStyleSheet("color: red")
            else:
                with h5py.File(filename, mode = 'a') as self.hdf5_file:
                    current_index = len(self.hdf5_file)
                    self.hdf5_file.create_dataset(str(current_index), data = self.stored_signal)
                    self.graphwidget_raw.clear()
                    self.stored_signal = None
                    self.warning_label.setText('Сигнал записан')
                    self.warning_label.setStyleSheet("color: green")
                    self.savepath_directory_info.setText(
                'Директория для сохранения файлов: \n{} \nСохранено: {}'.format(
                self.savepath_directory.text(),
                str(self.count_objects_in_directory())))
            
    
    def get_data(self):
        buffer_data = sys.stdin.buffer.read(80000)
        self.current_data = sp.central_chl(np.frombuffer(buffer_data))
        
        self.stored_signal = self.cropper(self.current_data)
        if self.stored_signal is None:
            pass
        else:
            self.system_state.setText(str(len(self.stored_signal)))
            self.plot_graph()
        
        

    def plot_graph(self):
        self.graphwidget_raw.clear()
        self.graphwidget_raw.plot(self.stored_signal)
        
        self.start_timer()
    
        

if __name__ == '__main__':
    
    app = QApplication(sys.argv)
    ex = saver()
    ex.show()
    sys.exit(app.exec_())
    
    
    
