import sys
import h5py
import json
import os
from PyQt5.QtWidgets import (QApplication, QMainWindow, QCalendarWidget,
                             QMessageBox, QTableWidget, QDialog,
                             QListWidget,QVBoxLayout, QHBoxLayout,
                             QFileDialog, QPushButton, QLabel,
                             QWidget, QTableWidgetItem, QTextEdit,
                             QCheckBox, QButtonGroup, QSizePolicy)
# from PyQt5.QtCore import QDate
from PyQt5 import QtCore
from PyQt5.QtCore import QDate

from PyQt5.QtGui import (QPainter, QColor, QBrush,
                         QPen, QTextCharFormat, QFont)
import numpy as np

from matplotlib.backends.backend_qt5agg import (FigureCanvasQTAgg as FigureCanvas,
                                                NavigationToolbar2QT as NavigationToolbar)

import matplotlib.pyplot as plt


def get_non_empty_directories(path):
    non_empty_dirs = []
    for item in os.listdir(path):
        full_path = os.path.join(path, item)
        if os.path.isdir(full_path) and os.listdir(full_path):
            non_empty_dirs.append(item)
    return non_empty_dirs

class CustomCalendarWidget(QCalendarWidget):
    def __init__(self, parent = None):
        super().__init__()
        self.setStyleSheet("""
            QCalendarWidget QAbstractItemView:focus {
                background: none;
            }
            QCalendarWidget QAbstractItemView {
                alternate-background-color: transparent;
                selection-background-color: transparent;
                selection-color: black;
            }
        """)
        self.parent = parent
        self.setGridVisible(True)
        self.clicked.connect(self.showDate)
        self.clicked.connect(self.on_calendar_cell_clicked)
        self.clicked.connect(self.parent.table.updateTable)

        self.colors_data = {}
        
    def paintCell(self, painter: QPainter, rect, date):
        super().paintCell(painter, rect, date)
        date_string = date.toString('yyyy_MM_dd')
        if date_string in self.colors_data:
            alpha = max(10, min(self.colors_data[date_string], 255))
            # Задаем цвет и заполняем ячейку
            color = QColor(255, 0, 0, alpha)  # Красный с заданной интенсивностью
            pen = QPen(QColor(255, 0, 0,), 2)
            painter.setPen(pen) 
            painter.fillRect(rect, QBrush(color))
            painter.drawRect(rect)
        # Устанавливаем цвет текста
        painter.setPen(QColor(0, 0, 0))  # Черный цвет текста
        text = str(date.day())
        painter.drawText(rect, QtCore.Qt.AlignCenter, text)
    
    def updateCalendar(self):
        self.colors_data = self.parent.count_of_alarms_for_calendar
        self.update() # Перерисовывает виджет для обновления цвета ячеек
    
    def showDate(self, date):
        self.date = date.toString("yyyy_MM_dd")
        if self.parent.selected_zone is not None:
            if self.date not in self.parent.count_of_alarms_for_calendar.keys():
                self.parent.date_status.setText(f"{self.date} обнаружено 0 тревог")
            else:
                self.parent.date_status.setText("{} обнаружено {} тревог".format(
                    self.date,
                    self.parent.count_of_alarms_for_calendar[self.date]
                ))
    
    def on_calendar_cell_clicked(self, date):
        self.date = date.toString("yyyy_MM_dd")
        if (self.parent.directory is None) or (self.parent.selected_zone is None):
            self.parent.date_status.setText("Не выбрана директория или зона")
            self.parent.date_status.setStyleSheet("color: red")
        else:
            path = os.path.join(
                self.parent.directory,
                self.parent.selected_zone,
                self.date
            )
            if os.path.exists(path):
                self.parent.table.current_date_index = self.parent.table.date_of_alarms.index(self.date)
                self.parent.table.current_hour_index = 0
                self.parent.table.current_alarm_index = 0

                self.parent.table.transfer_to_plot_alarm()
                    
class CustomTableWidget(QTableWidget):
    def __init__(self, parent = None):
        super().__init__()
        self.setStyleSheet("""
            QTableWidget::item {
                background: transparent;
                color: black; 
            }
            QTableWidget::item:selected {
                background: transparent;
                color: black;
            }
        """)
        self.parent = parent
        self.cellClicked.connect(self.table_cell_clicked)
        self.current_date_index = 0
        self.current_hour_index = 0
        self.current_alarm_index = 0
        self.path_result = None
    
    def updateTable(self, date):
        self.parent.calendar.date = date.toString("yyyy_MM_dd")
        if (self.parent.directory is None) or (self.parent.selected_zone is None):
            self.parent.date_status.setText("Не выбрана директория или зона")
            self.parent.date_status.setStyleSheet("color: red")
        else:
            path_to_alarms = os.path.join(self.parent.directory,
                                          self.parent.selected_zone,
                                          self.parent.calendar.date)
            self.setColumnCount(2)
            self.setHorizontalHeaderLabels(["Час тревоги", "Кол-во тревог"])
            if os.path.exists(path_to_alarms):
                self.hour_of_alarms = sorted(get_non_empty_directories(path_to_alarms))
                self.setRowCount(len(self.hour_of_alarms))
                for i in range(len(self.hour_of_alarms)):
                    item_1 =  QTableWidgetItem(self.hour_of_alarms[i])
                    item_2 = QTableWidgetItem(
                        str(self.parent.count_of_alarms[self.parent.calendar.date][self.hour_of_alarms[i]]) 
                        )
                    item_1.setFlags(item_1.flags() & ~QtCore.Qt.ItemIsEditable)
                    item_2.setFlags(item_2.flags() & ~QtCore.Qt.ItemIsEditable)
                    self.setItem(i, 0, item_1)
                    self.setItem(i, 1, item_2)
            else:
                self.setRowCount(1)
                self.setColumnCount(2)
                item_1 = QTableWidgetItem("-")
                item_2 = QTableWidgetItem("-")
                item_1.setFlags(item_1.flags() & ~QtCore.Qt.ItemIsEditable)
                item_2.setFlags(item_2.flags() & ~QtCore.Qt.ItemIsEditable)
                self.setItem(0, 0, item_1)
                self.setItem(0, 1, item_2)
    def table_cell_clicked(self, row, column):
        item = self.item(row, column)
        filename = "{}_{}_{}_00_00_000.hdf5".format(self.parent.selected_zone,
                                          self.parent.calendar.date,
                                          item.text()[:2])
        if item.column() == 0:
            path = os.path.join(
                self.parent.directory,
                self.parent.selected_zone,
                self.parent.calendar.date,
                item.text(),
                filename
                )
            self.current_group_path = path
            if os.path.exists(path):
                self.current_date_index = self.date_of_alarms.index(self.parent.calendar.date)
                self.current_hour_index = self.hours_list.index(item.text())
                self.current_alarm_index = 0
            
                self.transfer_to_plot_alarm()
        
    def transfer_to_plot_alarm(self):
        path_to_zone = os.path.join(
                                    self.parent.directory,
                                    self.parent.selected_zone
                                    )
        self.date_of_alarms = sorted(get_non_empty_directories(path_to_zone))
        path_to_date = os.path.join(
                                    path_to_zone,
                                    self.date_of_alarms[self.current_date_index],
                                    )
        self.hours_list = sorted(get_non_empty_directories(path_to_date))
        if len(self.hours_list) != 0:
            filename = "{}_{}_{}_00_00_000.{}".format(
                self.parent.selected_zone,
                self.date_of_alarms[self.current_date_index],
                self.hours_list[self.current_hour_index][:2],
                "hdf5"
            )
            self.path_result = os.path.join(
                                        path_to_date,
                                        self.hours_list[self.current_hour_index],
                                        filename
                                    )
            with h5py.File(self.path_result, "r") as file:
                self.current_group_alarm_names  = sorted(list(file.keys()))
                self.parent.plot_main.plot(file,
                                        self.current_group_alarm_names[self.current_alarm_index])
                
            y, m, d = list(map(int, self.date_of_alarms[self.current_date_index].split("_")))
            date_to_higlight = QDate(y, m, d)
            self.parent.table.updateTable(date_to_higlight)
            
            format_ceil = QTextCharFormat()
            format_ceil.setFontWeight(QFont.Bold)  # Устанавливаем жирный шрифт
            self.parent.calendar.setDateTextFormat(date_to_higlight, format_ceil)
            self.parent.checkbox_group.setExclusive(False)
            for button in self.parent.checkbox_group.buttons():
                button.setChecked(False)  # Снимаем галочку
            self.parent.checkbox_group.setExclusive(True)

class OptionsDialogWidget(QDialog):
    def __init__(self, zones):
        super().__init__()
        self.setWindowTitle("Выберите зону")
        self.setGeometry(400, 300, 500, 200)
    
        self.list_widget = QListWidget()
        self.list_widget.addItems(zones)
        self.list_widget.doubleClicked.connect(self.select_zone)
        
        layout = QVBoxLayout()
        layout.addWidget(self.list_widget)
        self.setLayout(layout)
        
    def select_zone(self, index):
        zone = self.list_widget.item(index.row()).text()
        self.selected_zone = zone
        self.accept()  # Закрываем диалог

class PlotCanvasWidget(FigureCanvas):
    def __init__(self, parent=None, width=5, height=4, dpi=100):
        self.fig, self.ax = plt.subplots(1, 2, figsize=(width, height), dpi=dpi)
        super().__init__(self.fig)
        self.parent = parent
        
        self.ax[0].set_title('Сырой сигнал с интерферометра')
        self.ax[0].set_xlabel('Время, мс')
        self.ax[0].set_ylabel('Амплитуда, отн. ед.')
        self.ax[0].set_ylim([0, 16384])
        self.ax[1].set_xlabel('Классы')
        self.ax[1].set_title('Распределение вероятностей \n классификатора')
        self.ax[1].set_ylim([0, 1])
        self.draw()

    def plot(self, file, current_index: str):
        dataset = file[current_index]
        data = np.array(dataset)
        try:
            probs_dict = json.loads(
                json.loads(dataset.attrs["probabilities"])
                )
        except:
            probs_dict = json.loads(dataset.attrs["probabilities"])
        datetime =  dataset.attrs["date_time"]
        
        title = current_index + "\n" + "Дата, время:" + datetime
        self.ax[0].clear()
        self.ax[0].plot(data)
        self.ax[0].set_title(title)
        self.ax[0].set_xlabel('Время, мс')
        self.ax[0].set_ylabel('Амплитуда, отн. ед.')
        self.ax[0].set_ylim([0, 16384])
        
        if "label" not in dataset.attrs:
            self.parent.markup_label.setText("Истинный класс: - ")
        else:
            self.parent.markup_label.setText(f"Истинный класс:{dataset.attrs["label"]}")
            
        self.ax[1].clear()
        self.ax[1].bar(probs_dict.keys(), probs_dict.values(), alpha = 1, edgecolor='black')
        self.ax[1].set_xlabel("Классы")
        self.ax[1].set_title("Распределение вероятностей по классам")
        self.ax[1].set_ylim([0, 1])

        self.draw()
    
class LabelsTextWidget(QWidget):
    def __init__(self, parent = None):
        super().__init__()
        
        self.parent = parent
        self.setWindowTitle("Классы для разметки")
        self.setGeometry(100, 100, 400, 300)
        layout = QVBoxLayout()
        self.text_edit = QTextEdit(self)
        self.text_edit.setPlaceholderText("Введите типы воздействий через Enter...")
        self.submit_button = QPushButton("Выбрать классы", self)
        self.submit_button.clicked.connect(self.change_labels)
        layout.addWidget(self.text_edit)
        layout.addWidget(self.submit_button)
        self.setLayout(layout) 
    def change_labels(self):
        text = self.text_edit.toPlainText()
        lines = text.strip().splitlines()
        labels_list = [line for line in lines]
        self.parent.markup_labels = labels_list
        self.update_checkboxes()
        self.close()
        
    def clear_layout(self, layout):
        if layout is not None:
            while layout.count() > 0:
                item = layout.itemAt(0)
                widget = item.widget()
                if widget is not None:
                    widget.deleteLater()
                layout.removeItem(item)

    def clear_checkboxes(self, button_group):
        if button_group is not None:
            for checkbox in button_group.buttons():
                button_group.removeButton(checkbox)
                checkbox.deleteLater()
            
    def update_checkboxes(self):
        self.clear_layout(self.parent.graph_layout_checkboxes_layout)
        self.clear_checkboxes(self.parent.checkbox_group)
        for i, markup_label in enumerate(self.parent.markup_labels):
            checkbox = QCheckBox(markup_label)
            self.parent.graph_layout_checkboxes_layout.addWidget(checkbox)
            self.parent.checkbox_group.addButton(checkbox, id = i)
        self.parent.checkbox_group.buttonClicked[int].connect(self.set_label)
    def set_label(self, id):
        if self.parent.table.path_result is not None:
            with h5py.File(self.parent.table.path_result, "a") as file:
                alarm_names = self.parent.table.current_group_alarm_names
                index = self.parent.table.current_alarm_index
                file[alarm_names[index]].attrs["label"] = self.parent.markup_labels[id]
                self.parent.markup_label.setText(f"Истинный класс: {self.parent.markup_labels[id]}")

class HDF5Viewer(QMainWindow):
    def __init__(self):
        super().__init__()
        
        self.setWindowTitle('Аналитика тревог ZB')
        # Graph widgets, navigation and some buttons 
        
        self.plot_main = PlotCanvasWidget(self, width=10, height=5)
        self.toolbar = NavigationToolbar(self.plot_main, self)
        self.next_button = QPushButton("Перейти к следующей тревоге")
        self.next_button.setEnabled(False)
        self.previous_button = QPushButton("Перейти к предыдущей тревоге")
        self.previous_button.setEnabled(False)
        
        self.button_for_class_labels = QPushButton("Изменить список тревог для разметки")
        self.button_for_class_labels.clicked.connect(self.change_class_labels)
        self.button_for_class_labels.setEnabled(False)
        self.markup_label = QLabel("Истинный класс: -")
        
        
        # Create navigation buttons
        self.table = CustomTableWidget(parent = self)
        self.calendar = CustomCalendarWidget(parent = self)
        
        self.file_select_button = QPushButton("Выбрать директорию")
        self.file_select_button.setStyleSheet("background-color: green; color: white")
        self.file_status = QLabel("Директория: не выбрана")
        self.directory = None
        self.date_status = QLabel("Дата: Выберите дату в календаре")
        self.file_select_button.clicked.connect(self.load_dataset)
        
        self.zone_select_text = QLabel("Зона не выбрана")
        self.zone_select_button = QPushButton("Выбрать зону")
        self.selected_zone = None
        self.zone_select_button.clicked.connect(self.show_zone_options)
        self.zone_select_button.setEnabled(False)
        self.whole_count_of_alarms = QLabel("Общее количество тревог в зоне: ...")
        self.table.current_date_index = None
        
        # Layout management
        self.layout = QHBoxLayout()
        self.graph_layout = QVBoxLayout()
        self.graph_layout.addWidget(self.toolbar)
        self.graph_layout.addWidget(self.plot_main)
        
        self.graph_layout_buttons = QHBoxLayout()
        self.graph_layout_buttons.addWidget(self.previous_button)
        self.graph_layout_buttons.addWidget(self.next_button)
    
        
        self.next_button.clicked.connect(lambda: self.change_alarm(flag = "next"))
        self.previous_button.clicked.connect(lambda: self.change_alarm(flag = "prev"))
    
        self.graph_layout_checkboxes_layout = QHBoxLayout()
        self.checkbox_group = QButtonGroup()
        
        self.graph_layout.addWidget(self.markup_label)
        self.graph_layout.addLayout(self.graph_layout_checkboxes_layout)
        self.graph_layout.addLayout(self.graph_layout_buttons)
        self.graph_layout.addWidget(self.button_for_class_labels)

        self.layout.addLayout(self.graph_layout)
        
        self.setup_layout = QVBoxLayout()
        self.setup_layout.addWidget(self.calendar)
        self.setup_layout.addWidget(self.table)
        self.setup_layout.addWidget(self.file_select_button)
        self.setup_layout.addWidget(self.file_status)
        self.setup_layout.addWidget(self.date_status)
        
        self.zone_select_layout = QVBoxLayout()
        self.zone_select_button_text_layout = QHBoxLayout()
        self.zone_select_button_text_layout.addWidget(self.zone_select_button)
        self.zone_select_button_text_layout.addWidget(self.zone_select_text)
        self.zone_select_layout.addLayout(self.zone_select_button_text_layout)
        self.zone_select_layout.addWidget(self.whole_count_of_alarms)
        self.setup_layout.addLayout(self.zone_select_layout)
        
        self.layout.addLayout(self.setup_layout)

        container = QWidget()
        container.setLayout(self.layout)
        self.setCentralWidget(container)
        
        self.show()

    def change_class_labels(self):
        self.text_input_widget = LabelsTextWidget(parent=self)
        self.text_input_widget.show()
        pass
    def add_checkboxes(self):
        # Получаем текст из текстового редактирования и разбиваем его на строки
        for i in reversed(range(self.parent.group_layout.count())): 
            self.parent.group_layout.itemAt(i).widget().setParent(None)
        texts = self.text_edit.toPlainText().split('\n')
        for text in texts:
            text = text.strip()
            if text:  # Проверяем, что строка не пустая
                self.parent.add_checkbox(text)
        self.close()

       
    def change_alarm(self, flag = "next"):
        """
        Реализуется логика, позволяющая при помощи кнопок
        под графиком переключаться между тревогами.
        """
        if self.table.current_date_index is None:
            self.table.current_alarm_index = 0
            self.table.current_hour_index = 0
            self.table.current_date_index = 0
            self.table.transfer_to_plot_alarm()
        else:    
            if flag == "next":
                step = 1
            elif flag == "prev":
                step = -1
            self.table.current_alarm_index += step
            
            self.date_list = list(self.count_of_alarms.keys()) # Все даты нарушений
            # Обработка выхода за границу при пролистывании вперед 
            if self.table.current_alarm_index >= len(self.table.current_group_alarm_names):
                self.table.current_alarm_index = 0
                self.table.current_hour_index += step
                if self.table.current_hour_index >= len(self.table.hours_list):
                    self.table.current_hour_index = 0
                    self.table.current_alarm_index = 0
                    self.table.current_date_index += step
                    if self.table.current_date_index >= len(self.table.date_of_alarms):
                        self.table.current_hour_index = 0
                        self.table.current_alarm_index = 0
                        self.table.current_date_index = 0
            # Обработка выхода за границу при пролистывании назад
            elif self.table.current_alarm_index < 0:
                self.table.current_hour_index += step
                if self.table.current_hour_index < 0:
                    self.table.current_date_index += step
                    if self.table.current_date_index < 0:
                        self.table.current_date_index = len(self.date_list) - 1
                        self.hours_list = list(self.count_of_alarms[
                            self.date_list[self.table.current_date_index]
                        ].keys())
                        self.table.current_hour_index = len(self.hours_list) - 1
                        self.table.current_alarm_index = self.count_of_alarms[
                            self.date_list[self.table.current_date_index]
                        ][
                            self.hours_list[self.table.current_hour_index]
                        ] - 1
                    
                    elif self.table.current_date_index >= 0:
                        self.hours_list = list(self.count_of_alarms[
                            self.date_list[self.table.current_date_index]
                        ].keys())
                        
                        self.table.current_hour_index = len(self.hours_list) - 1
                        
                        self.table.current_alarm_index = self.count_of_alarms[
                            self.date_list[self.table.current_date_index]
                        ][
                            self.hours_list[self.table.current_hour_index]
                        ] - 1
                    
                    
                elif self.table.current_hour_index >= 0:
                    self.hours_list = list(self.count_of_alarms[
                            self.date_list[self.table.current_date_index]
                        ].keys())
                    self.table.current_alarm_index = self.count_of_alarms[
                            self.date_list[self.table.current_date_index]
                        ][
                            self.hours_list[self.table.current_hour_index]
                        ] - 1

            self.table.transfer_to_plot_alarm()
            
        
    def load_dataset(self):
        self.directory = str(QFileDialog.getExistingDirectory(self, "Выберите директорию"))
        marker_path = os.path.join(self.directory, "marker.txt")
        if not os.path.isfile(marker_path):
            message = "Выбранная неправильная директория"
            self.file_status.setText(message)
            self.file_status.setStyleSheet("color: red")
        else:
            message = "Директория успешно загружена"
            self.file_status.setText(message)
            self.file_status.setStyleSheet("color: green")
            self.file_select_button.setStyleSheet("background-color: white; color: black")
            self.zones = [d for d in os.listdir(self.directory) 
                          if os.path.isdir(os.path.join(self.directory, d))]
            self.zone_select_button.setEnabled(True)
            self.zone_select_button.setStyleSheet("background-color: green; color: white")
    
    def show_zone_options(self):
        dialog = OptionsDialogWidget(self.zones)
        if dialog.exec_():
            self.selected_zone = dialog.selected_zone
            self.zone_select_text.setText(f"Выбранная зона: {self.selected_zone}")
            self.zone_select_button.setStyleSheet("background-color: white; color: black")
            self.table.date_of_alarms = sorted(
                get_non_empty_directories(os.path.join(self.directory, self.selected_zone)
                ))
            
            self.count_of_alarms = {
                date: {
                    hour : 0
                    for hour in sorted(get_non_empty_directories(os.path.join(
                        self.directory, self.selected_zone, date)))
                }
                for date in self.table.date_of_alarms
            }
            
            for date in self.table.date_of_alarms:
                path_to_date = os.path.join(self.directory, self.selected_zone, date)
                hours = sorted(os.listdir(path_to_date))
                for hour in hours:
                    filename = "{}_{}_{}_00_00_000.{}".format(self.selected_zone,
                                          date,
                                          hour[:2],
                                          "hdf5")
                    path_to_date_hour = os.path.join(self.directory, self.selected_zone,
                                                    date, hour, filename)
                    if os.path.exists(path_to_date_hour):
                        with h5py.File(path_to_date_hour, "r") as alarm_file:
                            self.count_of_alarms[date][hour] += len(alarm_file) 
            
            self.count_of_alarms_for_calendar = {
                date: sum(info_per_house.values())
                for date, info_per_house in  self.count_of_alarms.items()
                }
            self.whole_count_of_alarms.setText(
                "Общее количество тревог в зоне: " + 
                str(sum(self.count_of_alarms_for_calendar.values())))
            self.previous_button.setEnabled(True)
            self.next_button.setEnabled(True)
            self.button_for_class_labels.setEnabled(True)
            self.calendar.updateCalendar()
            

if __name__ == '__main__':
    app = QApplication(sys.argv)
    app.setFont(QFont("Arial", 12))
    
    viewer = HDF5Viewer()
    sys.exit(app.exec_())