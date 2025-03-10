import sys
from PyQt5.QtWidgets import QApplication, QWidget, QVBoxLayout, QRadioButton, QPushButton, QButtonGroup

class MyWidget(QWidget):
    def __init__(self):
        super().__init__()

        self.initUI()

    def initUI(self):
        layout = QVBoxLayout()

        # Создаем QButtonGroup
        self.button_group = QButtonGroup(self)

        # Создаем радио-кнопки и добавляем их в группу
        self.radio1 = QRadioButton('Option 1')
        self.radio2 = QRadioButton('Option 2')
        self.radio3 = QRadioButton('Option 3')

        # Добавляем кнопки в группу
        self.button_group.addButton(self.radio1)
        self.button_group.addButton(self.radio2)
        self.button_group.addButton(self.radio3)

        # Добавляем радио-кнопки на макет
        layout.addWidget(self.radio1)
        layout.addWidget(self.radio2)
        layout.addWidget(self.radio3)

        # Создаем кнопку для вызова функции
        self.clear_button = QPushButton('Снять выбор')
        self.clear_button.clicked.connect(self.clear_selection)
        
        layout.addWidget(self.clear_button)

        self.setLayout(layout)
        self.setWindowTitle('QButtonGroup Example')
        self.show()

    def clear_selection(self):
        # Снимаем выбор с всех кнопок
        self.button_group.setExclusive(False)  # Позволяем снимать выделение с нескольких кнопок
        for button in self.button_group.buttons():
            button.setChecked(False)  # Снимаем галочку
        
        self.button_group.setExclusive(True)  # Включаем эксклюзивный режим снова, если это нужно

if __name__ == "__main__":
    app = QApplication(sys.argv)
    ex = MyWidget()
    sys.exit(app.exec_())