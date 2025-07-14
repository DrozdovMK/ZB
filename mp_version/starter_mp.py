'''
Пример multiprocessing без exec
Это реализация с использованием методов fork или spawn (зависит от ОС)
В одной программе реализуются и главный и дочерние процессы
Разветвление вымолнения происходит в
 p = multiprocessing.Process(target=childProcess, args=(argv, nchn, q, qMain))
Главный процесс прололжает выполнение
Дочерний процесс после запуска сразу прыгает на метод
    childProcess(argv, nchn, qIn, qOut)
    - argv  - аргументы запуска из главного процесса
    - nchn  - номер канала для идентифмкации ответа
    - qIn   - персональная очередь входных данных от главного процесса
    - aOut  - общая для всех дочерних процессов очередь ответов
Для реализации ожидания ответов от дочерних процессов
в главном процессе запущен поток с методом, котому передана общая очередь
    mainQueueWaiting(q)
Пока не реализована остановка дочерних процессов, при завершении главного процесса    
'''
import multiprocessing
import threading
import time
import sys
import json
import random
from mainloop_mp import Mainloop
from receiver_mp import DataReceiver

#======================================
#child process
#======================================

def childProcess(argv, nchn, qIn, qOut):
    argv["zone_num"] = nchn
    argv["qOut"] = qOut
    classifier = Mainloop(**argv)
    while True:        
        mes = qIn.get()             #ждем входного сообщения
        classifier.receiver(mes)    #вызываем обработку

#======================================
#main process
#======================================
        

def mainQueueWaiting(q):
    while True:
        res = q.get()
        print(res)

# нужно сделать:
async def reflectogram_to_zones(queues, channels, receiver):
    for c, q in zip(channels, queues):
        receiver.wait()
        data = receiver.data_window[:, c]
        q.put(data)
        
def mainProcess():
    # считывание json с базовыми настройками
    congig_path = "classifier_config.json"
    with open(congig_path, "r") as config_file:
        argv_dict = json.load(config_file)
    print("start main", argv_dict)
    #список каналов
    channels        = [33, 44, 55]
    #список процессов потомков
    childProcesses  = []
    #список очередей потомков
    childQueues     = []
    #главная очередь ответов от всех потомков
    qMain   = multiprocessing.Queue()
    #запускаем поток ожидания сообщений от потомков из главной очереди
    t1 = threading.Thread(target=mainQueueWaiting, args=(qMain,))
    t1.start()
    #создаем список процессов потомков из списка каналов
    for nchn in channels:
        q = multiprocessing.Queue() #персональная входная очередь для дочернего процесса
        childQueues.append(q)  
        p = multiprocessing.Process(target=childProcess, args=(argv_dict, nchn, q, qMain))
        childProcesses.append(p)    
        p.start()
    
    #данные отправлчемые процессам потомкам
    dataSize = 1024
    receiver = DataReceiver(socket_path='/tmp/das_driver')
    while True:

        # Здесь нужно реализовать логику получения из receiver данных
        # (Внутри receiver содержится data_window, который представляет собой матрицу)
        # нужно взять значения channels
        time.sleep(1.024)
#======================================

if __name__ == "__main__":
    mainProcess()        