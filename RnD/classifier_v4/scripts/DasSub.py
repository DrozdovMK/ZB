import asyncio
import json
import numpy as np

'''
Класс локального подписчика сырых данных от драйвера DAS (издатель)
class DasSubscriberClass:
Конструтор:
    def __init__(self, subId, host, queue, port=None, hostState=None, portState=None):
	Конструтор аргументы:
		- subId		    идентификатор подписчика (обязательный)
		- host	        path/IP к local/TCP сокету издателя данных (обязательный)
		- queue		    выходная очередь сообщений (обязательный)
        - port          порт, только для TCP издателя данных (по умолчанию None (local))
		- hostState     path/IP к local/TCP сокету издателя состояний (по умолчанию None (нет издателя состояний))
        - portState     порт, только для TCP издателя состояний None (local))

	 Публичные методы
	 	start()		    выполняет в цикле установку соединения с издателями, 
                        при удачном соединении переходит на цикл приема данных и от издателей, 
                        при обрыве связи, снова переходит на цикл установки соединения
        dataHeader()    возвращает заголовок принятых данных в формате DICT (см. Структура JSON Header data)
Особенности:
    - допускается работа с сокетом данных и с сокетом состояний 
	- используются Unix Domain Socket или TCP/IP Socket по выбору
    - выбор сокетов определяется конструктором
        - аргумент port указан      - TCP/IP Socket
        - аргумент port не указан   - Unix Domain Socket (по умолчанию)
	- пока реализован только прием, но в классе предусмотрена возможность отправки сообщений драйверу для дальнейших реализаций
	- в пользовательсой программе допускается использование нескольких эклемпляров подписчиков с разними идентификаторвми
	- взаимодествие с программой выполняется через очередь сообщений формате словаря (DICT)
    - собщения состояния передаются при изменении состояния
	- асинхронное исполнение с использование asyncio
    - реализована защита от повторного старта подписчика
Формат данных, принимаемых от драйвера:
	JSON Header		- в виде байтовой строки
	\0 				- нуль терминтор
	Byte array 		- байтовый массив пачки рефлектограмм, формат разметки данных в JSON Header
	Delimiter		- разделитель сообщений формата bytearray(b'<<<EndOfData.>>>')
Структура JSON Header data:
{
	"ADC_id": 		0,      #номер АЦП устройства
    "chnTypeId": 	"t",    #формат данных канала t-uint16
 	"correctOrder":	True,   #endian True-Litle(intel format), False-Big
    "devName": 		"DAS",  #наименование устройства
    "impulseNs": 	80,     #длительность импульса лазера в наноСек
    "meterOnChn": 	2,      #количество метров на один канал дальности
    "numTraces": 	1024,   #количество рефлектограмм в сообщении
    "scaleMax": 	16383,  #максимальное значение шкалы прибора
    "scaleMin": 	0,      #минимальное значение шкалы прибора
    "scanMs": 		1,      #период сканирующих импульсов в милиСек
    "traceBegin": 	0,      #длина транспортной части(пропуск) в каналах дальности
    "traceSize": 	1200    #длина одной рефлектограммы в каналах дальности
}

Формат состояний, принимаемых от драйвера:
{
 "ERROR": {
   "error_string": "no errors",
   "errno": 0   
 }
}
\n

ВНИМАНИЕ!!!
    - время жизни 'data' в сообщении 'dataready'    - до прихода нового сообщения от устройства
    - время жизни данных в методе dataHeader()      - до прихода нового сообщения от устройства

Сообщения выходной очереди в формате словря (DICT)
    Сообщения от задачи данных
	- сообщение о приеме данных, тип данных в data_np -  numTraces * traceSize * uint16
        {'subid':self.subId, 'type':'dataready', 'task':'data', 'numTraces':numTraces, 'traceSize':traceSize, 'data':data_np}
	- сообщении при удачном соединении с издателем данных, переход к приемну данных
        {'subid':self.subId, 'type':'connect', 'task':'data', 'host':self.host, 'port':self.port}
	- сообщение при неудачном соединении с издателем, выдается циклически 1 раз в секуду до установки соединения 
        {'subid':self.subId, 'type':'connection error', 'task':'state', 'host':self.hostState, 'port':self.portState}
	- сообщение о разрыве соединения с издателем данных
        {'subid':self.subId, 'type':'disconnect', 'task':'data', 'host':self.host, 'port':self.port}
	- сообщение о попытке повторного старта подпмсчиков
        {'subid':self.subId, 'type':'error', 'task':'data', 'errstring':'already started'}
    Сообщения от задачи состояний
	- сообщении при удачном соединении с издателем состояний, переход к приему состояний
        {'subid':self.subId, 'type':'connect', 'task':'state', 'host':self.hostState, 'port':self.portState}
	- сообщение при неудачном соединении с издателем состояний, выдается циклически 1 раз в секуду до установки соединения 
        {'subid':self.subId, 'type':'connection error', 'task':'state', 'host':self.hostState, 'port':self.portState}
	- сообщение об изменении состояния устройства
        {'subid':self.subId, 'type':'state changed', 'task':'state', 'errno':errno, 'errstring':error_string}
	- сообщение о разрыве соединения с издателем состояний, переход к установке соединения
        {'subid':self.subId, 'type':'disconnect', 'task':'state', 'host':self.hostState, 'port':self.portState}

Пример использования:

from DasSub import DasSubscriberClass

async def main2():
	# создаем выходную очередь для подисчика
    queue	= asyncio.Queue()
    # создаем экземпляр подписчика
        #local Unix Domain Socket
    sub0 	= DasSubscriberClass(0, '/tmp/das_driver', queue)
    #sub0 	= DasSubscriberClass(0, '/tmp/das_driver', queue, hostState='/tmp/das_errors')
        #tcp/ip
    #sub0 	= DasSubscriberClass(0, '127.0.0.1', queue, port=6000)
    #sub0 	= DasSubscriberClass(0, '127.0.0.1', queue, port=6000, hostState='127.0.0.1', portState=6001)
    # запускам подписчик
    sub0.start()
    # в цикле ожидаем приема соббщений от подписчиков
    while True:
        msg = await queue.get()
        print(msg)
        if(msg['type'] == 'dataready'): print(sub0.dataHeader())
#вызываем корневую сопрограмму
if __name__ == "__main__":
    print('DasSub2.py')
    asyncio.run(main2())
'''

#Класс локального подписчика сырых данных и состояний от издателя DAS

class DasSubscriberClass:
#public
    def __init__(self, subId, host, queue, port=None, hostState=None, portState=None):
        self.subId	        = subId
        self.queue 	        = queue
        self.tcpTimeout     = 3
        #data task
        self.host 	        = host
        self.port           = port
        self.delimiter      = bytearray(b'<<<EndOfData.>>>')
        self.task           = None
        self.dataHdr        = None
        #state task
        self.taskState      = None
        self.hostState 	    = hostState
        self.portState      = portState
        self.errno          = -1000
        self.error_string   = ''
    def start(self):
        #task data
        if(self.task != None):
            self.queue.put_nowait({'subid':self.subId, 'type':'error', 'task':'data', 'errstring':'already started'})
            return False
        self.task = asyncio.create_task(self.__start())
        #task state
        if(self.hostState != None):
            self.taskState  = asyncio.create_task(self.__startState())
        return True;
    def dataHeader(self):
        return self.dataHdr

#private task state
    async def __startState(self):
        while True:
            await self.__connectState()     #return при установке соединения (delay 1 sec)
            await self.__receiverState()    #return при обрыве соединения
    async def __connectState(self):
        while True:
            try:
                if(self.portState == None): #local
                    self.readerState, self.writerState = await asyncio.open_unix_connection(self.hostState)
                else :                      #tcp
                    self.readerState, self.writerState = await asyncio.wait_for(asyncio.open_connection(self.hostState, self.portState), self.tcpTimeout)
                self.queue.put_nowait({'subid':self.subId, 'type':'connect', 'task':'state', 'host':self.hostState, 'port':self.portState})
                break
            except Exception as err:
                self.queue.put_nowait({'subid':self.subId, 'type':'connection error', 'task':'state', 'host':self.hostState, 'port':self.portState, 'errstring':err})
                await asyncio.sleep(1)
        return True
    async def __receiverState(self):
        while True:
            try:
                jstr    = await self.readerState.readline()
                jobj    = json.loads(jstr[:-1].decode())
                err     = jobj['ERROR']
                error_string = err['error_string']
                errno   = err['errno']
                #queue put only change
                if((errno != self.errno) or (error_string != self.error_string)):
                    self.errno = errno
                    self.error_string = error_string
                    self.queue.put_nowait({'subid':self.subId, 'type':'state changed', 'task':'state', 'errno':errno, 'errstring':error_string})
            except Exception as err:
                self.queue.put_nowait({'subid':self.subId, 'type':'disconnect', 'task':'state', 'host':self.hostState, 'port':self.portState})
                break
        return False
#private task data
    async def __start(self):
        while True:
            await self.__connect()      #return при установке соединения (delay 1 sec)
            await self.__receiver()     #return при обрыве соединения
    async def __connect(self):
        while True:
            try:
                if(self.port == None):  #local
                    self.reader, self.writer = await asyncio.open_unix_connection(self.host)
                else :                  #tcp
                    self.reader, self.writer = await asyncio.wait_for(asyncio.open_connection(self.host, self.port), self.tcpTimeout)
                self.queue.put_nowait({'subid':self.subId, 'type':'connect', 'task':'data', 'host':self.host, 'port':self.port})
                break
            except Exception as err:
                self.queue.put_nowait({'subid':self.subId, 'type':'connection error', 'task':'data', 'host':self.host, 'port':self.port, 'errstring':err})
                await asyncio.sleep(1)
        return True
    async def __receiver(self):
        buffer = bytearray()
        while True:
            try:
                #читаем все до разделителя
                data = await self.reader.readuntil(separator=self.delimiter)
                buffer          += data
                #json заголовок
                delimiterJSON   = buffer.index(b'\0')
                jsonHdrBytes    = buffer[:delimiterJSON]
                self.dataHdr    = json.loads(jsonHdrBytes.decode())
                #разметка пачки рефлектограмм
                numTraces       = self.dataHdr['numTraces']
                traceSize       = self.dataHdr['traceSize']
                #получаем байтовый массив данных
                byteData        = buffer[delimiterJSON + 1: delimiterJSON + numTraces * traceSize * 2 + 1]
                #преобразуем байтовый массив в двумерный numpy массив
                data_np         = np.frombuffer(byteData, dtype=np.uint16).reshape(numTraces, traceSize)
                #очищвем буфер
                buffer.clear()
                self.queue.put_nowait({'subid':self.subId, 'type':'dataready', 'task':'data', 'numTraces':numTraces, 'traceSize':traceSize, 'data':data_np})
            #не найден разделитель, превышен лимит, просто все считывам
            except asyncio.exceptions.LimitOverrunError as e:
                buffer += await self.reader.read(e.consumed)
            #рвзрыв соединения, выход с ошибкой
            except asyncio.exceptions.IncompleteReadError as err:
                buffer.clear()
                self.queue.put_nowait({'subid':self.subId, 'type':'disconnect', 'task':'data', 'host':self.host, 'port':self.port})
                break
        return False        
            
#example
async def main2():
    queue	= asyncio.Queue()
        #local
    sub0 	= DasSubscriberClass(0, '/tmp/das_driver', queue)
    #sub0 	= DasSubscriberClass(0, '/tmp/das_driver', queue, hostState='/tmp/das_errors')
        #tcp/ip
    #sub0 	= DasSubscriberClass(0, '127.0.0.1', queue, port=6000)
    #sub0 	= DasSubscriberClass(0, '127.0.0.1', queue, port=6000, hostState='127.0.0.1', portState=6001)
        #Алексей tcp/ip 
    #sub0 	= DasSubscriberClass(0, '10.77.171.81', queue, port=6000)
    #sub0 	= DasSubscriberClass(0, '10.77.171.81', queue, port=6000, hostState='127.0.0.1', portState=6001)

    #sub0 	= DasSubscriberClass(0, '10.77.171.82', queue, port=6000)

    sub0.start()

    while True:
        msg = await queue.get()
        print(msg)
        if(msg['type'] == 'dataready'): print(sub0.dataHeader())

#вызываем корневую сопрограмму
if __name__ == "__main__":
    print('DasSub2.py')
    asyncio.run(main2())

#проба
    #array = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]])
    #columns = array[:, 2]
    #print(columns)
    #A = np.zeros((rows, cols))








