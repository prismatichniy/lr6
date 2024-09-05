import re
from datetime import datetime
from sqlalchemy import create_engine
from threading import Thread, Event
from sqlalchemy.orm import scoped_session, sessionmaker
from multiprocessing import Queue, Process, Value, Lock
from time import sleep  # Используется для тестирования

class TaskTimer(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.stopped = Event()  # Событие для остановки потока
        self.do_func = None  # Функция, которую нужно выполнять

    def run(self, do_func):
        self.do_func = do_func  # Установка функции для выполнения
        while self.stopped.wait(1):  # Ожидание 1 секунду
            if self.do_func:
                self.do_func()  # Выполнение функции

class TaskHandler(object):
    def __init__(self):
        self.queue = Queue()  # Очередь для задач
        self.daemon_workers = []  # Список процессов-демонов
        self.busy_workers = Value('i', 0)  # Счётчик занятых процессов
        self.workers_number = 0  # Общее количество процессов
        self.lock = Lock()  # Блокировка для потокобезопасности

    @staticmethod
    # Проверяет, соответствует ли текущее время расписанию задачи
    def is_proper_time(val, task_val):
        proper_val = list(task_val.split(','))
        res = False

        for time_val in proper_val:
            if time_val:
                if task_val == '*':
                    res = True
                elif '*/' in task_val:
                    reg_pat = re.compile('[0-9]+')
                    vals = reg_pat.findall()

                    if '-' in task_val and len(vals) >= 2:
                        res = 0 == (int(val) % int(vals[0])) - vals[1]
                    elif vals:
                        res = 0 == int(val) % int(vals[0])
                else:
                    res = int(val) == int(n)
            if res:
                break
        return res

    def put_in_queue(self, task):
        # Добавляет задачу в очередь, если есть свободные процессы
        print(self.busy_workers.value)

        if self.busy_workers.value == self.workers_number:
            print('All processes are busy, can\'t start ' + str(task.name))
        else:
            self.date_check(task)

     # Проверяет, соответствует ли текущее время расписанию задачи
    def date_check(self, task):
        date_now = datetime.now()

        try:
            time_table = [
                [date_now.minute, task.mins],
                [date_now.hour, task.hours],
                [date_now.day, task.day],
                [date_now.month, task.month],
                [date_now.weekday(), task.week_day]
            ]

            check = True

            for val in time_table:
                check = check and self.is_proper_time(val[0], val[1])
                if not check:
                    break

            if check:
                self.queue.put(task)  # Добавление задачи в очередь
        except Exception as e:
            print('Data corrupted!')
            print(str(e))

    def create_workers_pool(self):
        # Создаёт пул процессов-демонов
        for worker in range(self.workers_number):
            p = Process(target=self.run_task)
            p.daemon = True
            p.start()
            self.daemon_workers.append(p)

    def run_task(self):
        # Выполняет задачи из очереди
        while True:
            data = self.queue.get()  # Получение задачи из очереди
            self.lock.acquire()
            self.busy_workers.value += 1  # Увеличение счётчика занятых процессов
            self.lock.release()

            sleep(10)  # Имитация обработки задачи
            print('data found to be processed: {}'.format(data.name))

            self.lock.acquire()
            self.busy_workers.value -= 1  # Уменьшение счётчика занятых процессов
            self.lock.release()

class TaskManager(TaskHandler):
    def __init__(self):
        super().__init__()
        self.sql_query = 'SELECT * FROM tasks WHERE active=true'  #запрос для получения активных задач
        self.engine = None  # Движок базы данных
        self.db_session = None  # Сессия базы данных
        self.workers_number = 4  # Количество процессов

    # Выполняет запрос к базе данных для получения задач
    def ask_for_tasks(self):
        try:
            records = self.db_session.execute(self.sql_query)
            self.db_session.commit()
            return records
        except Exception as e:
            print('Failed making query due to error: ')
            print(str(e))

    # Проверяет и добавляет задачи в очередь
    def check_and_run(self):
        tasks = self.ask_for_tasks()
        if tasks:
            for task in tasks:
                self.put_in_queue(task)

    # Подключается к базе данных
    def connect_db(self):
        print('Trying to connect to db...')

        try:
            # TODO: добавить обработчик конфигурации
            engine = create_engine(config)
            print('Connected to db')
            self.engine = engine
            self.db_session = scoped_session(sessionmaker(bind=self.engine))
        except Exception as e:
            print('Failed trying to connect to db due to error: ')
            print(str(e))
