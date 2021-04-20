from mesa import Agent, Model, time
from time import sleep
import pythoncom

from OUlook import looking_for_mails
import datetime as dt
import calendar
import sqlite3
import pandas as pd
import freq_check_func as fcf
from Settings import regular_tasks_container, datebase_name, path_to_datebase
import warnings
import logging
import traceback
from memory_profiler import memory_usage
from telegram import TelegramBot


logging.basicConfig(filename='usage.log',  level=logging.DEBUG)
warnings.simplefilter(action='ignore', category=FutureWarning)

class Container():
    def __init__(self):
        self.tasks = []

    def get_task_quantity(self):
        return len(self.tasks)

    def pop(self):
        return self.tasks.pop()


class db_manager():
    def __init__(self):
        self.path = path_to_datebase
        self.db = datebase_name
        self.con = sqlite3.connect(path_to_datebase)

    def insert_into_db(self, df):
        df.to_sql("Events", self.con, if_exists="append")
        self.con.commit()

    def look_for_telegram_task(self):
        df = pd.read_sql('select * from Telegram_query', self.con)
        cur = self.con.cursor()
        cur.execute('delete from Telegram_query')
        self.con.commit()
        cur.close()
        return df

    def close(self):
        if self.con:
            self.con.close()

class HR_agent(Agent):
    """An agent for create workers and give them tasks"""
    """Only 1 task for every worker"""
    def __init__(self, unique_id,  model):
        super().__init__(unique_id, model)
        self.quantity = Process.container.get_task_quantity()

    def step(self):
        i = 0

        while Process.container.tasks:
            Process.Workers.append(
                Worker(
                    unique_id='Worker_' + str(i),
                    task=Process.container.pop(),
                    model=FakeModel()
                )
            )
            i += 1


class Worker(Agent):
    """An agent for realise tasks"""
    def __init__(self, unique_id, task, model):
        super().__init__(unique_id, model)
        self.task = task
        self.model = model
        self.bot = TelegramBot()


    def step(self):
        db = db_manager()
        try:
            self.task.func(self.task.params, self.task.customer)

            d = {'Datetime': dt.datetime.now(), 'Task name': self.task.name, 'Worker':self.unique_id,
                 'Status':'Done','Customer':self.task.customer,'System':self.task.system}
            df = pd.DataFrame(data=d,index=[0]).set_index('Datetime')
            db.insert_into_db(df=df)
            fcf.last_worker_datetime.update({self.task.name:dt.datetime.now()})

            try:
                if self.task.params['push_it'] == True:
                    self.bot.send_group_message(self.task.name + ' done')
            except:
                pass

        except Exception as e:
            err = str(traceback.format_exc())
            d = {'Datetime': dt.datetime.now(), 'Task name': self.task.name, 'Worker': self.unique_id,
                 'Status': 'Error', 'Customer': self.task.customer, 'System': self.task.system}
            df = pd.DataFrame(data=d, index=[0]).set_index('Datetime')
            db.insert_into_db(df=df)

            try:
                if self.task.params['push_it'] == True:
                    self.bot.send_group_message(self.task.name + ' error: ' + err)
            except:
                pass

        finally:
            db.close()


class Outlook_searcher_agent(Agent):
    """An agent for search tasks by Outlook."""
    def __init__(self, unique_id, model):
        super().__init__(unique_id, model)


    def step(self):
        # The agent's step will go here.
        Process.container.tasks = looking_for_mails()


class Scheduler(Agent):
    """An agent for schedule tasks"""
    def __init__(self, unique_id, model):
        super().__init__(unique_id, model)

    def validate_last_worker_datetime(self):
        try:
            if min(fcf.last_worker_datetime.values()).date() != dt.datetime.now().date():
                fcf.last_worker_datetime.clear()
        except:
            pass

    def step(self):
        self.validate_last_worker_datetime()
        adding_tasks = []
        today_weekday = calendar.day_name[dt.datetime.today().weekday()]
        for task in regular_tasks_container:
            if fcf.date_validator(period=task.period,
                                  times=task.times,
                                  name=task.task.name,
                                  day_of_month=task.day_of_month):
                Process.container.tasks.append(task.task)

#test her
class Telegram_tasks(Agent):
    """An agent for schedule tasks"""
    def __init__(self, unique_id, model):
        super().__init__(unique_id, model)

    def step(self):
        db = db_manager()
        adding_tasks = db.look_for_telegram_task()
        db.close()
        for task in regular_tasks_container:
            if task.task.name in adding_tasks['task'].values:
                Process.container.tasks.append(task.task)


"""A model with scheduler and HR agents"""
class Process(Model):
    container = Container()
    Workers = []

    def __init__(self):
        self.schedule = time.BaseScheduler(self)  #Chose scheduling method
        self.schedule.add(Outlook_searcher_agent('Ou.S', FakeModel()))
        self.schedule.add(Telegram_tasks('Telegram', FakeModel()))
        self.schedule.add(Scheduler("Scheduler", FakeModel()))
        self.schedule.add(HR_agent('HR', FakeModel()))


    def step(self):
        '''Advance the model by one step.'''
        self.schedule.step()
        runner = WorkersRunner()
        runner.step()


"""The second model to use worker-agents"""

class WorkersRunner(Model):
    def __init__(self):
        self.scheduler = time.BaseScheduler(FakeModel())

        while Process.Workers:
            self.scheduler.add(Process.Workers.pop())

    def step(self):
        '''Advance the model by one step.'''
        self.scheduler.step()

class FakeModel(Model):
    def __init__(self):
        super(FakeModel, self).__init__();

def start_process():
    pythoncom.CoInitialize()
    empty_model = Process()


    def runloop():
        empty_model.step()
        logging.debug(str(dt.datetime.now()) + ' ' + str(memory_usage()[0]))
        sleep(5)

    while True:
        runloop()



