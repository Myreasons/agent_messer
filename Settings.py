import warnings
import sys
from nptime import nptime
import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '\worker functions')) #path to task folder

warnings.simplefilter(action='ignore', category=FutureWarning)

'''Telegram settings'''
#accepted users IDs
telegram_seq = {
    'teest': 11111,
}
teletoken = 'test' #bot token
grupchatid = -11111 #group_chat to push messages

class Task():
    #name - display name
    #customer - customer name
    #system - system classificator name
    #params - any params for task. Add {'push_it': True} to auto push telegram log messages
    #func - link to worker function
    def __init__(self, name, customer, system, params, func=''):
        self.name = name
        self.customer = customer
        self.system = system
        self.params = params
        self.func = func

    def set_parametrs(self, new_name, new_customer, new_system, new_params, new_func):
        self.name = new_name
        self.customer = new_customer
        self.system = new_system
        self.params = new_params
        self.func = new_func


class regular_task():
    # task - worker function task
    # time_start - task start time
    # period - type of period freq
    # freq - time freq
    # time end - time when tasks end their work
    # *day_of_month  - if period == month, chose the day [1...31] when task will start.
    def __init__(self, task, time_start, period, freq, time_end, day_of_month=''):
        self.task = task
        self.time_start = time_start
        self.period = period
        self.freq = freq
        self.time_end = time_end
        self.day_of_month = day_of_month
        self.times = self.create_period_list(
            self.time_start,
            self.time_end,
            self.freq
        )

    def create_period_list(self, start, end, freq):
        self.times = []
        self.start = start
        self.stop = end
        self.freq = freq

        self.times.append(self.start)
        while self.start <= self.stop:
            if self.start == self.stop:
                break

            if self.start + self.freq < self.start:
                break
            else:
                self.start += self.freq
                self.times.append(self.start)
        return (self.times)


path_to_datebase = os.path.join(os.path.dirname(__file__), '\Database\Event collector.db')
datebase_name = 'Event collector.db'

def first_try(param='', Customer=''):
    print ('nice')

# commands and worker-functions mapping for Outlook
functions_map = {
    'name': first_try
    }

# task, time_start, period, freq, time_end
regular_tasks_container = {
    regular_task(
        task=Task('JIRA assistent','JIRA','Settings','',func=start_jira),
        time_start = nptime(0,0,1),
        period = 'Every day',
        freq = datetime.timedelta(minutes=5),
        time_end = nptime(23,55,15)
        )
    }

    


# Examples
'''
create_daily_chat, create_report1

regular_tasks_container = {
    regular_task(
        task=Task('TEST_EVERYDAY','self','Settings',[1,2],func=f.deviation_analyse),
        time_start = nptime(5,0,0),
        period = 'Every day',
        freq = datetime.timedelta(seconds=1),
        time_end = nptime(23,0,0)
    ),
    regular_task(
            task=Task('TEST_SOMEDAYS','self','Settings',[1,2],func=f.deviation_analyse),
            time_start = nptime(10,0,0),
            period = ['Monday','Tuesday'],
            freq = datetime.timedelta(hours=0),
            time_end = nptime(10,0,0)
        ),
    regular_task(
            task=Task('TEST_EVERY_MONTH','self','Settings',[1,2],func=f.deviation_analyse),
            time_start = nptime(5,0,0),
            period = 'Every month',
            freq = datetime.timedelta(hours=1),
            time_end = nptime(10,0,0),
            day_of_month = [19,20,21]
        ),
    regular_task(
        task=Task('Отправка Daily на FTP','EPGU',
            'Settings',{'push_it': True},func=create_daily_chat),
        time_start = nptime(6,30,0),
        period = 'Every day',
        freq = datetime.timedelta(hours=0),
        time_end = nptime(6,30,0)

    }

'''
