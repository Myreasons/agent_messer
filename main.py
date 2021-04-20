from Classes import start_process
#from telegram_pool_starter import start_telegram_pool
from threading import Thread
from telegram import BotPooler
import pythoncom

i = BotPooler()
def start_telegram_pool():
	pythoncom.CoInitialize()
	i.start_pool()


th_1, th_2 = Thread(target=start_process), Thread(target = start_telegram_pool)

if __name__ == '__main__':
	th_1.start(), th_2.start()
	th_1.join(), th_2.join()