import requests
import telebot
from Settings import regular_tasks_container, path_to_datebase, telegram_seq, teletoken, grupchatid
import pandas as pd
import sqlite3

class TelegramBot:

	def __init__(self):
	    self.token = teletoken
	    self.api_url = "https://api.telegram.org/bot{}/".format(self.token)
	    self.group_chat_push = grupchatid

	def get_updates(self, offset=None, timeout=30):
	    method = 'getUpdates'
	    params = {'timeout': timeout, 'offset': offset}
	    resp = requests.get(self.api_url + method, params)
	    result_json = resp.json()['result']
	    return result_json

	def send_group_message(self, text, chat_id = grupchatid):
	    params = {'chat_id': chat_id, 'text': text}
	    method = 'sendMessage'
	    resp = requests.post(self.api_url + method, params)
	    return resp

	def send_private_message(self, text, chat_id):
		params = {'chat_id': chat_id, 'text': text}
		method = 'sendMessage'
		resp = requests.post(self.api_url + method, params)
		return resp

	def get_last_update(self):
	    get_result = self.get_updates()

	    if len(get_result) > 0:
	        last_update = get_result[-1]
	    else:
	        last_update = get_result[len(get_result)]

	    return last_update


class BotPooler:
	bot = telebot.TeleBot(teletoken)

	@bot.message_handler(content_types=['text'])
	def get_text_messages(message):
		print(message.from_user.id, message.from_user.first_name, message.text.lower())
		if message.from_user.id in telegram_seq.values():

			if 'помоги' in message.text.lower():
				d = []
				for n in regular_tasks_container:
					d.append(n.task.name)
				mes = 'Вот список доступных команд: \n' + '\n'.join([str(elem) for elem in d])
				bot = telebot.TeleBot(teletoken).send_message(message.chat.id, mes)
			else:
				i = 0
				for tasks in regular_tasks_container:
					if tasks.task.name.lower() == message.text.lower():
						print(message.from_user.id, message.from_user.first_name, tasks.task.name)
						con = sqlite3.connect(path_to_datebase)
						df = pd.DataFrame(data={'task': tasks.task.name},index=[0])
						df.to_sql("Telegram_query", con, if_exists="append")
						con.commit()
						con.close()
						i+=1
						telebot.TeleBot(teletoken).send_message(message.chat.id, 'Задача направлена на исполнение!')

				if i == 0:
					bot = telebot.TeleBot(teletoken).send_message(message.chat.id, 'Непонятная команда. Используйте "Помоги" для вывода справки.')


	def start_pool(self):
		while True:
			try:
				self.bot.polling(none_stop=True, interval = 0)
			except:
				print('Seems like eth connection is unstable')

