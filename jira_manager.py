import win32com.client
import Settings as s
import warnings
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import psycopg2
import jira
from jira import JIRA
import re
import sys
sys.path.insert(0, r'C:\Users\Admin\Desktop\Projects\agent messer\sub lib')
from outlook_manager import Mail
warnings.simplefilter(action='ignore', category=FutureWarning)
#from OUlook import adress_security_checker
import requests
import telebot
import shutil

def shiz(number):
		counter = {
		'0': 'ноль', 
		'1': 'целковый', 
		'2': 'чекушка', 
		'3': 'порнушка', 
		'4': 'пердушка', 
		'5': 'засирушка', 
		'6': 'жучок', 
		'7': 'мудачок', 
		'8': 'хуй на воротничок', 
		'9': 'дурачок'
		}
		res = ''
		number = str(number)
		for symb in number:
			try:
				res += counter[symb] + ' '
			except:
				res += symb
		res += 'ВСЕ'
		return res

class TelegramBot1:

	def __init__(self):
	    self.token = '1453126526:AAFPCES1bseQnOm5T88OOrRj0_dM1gn4qDo'
	    self.api_url = "https://api.telegram.org/bot{}/".format(self.token)

	def send_group_message(self, text, chat_id = -320444958):
	    params = {'chat_id': chat_id, 'text': text}
	    method = 'sendMessage'
	    resp = requests.post(self.api_url + method, params)
	    return resp

	def send_private_message(self, text, chat_id):
		params = {'chat_id': chat_id, 'text': text}
		method = 'sendMessage'
		resp = requests.post(self.api_url + method, params)
		return resp


class JiraManager():
	def __init__(self):
		self.jira_options = {'server': 'https://jira.omnichat.tech/'}
		self.jira = JIRA(options=self.jira_options, auth=('a.balov', 'MBcLySH5'))
		self.df_to_check = pd.DataFrame()
		self.df_to_score = pd.DataFrame()
		self.minimal_scores = 12
		self.bot = TelegramBot1()
		self.engine = create_engine('postgresql+psycopg2://postgres:ajWTduSs@172.19.16.130:5432/jetforms_src')
		self.con = psycopg2.connect(database="jetforms_src", 
								  user="postgres", 
								  password="ajWTduSs",
								  host="172.19.16.130", 
								  port="5432")
		self.df_scores = pd.read_sql('select * from jira_scoretable', self.con)
		self.df_ignorelist = pd.read_sql('select * from jira_excepted_users', self.con)


	def _adress_security_checker(self, adress):
		rtk = ['ROSTELECOM', r'rtk-dwh-support@rt.ru', 'rostelecom-cc.ru', 'VOTpreparer_UNI@south.rt.ru',r'@sibir.rt.ru','Alla.Bulda@RT.RU',
			r'@dv.rt.ru', r'@nw.rt.ru','@mail.ntt.ru','@rt.ru', '@RT.RU','@center.rt.ru','@volga.rt.ru','@south.rt.ru']
		for i in rtk:
		    if i in adress:
		        return True
		return False


	def get_mail(self):
		df = pd.DataFrame()
		outlook = win32com.client.Dispatch("Outlook.Application").GetNamespace("MAPI")
		inbox = outlook.GetDefaultFolder(6)

		for item in inbox.Items:
			if item.SenderEmailType == 'EX':
				task_customer = item.Sender.GetExchangeUser().PrimarySmtpAddress
			else:
				task_customer = item.SenderEmailAddress
			if self._adress_security_checker(task_customer):
				try:
					if self.attribute_mail_checker(who=task_customer, 
												whom=item.To, 
												theme=self.beautiful_theme(item.subject),
												text = item.body.split("с уважением")[0]):
						df = df.append({'text': item.body,
						                'subj': self.beautiful_theme(item.subject),
						                'cust': task_customer}, 
						                ignore_index=True)
				except:
						print('')
			item.delete()
		self.df_to_check = self.df_to_check.append(df)

    

	def beautiful_theme(self, theme):
		return theme.replace("FW: ", "").replace("FW:", "").replace("RE: ", "").replace("RE:", "")

	#first checking step: customer and mail-adress To
	def attribute_mail_checker(self, who='', whom='', theme='', text=''):
		if 'reports@rostelecom-cc.ru' in whom:
			for item in self.df_ignorelist['mail']:
				if who == item:
					return False
		else:
			return False

		atr = self.check_task_from_db(self.beautiful_theme(theme))
		if len(atr) != 0:
			self.add_comment_to_issue(str(atr[0]), '#S'+text.split('С уважением')[0])
			return False

		return True


	#second checking step: semantic points
	def semantic_manager(self):

		def splitter(text):
			text = text.lower()
			text = text.split("с уважением")
			text = text[0].replace('_x000d_', '')
			text[0].replace(',', '')
			text[0].replace('.', '')
			text[0].replace('\n', ' ')
			return text.split(' ')

		scoretable = {}
		for ind in self.df_scores.index.values:
			scoretable.update({self.df_scores['text'].iloc[ind]: self.df_scores['score'].iloc[ind]})

		for ind2 in self.df_to_check.index.values:
			item = self.df_to_check['text'].iloc[ind2]
			scores = 0
			for word in splitter(item):
				parsed_str = word.replace('\n', '').replace('.', '').replace(',', '')
				try:
					scores += scoretable[parsed_str]
				except:
					scores += 0
			if scores >= self.minimal_scores:
				self.df_to_score = self.df_to_score.append(self.df_to_check.iloc[ind2])

	def create_jira_issue(self, issue_dict, customer):
		new_issue = self.jira.create_issue(fields=issue_dict)
		self.bot.send_group_message('Вам заявочка пришла, '+shiz(str(new_issue))+': '+issue_dict['summary']+'\n'+customer+' пишет: '+str(new_issue.fields.description))
		return str(new_issue)

	def send_mail_to_user(self, customer, text, mail_theme, att = ''):
		i = Mail(to = customer, 
			copy = 'reports@rostelecom-cc.ru', 
			theme = mail_theme, 
			body = text, 
			attach = att
			)
		i.send_mail()


	def search_jira_issue(self, id):
		issue = self.jira.issue(id)
		res = {'ID': str(issue),
			 'issuetype': issue.fields.issuetype.name,
			 'status': issue.fields.status.name,
			 'summary': issue.fields.summary,
			 'description': issue.fields.status.description,
			 'created': issue.fields.created[:10],
			 'resolutiondate': str(issue.fields.resolutiondate)[:10],
			'assignee': str(issue.fields.assignee)}
		return res


	def add_task_to_db(self, customer, jira_att):
		jira_att.update({'customer': customer})
		df = pd.DataFrame(jira_att, index=[0])
		df.to_sql('jira_archive', self.engine, schema ='public', if_exists="append", index = False)


	def get_jira_updates(self):
		df_archive = pd.read_sql('''select * from jira_archive where status <> 'Done' 
			and status <> 'Закрыто' ''', self.con)
		df_archive['created'] = pd.to_datetime(df_archive['created']).apply(lambda x: str(x.date()))


		for ind in df_archive.index.values:
			try:
				df_archive['resolutiondate'].iloc[ind] = str(df_archive['resolutiondate'].iloc[ind].date())
			except:
				pass

			issue = self.jira.issue(df_archive['ID'].iloc[ind])
			customer = df_archive['customer'].iloc[ind]
			jira_dict = {'ID': str(issue),
			 'issuetype': issue.fields.issuetype.name,
			 'status': issue.fields.status.name,
			 'summary': issue.fields.summary,
			 'description': issue.fields.status.description,
			 'created': issue.fields.created[:10],
			 'resolutiondate': str(issue.fields.resolutiondate)[:10],
			'assignee': str(issue.fields.assignee)}
			jira_dict.update({'customer': customer})
			jira_dict.update({'comments': self.get_comments(jira_dict['ID'])})
			
			df_to_dict = df_archive.iloc[ind].to_dict()

			if jira_dict['resolutiondate'] != df_to_dict['resolutiondate'] or jira_dict['comments'] != df_to_dict['comments']:
				q = '''DELETE FROM jira_archive WHERE "ID" = '%s' '''% df_archive['ID'].iloc[ind]
				cur = self.con.cursor()
				cur.execute(q)
				self.con.commit()
				to_add = pd.DataFrame(jira_dict, index =[0])
				to_add.to_sql('jira_archive', self.engine, schema ='public', if_exists="append", index = False)

				self.send_mail_to_user(jira_dict['customer'],
					'Обновление по заявке: ' + jira_dict['ID'] + '\n'+ self.beautiful_body(jira_dict)+ '\n\n',
					jira_dict['summary'], self.get_attach_from_jira(jira_dict['ID']))


	def check_task_from_db(self, theme):
		df_search = pd.read_sql('''select "ID" from jira_archive where status <> 'Done'
			AND summary ='%s' ''' %theme, self.con)
		return(df_search['ID'])


	def add_comment_to_issue(self, id, text):
		self.jira.add_comment(str(id), 'Пользователь пишет: '+text)
		self.bot.send_group_message('Пользователь пишет: , '+ text +'\nЗаявочка: '+shiz(str(id)))


	def update_sources(self):
		self.df_scores = pd.read_sql('select * from jira_scoretable', self.con)
		self.df_ignorelist = pd.read_sql('select * from jira_excepted_users', self.con)


	def get_comments(self, id):
		
		comment_text = ''
		issue = self.jira.issue(id)
		comments = self.jira.comments(issue)
		for comment in comments:
			if '#S' in str(comment.body):
				comment_text += str(comment.author)+ ': '+str(comment.body) + '\n'

		return comment_text.replace('Балов Александр: Пользователь пишет','Пользователь пишет')

		

	def look_comments_from_jira(self, id):
		issue = self.jira.issue(id)
		att_list = []
		comments = self.jira.comments(issue)
		for comment in comments:
			att_list.append(str(comment.body).replace('[','').replace(']','').replace('!','').replace('^','').replace(']',''))
		return att_list


	def get_attach_from_jira(self, id):
		issue = self.jira.issue(id)
		att_list = []
		true_list = self.look_comments_from_jira(id)
		for attachment in issue.fields.attachment:
			image = attachment.get()
			filename=attachment.filename
			if filename in true_list:
				completeName = r'C:\Users\Admin\Desktop\Projects\agent messer\sent_mail\\'+filename
				with open(completeName, 'wb') as f:
					f.write(image)	
				att_list.append(completeName)
		return att_list


	def beautiful_body(self, dict):
		outup = ''
		for key in dict.keys():
			outup += str(key) + ': ' + str(dict[key]).replace('#S','') + '\n'
		return outup
	
	def main_input_proccess(self):
		self.update_sources()
		self.get_mail()
		self.semantic_manager()
		if len(self.df_to_score) > 0:
			for ind in self.df_to_score.index.values:
				issue_dict = {'project': 'BI',
							'summary': str(self.df_to_score['subj'].iloc[ind]),
							'description': str(self.df_to_score['text'].iloc[ind]),
							'issuetype': {'name': 'Задача'}}
				current_issue = str(self.create_jira_issue(issue_dict, self.df_to_score['cust'].iloc[ind]))
				self.add_task_to_db(self.df_to_score['cust'].iloc[ind], self.search_jira_issue(current_issue))
				self.send_mail_to_user(self.df_to_score['cust'].iloc[ind], 
					'По вашему обращению зарегистрирована задача: ' + current_issue + '\n\n', 
					self.df_to_score['subj'].iloc[ind])


	def main_output_proccess(self):
		self.get_jira_updates()



def start_jira(param='', Customer=''):
	i = JiraManager()
	i.main_input_proccess()
	i.main_output_proccess()