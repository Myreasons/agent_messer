import pandas as pd
import os
import pyodbc
import datetime as dt
import os
import psycopg2
from sqlalchemy import create_engine

def drop_src(mrf, city, skill, skillgroup, mindate, maxdate):
	print('src',mrf, city, skill, skillgroup, mindate, maxdate)
	con = psycopg2.connect(
		  database="sue", 
		  user="postgres", 
		  password="ajWTduSs",
		  host="172.19.16.130", 
		  port="5432")
	#МРФ	Площадка	Служба	Служба_групп
	droper = '''DELETE FROM erkc.ercc_source
	    WHERE "Дата" >= \'''' +mindate + '''\'
	    AND "Дата" <= \'''' +maxdate + '''\'
	    AND "МРФ" in ''' + mrf + '''
	    AND "Площадка" in ''' + city + '''
	    AND "Служба" in ''' + skill + '''
	    AND "Служба_групп" in ''' + skillgroup
		
	try:
		cur = con.cursor()
		cur.execute(droper)
		con.commit()
		con.close()
	except:
		print('cant drop: table doesnt exist')

			
def drop_fvr(city, project, category, mindate, maxdate):
	print('fvr',city, project, category, mindate, maxdate)
	con = psycopg2.connect(
		  database="sue", 
		  user="postgres", 
		  password="ajWTduSs",
		  host="172.19.16.130", 
		  port="5432")
	#Площадка	Проект	Категория персонала
	droper = '''DELETE FROM erkc.fvr_new
	    WHERE "Дата" >= '%s' '''  % mindate + '''
	    AND "Дата" <= '%s' '''  % maxdate + '''
	    AND "Площадка" in ''' + city + '''
	    AND "Проект" in ''' + project + '''
	    AND "Категория персонала" in ''' + category
	
	try:
		cur = con.cursor()
		cur.execute(droper)
		con.commit()
		con.close()
	except:
		print('cant drop: table doesnt exist')


def set_to_str(argv):
	print(argv)
	str = '''('''
	for el in argv:
		str += '\'' + el + '\', '
	str = str[0:-2]
	str += ''')'''
	return str

def beautiful_df(df):
	src_c = ['МРФ',	'Площадка',	'Служба',	'Служба_групп',	'Дата',	'Прогноз нагрузки',	'ФВО план.',
	'ФВО базовое',	'Поставлено в очередь',	'Обслужено операторами','Обслужено в пределах TST',
	'Среднее t ожидания',	'Макс. t ожидания',	'Среднее t обработки обращения (AHT) вход.',
	'Среднее время диалога вход.',	'Среднее t поствызова вход.',	'ФВО вход.',
	'ФВО не оплачиваемое',	'FCR',	'Кол-во исходящих',	'Среднее t обслуживания (AHT) исх.',
	'ФВО исх.',	'кол-во платных справок',	'кол-во обработанных эл. обращений',
	'кол-во тлг и объявлений, в т.ч. служебные',	'МД',	'Вызовов пропущено']
	fvr_c = ['Площадка',	'Дата',	'Проект',	'Категория персонала',	'Время по табелю + СУ и РВД',
	'Время по табелю',	'СУ и РВД',	'Время в системе ',	'Ring Time ',	'Время разговора (Talk Time)',
	'Время удержания (Hold)',	'Wrap up time',	'Время ожидания вызова',	'Перерыв',	'Тренинг',	'Коучинг',
	'Наставничество',	'Вызов к руководителю ',	'Прочие оплачиваемые перерывы',	'Wrap up time не оплачиваемое',
	'Исходящий обзвон',	'Дискретные контакты  (e-mail, МПЗ, ЕЛК и прочие), системное время',
	'Дискретные контакты  (e-mail, МПЗ, ЕЛК и прочие),  актируемое время',	'Обед',	'Оплачиваемое заказчиком обучение',
	'Оплата статуса Ring']
	df = df.reset_index()
	if not('Площадка' in list(df.columns.values)):
		df.columns = df.iloc[1]
		df = df.drop([1])

	df = df.loc[df['Площадка'].isnull() == False]
	df = df.loc[df['Площадка'] != "ИТОГО:"]
	
	#df = df.loc[df['Дата'].isnull() == False]
	
	if set(fvr_c) <= set(list(df.columns.values)):
		df = df.iloc[:, 0:36]
		df = df[fvr_c]
		df = df.loc[df['Дата'].isnull() == False]
		df['Дата'] = pd.to_datetime(df['Дата'], errors='coerce')
		df = df.loc[df['Проект'].isnull() == False]
		df = df.rename(columns={'Дискретные контакты  (e-mail, МПЗ, ЕЛК и прочие), системное время': 'ДК Системное',
							'Дискретные контакты  (e-mail, МПЗ, ЕЛК и прочие),  актируемое время':'ДК актируемое',
									'Время разговора (Talk Time)':'Время разговора',
									'Время удержания (Hold)':'Время удержания'})
		drop_fvr(city = set_to_str(set(df['Площадка'].unique())), 
			project = set_to_str(set(df['Проект'].unique())), 
			category = set_to_str(set(df['Категория персонала'].unique())), 
			mindate = str(df['Дата'].min()), 
			maxdate = str(df['Дата'].min())
			)

		engine = create_engine('postgresql+psycopg2://postgres:ajWTduSs@172.19.16.130:5432/sue')
		df.to_sql('fvr_new', engine, schema='erkc', if_exists="append", index = False)

	elif set(src_c) <= set(list(df.columns.values)):
		df = df[src_c]
		df = df.loc[df['Дата'].isnull() == False]
		df['Дата'] = pd.to_datetime(df['Дата'], errors='coerce')
		#МРФ	Площадка	Служба	Служба_групп
		df = df.rename(columns={'Среднее t обработки обращения (AHT) вход.': 'AHT вх',
									'Среднее t обслуживания (AHT) исх.':'AHT исх'})

		drop_src(mrf = set_to_str(set(df['МРФ'].unique())), 
			city = set_to_str(set(df['Площадка'].unique())), 
			skill = set_to_str(set(df['Служба'].unique())), 
			skillgroup = set_to_str(set(df['Служба_групп'].unique())), 
			mindate = str(df['Дата'].min()), 
			maxdate = str(df['Дата'].min())
			)

		engine = create_engine('postgresql+psycopg2://postgres:ajWTduSs@172.19.16.130:5432/sue')
		df.to_sql('ercc_source', engine, schema='erkc', if_exists="append", index = False)
	else:
		raise ValueError('Excel file has bad header')
	df = df.set_index('Площадка')


	return df


def excel_to_db(param='', Customer=''):
	df_src, df_fvr = pd.DataFrame(), pd.DataFrame()
	path=r"C:\Users\Admin\Desktop\Projects\agent messer\sent_mail\20210117"
	files = os.listdir(path = path)
	for file in files:
		df_src = beautiful_df(pd.read_excel(path +r'\\' + file, sheet_name = 'источник', engine='openpyxl'))
		df_fvr = beautiful_df(pd.read_excel(path +r'\\' + file, sheet_name = 'ФВР_new', engine='openpyxl'))



'''
some_list = ['a', 'b', 'c', 'sadasdasd', '']
print(set_to_str(set(some_list)))
'''
print('\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n')
excel_to_db()

