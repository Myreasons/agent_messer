import pandas as pd 
import pyodbc
import datetime as dt
import os
import psycopg2
from sqlalchemy import create_engine

date_start = dt.datetime.today().date() - dt.timedelta(days = 2)
date_end = dt.datetime.today().date()

q = '''
	WITH calendar AS (
   SELECT DISTINCT mart.dim_date.date_date "Дата", 
   mart.dim_interval.interval_name "Интервал", 
   mart.dim_skill.skill_name "Навык"
	FROM mart.dim_date
	INNER JOIN mart.dim_interval
	ON mart.dim_interval.interval_id <=1000
	inner join mart.dim_skill
	ON mart.dim_skill.skill_id  > -5
	WHERE mart.dim_date.date_date BETWEEN '%s' ''' % date_start + ''' AND '%s' ''' % date_end + '''
	AND			mart.dim_skill.skill_id IN (10,3,187,198,191,189,25)
        ),
        personel_fact AS (
        SELECT      sum(mart.fact_schedule_forecast_skill.scheduled_resources_m)/60*4 fact_resources_h,
                    mart.dim_date.date_date "Дата",
                    mart.dim_skill.skill_name "Навык",
                    mart.dim_interval.interval_name "Интервал"
        FROM		mart.fact_schedule_forecast_skill
        INNER JOIN 	mart.bridge_time_zone 
        ON 			mart.fact_schedule_forecast_skill.interval_id = mart.bridge_time_zone.interval_id 
        AND 		mart.fact_schedule_forecast_skill.date_id = mart.bridge_time_zone.date_id
        INNER JOIN 	mart.dim_date 
        ON 			mart.bridge_time_zone.local_date_id = mart.dim_date.date_id 
        INNER JOIN 	mart.dim_interval 
        ON 			mart.bridge_time_zone.local_interval_id = mart.dim_interval.interval_id 
        INNER JOIN 	mart.dim_time_zone 
        ON 			mart.dim_time_zone.time_zone_id = mart.bridge_time_zone.time_zone_id
        INNER JOIN	mart.dim_skill
        ON			mart.dim_skill.skill_id = mart.fact_schedule_forecast_skill.skill_id
        WHERE 		mart.dim_date.date_date BETWEEN '%s' ''' % date_start + ''' AND '%s' ''' % date_end + '''
        AND			mart.fact_schedule_forecast_skill.scenario_id = 2
        AND			mart.dim_time_zone.time_zone_code ='Russian Standard Time'
        AND			mart.dim_skill.skill_id IN (10,3,187,198,191,189,25)
        GROUP BY	mart.dim_skill.skill_name,
                    mart.dim_date.date_date,
                    mart.dim_interval.interval_name),
                    forecasted_table AS (
        SELECT 		mart.dim_skill.skill_name AS "Навык",
                    mart.dim_date.date_date AS "Дата",
                    mart.dim_interval.interval_name AS "Интервал",
                    SUM(mart.fact_forecast_workload.forecasted_calls) AS "Прогноз звонков",
                    COALESCE(SUM(mart.fact_forecast_workload.forecasted_after_call_work_s) / NULLIF(SUM(mart.fact_forecast_workload.forecasted_calls),0),0) AS "Прогноз постобработки",
                    COALESCE(SUM(mart.fact_forecast_workload.forecasted_talk_time_s) /  NULLIF(SUM(mart.fact_forecast_workload.forecasted_calls),0),0) AS "Прогноз ATT"
        FROM 		mart.fact_forecast_workload
        INNER JOIN 	mart.dim_skill
        ON 			mart.fact_forecast_workload.skill_id = mart.dim_skill.skill_id
        INNER JOIN 	mart.bridge_time_zone 
        ON 			mart.fact_forecast_workload.interval_id = mart.bridge_time_zone.interval_id 
        AND 		mart.fact_forecast_workload.date_id = mart.bridge_time_zone.date_id
        INNER JOIN 	mart.dim_date 
        ON 			mart.bridge_time_zone.local_date_id = mart.dim_date.date_id
        INNER JOIN 	mart.dim_interval 
        ON 			mart.bridge_time_zone.local_interval_id = mart.dim_interval.interval_id
        INNER JOIN 	mart.dim_time_zone
        ON 			mart.dim_time_zone.time_zone_id = mart.bridge_time_zone.time_zone_id
        WHERE		mart.dim_date.date_date BETWEEN '%s' ''' % date_start + ''' AND '%s' ''' % date_end + '''
        AND			mart.dim_skill.skill_id IN (10,3,187,198,191,189,25)
        AND			mart.fact_forecast_workload.scenario_id = 3
        AND			mart.dim_time_zone.time_zone_code = 'Russian Standard Time'
        GROUP BY 	mart.dim_skill.skill_name,
                    mart.dim_date.date_date,
                    mart.dim_interval.interval_name),
                    fact_table AS (
        SELECT 		mart.dim_skill.skill_name AS "Навык",
                    mart.dim_date.date_date AS "Дата",
                    mart.dim_interval.interval_name AS "Интервал",
                    mart.dim_skill.skill_id,
                    CASE
                    SUM(mart.fact_queue.offered_calls + mart.fact_queue.overflow_in_calls) 
                    WHEN NULL THEN 0
                    ELSE SUM(mart.fact_queue.offered_calls + mart.fact_queue.overflow_in_calls) END AS "Поступившие вызовы",
                    SUM(mart.fact_queue.abandoned_calls) AS "Неотвеченные вызовы",
                    COALESCE(SUM(mart.fact_queue.answered_calls_within_SL) / NULLIF(SUM(mart.fact_queue.offered_calls + mart.fact_queue.overflow_in_calls),0),0) AS "SL",
                    COALESCE(sum(mart.fact_queue.talk_time_s),0) AS "АТТ",
                    COALESCE(SUM(mart.fact_queue.after_call_work_s),0) AS "АCW",
                    SUM(mart.fact_queue.answered_calls) AS "Отвеченные вызовы",
                    sum(mart.fact_queue.answered_calls_within_sl) AS "answered in tst"
        FROM 		mart.fact_queue
        INNER JOIN  mart.dim_queue
        ON			mart.dim_queue.queue_id = mart.fact_queue.queue_id
        INNER JOIN	mart.bridge_queue_workload
        ON 			mart.bridge_queue_workload.queue_id = mart.dim_queue.queue_id
        INNER JOIN 	mart.dim_skill
        ON 			mart.bridge_queue_workload.skill_id = mart.dim_skill.skill_id
        INNER JOIN 	mart.bridge_time_zone 
        ON 			mart.fact_queue.interval_id = mart.bridge_time_zone.interval_id 
        AND 		mart.fact_queue.date_id = mart.bridge_time_zone.date_id
        INNER JOIN 	mart.dim_date 
        ON 			mart.bridge_time_zone.local_date_id = mart.dim_date.date_id
        INNER JOIN 	mart.dim_interval 
        ON 			mart.bridge_time_zone.local_interval_id = mart.dim_interval.interval_id
        INNER JOIN 	mart.dim_time_zone
        ON 			mart.dim_time_zone.time_zone_id = mart.bridge_time_zone.time_zone_id
        WHERE		mart.dim_date.date_date BETWEEN '%s' ''' % date_start + ''' AND '%s' ''' % date_end + '''
        AND			mart.dim_skill.skill_id IN (10,3,187,198,191,189,25)
		AND			mart.dim_time_zone.time_zone_code = 'Russian Standard Time'
        GROUP BY 	mart.dim_skill.skill_name,
                    mart.dim_date.date_date,
                    mart.dim_interval.interval_name,
                    mart.dim_skill.skill_id),
                    personel_plan AS (
        SELECT      sum(mart.fact_schedule_forecast_skill.forecasted_resources_incl_shrinkage_m) / 30 forecasted_resources_incl_shrinkage_m,
                    sum(mart.fact_schedule_forecast_skill.scheduled_resources_m) / 30 scheduled_resources_m,
                    sum(mart.fact_schedule_forecast_skill.forecasted_resources_m) / 30 forecasted_resources_m,
                    mart.dim_date.date_date "Дата",
                    mart.dim_skill.skill_name "Навык",
                    mart.dim_interval.interval_name "Интервал"
        FROM		mart.fact_schedule_forecast_skill
        INNER JOIN 	mart.bridge_time_zone 
        ON 			mart.fact_schedule_forecast_skill.interval_id = mart.bridge_time_zone.interval_id 
        AND 		mart.fact_schedule_forecast_skill.date_id = mart.bridge_time_zone.date_id
        INNER JOIN 	mart.dim_date 
        ON 			mart.bridge_time_zone.local_date_id = mart.dim_date.date_id 
        INNER JOIN 	mart.dim_interval 
        ON 			mart.bridge_time_zone.local_interval_id = mart.dim_interval.interval_id 
        INNER JOIN 	mart.dim_time_zone 
        ON 			mart.dim_time_zone.time_zone_id = mart.bridge_time_zone.time_zone_id
        INNER JOIN	mart.dim_skill
        ON			mart.dim_skill.skill_id = mart.fact_schedule_forecast_skill.skill_id
        WHERE 		mart.dim_date.date_date BETWEEN '%s' ''' % date_start + ''' AND '%s' ''' % date_end + '''
        AND			mart.fact_schedule_forecast_skill.scenario_id = 3
        AND			mart.dim_time_zone.time_zone_code ='Russian Standard Time'
        AND			mart.dim_skill.skill_id IN (10,3,187,198,191,189,25)
        GROUP BY	mart.dim_skill.skill_name,
                    mart.dim_date.date_date,
                    mart.dim_interval.interval_name),
                    src as(
		select 10 AS skill_id, 2 AS mrf_target_id, 10007 AS skill_target_id, 22 AS sip_group_id UNION
		select 3 AS skill_id, 2 AS mrf_target_id, 10008 AS skill_target_id, 22 AS sip_group_id UNION
		select 187 AS skill_id, 2 AS mrf_target_id, 10009 AS skill_target_id, 23 AS sip_group_id UNION
		select 198 AS skill_id, 2 AS mrf_target_id, 10011 AS skill_target_id, 24 AS sip_group_id UNION
		select 191 AS skill_id, 2 AS mrf_target_id, 10012 AS skill_target_id, 25 AS sip_group_id UNION
		select 189 AS skill_id, 2 AS mrf_target_id, 10010 AS skill_target_id, 23 AS sip_group_id UNION
		select 25 AS skill_id, 2 AS mrf_target_id, 10013 AS skill_target_id, 25 AS sip_group_id
		)
        SELECT 		src.mrf_target_id                                AS mrf_target_id,
		           src.sip_group_id                                 AS sip_group_id,
		           src.skill_target_id                              AS skill_target_id,			
        			calendar."Дата" 														AS date,
        			LEFT(calendar."Интервал", 5)                             AS interval,
        			fact_table."Отвеченные вызовы"											AS answered_fact_calls,
        			forecasted_table."Прогноз звонков" 										AS forecasted_calls,
		          	fact_table."Неотвеченные вызовы" + fact_table."Отвеченные вызовы" 		AS fact_calls,
		           	forecasted_table."Прогноз ATT"											AS forecasted_talk_time_att_s,
		           	forecasted_table."Прогноз звонков" 										AS forecasted_calls_att,
		           	fact_table."АТТ"														AS fact_talk_time_att_s,
		           	fact_table."Отвеченные вызовы" 											AS fact_answered_calls_att_s,
		           	fact_table."АCW" 														AS forecasted_talk_time_acw_s,
		           	forecasted_table."Прогноз звонков"										AS forecasted_calls_acw,
		           	forecasted_table."Прогноз постобработки"								AS fact_talk_time_acw_s,
		           	fact_table."Отвеченные вызовы" 											AS fact_answered_calls_acw,
		           	personel_plan."forecasted_resources_m"									AS forecasted_resources_rm,
		           	personel_plan."forecasted_resources_incl_shrinkage_m"					AS forecasted_resources_rm_incl_shrinkage,
		           	personel_plan."scheduled_resources_m"									AS scheduled_resources_rm_plan_mc,
		           	personel_fact."fact_resources_h"										AS scheduled_resources_rm_default,
		           	personel_plan."forecasted_resources_m" 									AS forecasted_resources_absents,
		           	personel_plan."forecasted_resources_incl_shrinkage_m" 					AS forecasted_resources_absents_incl_shrinkage,
		           	fact_table."answered in tst" 											AS answered_calls_within_sl
        FROM        calendar
        FULL OUTER JOIN 	personel_fact
        ON 			calendar.Дата = personel_fact.Дата
        AND			calendar.Интервал = personel_fact.Интервал
        AND            calendar.Навык = personel_fact.Навык
        FULL OUTER join 		fact_table
        ON fact_table.Дата = calendar.Дата
        AND fact_table.Интервал = calendar.Интервал
        AND fact_table.Навык = calendar.Навык
        FULL OUTER JOIN 	forecasted_table
        ON 			calendar.Дата = forecasted_table.Дата
        AND			calendar.Навык = forecasted_table.Навык
        AND			calendar.Интервал = forecasted_table.Интервал
        FULL OUTER JOIN 	personel_plan
        ON 			calendar.Дата = personel_plan.Дата
        AND			calendar.Навык = personel_plan.Навык
        AND			calendar.Интервал = personel_plan.Интервал
        INNER JOIN src
        ON src.skill_id = fact_table.skill_id
'''

drop_q = '''
DELETE FROM mart_final_result
WHERE mrf_target_id = 2
AND "date" >= '%s' '''  % date_start



connect = pyodbc.connect('DRIVER={SQL Server};SERVER=10.160.2.83;DATABASE=TeleoptiAnalytics;UID=ROTeleoptiAnalytics;PWD=ROTeleoptiAnalytics$')
cursor = connect.cursor()
df = pd.read_sql(q, connect)
connect.close()

con_db = psycopg2.connect(
	  database="Teleopti", 
	  user="db_user", 
	  password="2128506",
	  host="172.19.16.124", 
	  port="5432"
	)

cur = con_db.cursor()
cur.execute(drop_q)
con_db.commit()
con_db.close()

engine = create_engine('postgresql+psycopg2://db_user:2128506@172.19.16.124:5432/Teleopti')
df.to_sql('mart_final_result', engine, schema='public', if_exists="append", index = False)

