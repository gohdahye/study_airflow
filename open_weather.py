from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import logging
import psycopg2

def get_Redshift_connection():
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    user = "kdhnate222"  # 본인 ID 사용
    password = "Kdhnate222!1"  # 본인 Password 사용
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect(f"dbname={dbname} user={user} host={host} password={password} port={port}")
    conn.set_session(autocommit=True)
    return conn.cursor()


def extract(url):
    logging.info("Extract started")
    f = requests.get(url)
    f_js = f.json()
    logging.info("Extract done")
    return (f_js)


def transform(text):
    logging.info("transform started")
    # ignore the first line - header
    lines = text["list"]
    logging.info("transform done")
    return lines

def load(lines):
    cur = get_Redshift_connection()
    sql = "BEGIN;DELETE FROM kdhnate222.weather_forecast;"
    for r in lines:
            day = datetime.fromtimestamp(r["dt"]).strftime('%Y-%m-%d')
            min = r["main"]["temp_min"]
            max = r["main"]["temp_max"]
            sql += f"INSERT INTO kdhnate222.weather_forecast VALUES ('{day}', '{min}', '{max}');"
    sql += "END;"
    cur.execute(sql)
    logging.info(sql)
    logging.info("load done")


def etl():
    link = "http://api.openweathermap.org/data/2.5/forecast?lat=37.541&lon=126.986&units=metric&appid=7ffcfc194c9aca441bad340129790056"
    data = extract(link)
    lines = transform(data)
    load(lines)


dag_open_weather = DAG(
	dag_id = 'open_weather_assignment',
	catchup = False,
	start_date = datetime(2022,8,20), # 날짜가 미래인 경우 실행이 안됨
	schedule_interval = '0 2 * * *')

task = PythonOperator(
	task_id = 'perform_etl',
	python_callable = etl,
	dag = dag_open_weather)