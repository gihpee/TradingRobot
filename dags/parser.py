from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import requests
import psycopg2
import datetime

conn = psycopg2.connect(host='db', user='root', password='root', database='data', port='5432')
cur = conn.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS BTCUSDT(
    timestamp VARCHAR,
    type VARCHAR,
    price REAL,
    quantity REAL
)""")

cur.execute("""CREATE TABLE IF NOT EXISTS ETHUSDT(
    timestamp VARCHAR,
    type VARCHAR,
    price REAL,
    quantity REAL
)""")

cur.execute("""CREATE TABLE IF NOT EXISTS LTCUSDT(
    timestamp VARCHAR,
    type VARCHAR,
    price REAL,
    quantity REAL
)""")

cur.execute("""CREATE TABLE IF NOT EXISTS BNBUSDT(
    timestamp VARCHAR,
    type VARCHAR,
    price REAL,
    quantity REAL
)""")

cur.execute("""CREATE TABLE IF NOT EXISTS MATICUSDT(
    timestamp VARCHAR,
    type VARCHAR,
    price REAL,
    quantity REAL
)""")

cur.execute("""CREATE TABLE IF NOT EXISTS DOTUSDT(
    timestamp VARCHAR,
    type VARCHAR,
    price REAL,
    quantity REAL
)""")

cur.execute("""CREATE TABLE IF NOT EXISTS SOLUSDT(
    timestamp VARCHAR,
    type VARCHAR,
    price REAL,
    quantity REAL
)""")

cur.execute("""CREATE TABLE IF NOT EXISTS LINKUSDT(
    timestamp VARCHAR,
    type VARCHAR,
    price REAL,
    quantity REAL
)""")

cur.execute("""CREATE TABLE IF NOT EXISTS XRPUSDT(
    timestamp VARCHAR,
    type VARCHAR,
    price REAL,
    quantity REAL
)""")

cur.execute("""CREATE TABLE IF NOT EXISTS AVAXUSDT(
    timestamp VARCHAR,
    type VARCHAR,
    price REAL,
    quantity REAL
)""")
conn.commit()

def parser():
    try:
        symbols = ['BTCUSDT', 'ETHUSDT', 'LTCUSDT', 'BNBUSDT', 'MATICUSDT', 'DOTUSDT', 'SOLUSDT', 'LINKUSDT', 'XRPUSDT', 'AVAXUSDT']

        for symbol in symbols:
            url = f'https://api.binance.com/api/v3/depth?symbol={symbol}&limit=10'

            response = requests.get(url).json()

            timestamp = str(datetime.datetime.now())

            bids = response['bids']
            asks = response['asks']

            sorted_bids = sorted(bids, key=lambda x: float(x[0]), reverse=True)
            sorted_asks = sorted(asks, key=lambda x: float(x[0]))

            best_bids = bids[:25]
            best_asks = asks[:25]

            for el in best_bids:
                cur.execute(f"INSERT INTO {symbol} VALUES('{timestamp}', 'bid', '{el[0]}', '{el[1]}')")
                conn.commit()

            for el in best_asks:
                cur.execute(f"INSERT INTO {symbol} VALUES('{timestamp}', 'ask', '{el[0]}', '{el[1]}')")
                conn.commit()

    except Exception as e:
        error = str(e)
        print(error)


parser_dag = DAG(
    "parser",
    description='parser',
    schedule_interval="*/1 * * * *",
    start_date=days_ago(0, 0, 0, 0, 0),
    tags=['parser'],
    doc_md='parser'
)

parser_operator = PythonOperator(
    task_id='parser',
    python_callable=parser,
    dag=parser_dag
)

parser_operator