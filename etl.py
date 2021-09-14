from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
from google.cloud import bigquery
import pandas as pd
import requests
import http.client
import ast

class Config:
    dataset_id = os.environ.get("dataset_id")
    table_name = os.environ.get("table_name")
    url_bitcion = os.environ.get("url_bitcoin")
    token_gold = os.environ.get("token_gold")
    conversion_rate_url = os.environ.get("conversion_rate_url")

'''bitcoin_data_output_path = "/home/airflow/gcs/data/bitcoin_data.csv"
gold_data_output_path = "/home/airflow/gcs/data/gold_data.csv"
conversion_rate_output_path = "/home/airflow/gcs/data/conversion_rate.csv"
final_output_path = "/home/airflow/gcs/data/output.csv"'''


df = pd.DataFrame([])
gold_data = 0
conversion_rates = 0 

def get_bitcoin_data(url_bitcion):
    ###
    print('check url_bitcion {}'.format(url_bitcion))
    response = requests.get(url_bitcion)
    if response.status_code == 200:
        # This means something went wrong.
        jsondata = response.json()
        #print(jsondata)
        data={'Date':jsondata.get('time').get('updatedISO') ,'BTC_USD_rate':jsondata.get('bpi').get('USD').get('rate_float'),'BTC_THB_rate':jsondata.get('bpi').get('THB').get('rate_float')}
        global df
        df = pd.DataFrame(data=data,index=[1])
        df['Date'] = pd.to_datetime(df['Date'])
        print('BTC Dataframe')
        print(df)
    else:
        raise ValueError('response status {}'.format(response.status_code))

def get_gold_data(token_gold):
    ###
    print('check token_gold '+token_gold)
    conn = http.client.HTTPSConnection("www.goldapi.io")
    payload = ''
    headers = {
        'x-access-token': ''+token_gold,
        'Content-Type': 'application/json'
    }
    conn.request("GET", "/api/XAU/USD", payload, headers)
    response = conn.getresponse()
    if response.status == 200:
        
        data = response.read()
        data_dic = ast.literal_eval(data.decode("utf-8"))
        global gold_data
        gold_data = float(data_dic.get("price"))
        ###
        print('Gold price {}'.format(gold_data))

    else:
        raise ValueError('response status {} reason {}'.format(response.status, response.reason))    
    
def get_conversion_rates(conversion_rate_url):
    ###
    print("check conversion_rate_url {}".format(conversion_rate_url))
    response = requests.get(conversion_rate_url)
    if response.status_code == 200:
        jsondata = response.json()
        global conversion_rates
        conversion_rates = float(jsondata.get('conversion_rates').get('THB'))
        ##
        print("check conversion_rate {}".format(conversion_rates))
    else:
        raise ValueError('response status {}'.format(response.status_code))

def merge_data():
    #จำเป็นต้องส่งออกเป็น csv
    #ทำเป็น cloud func น่าจะดีกว่า ?
    #เขียนที่ ram มีค่าใช้จ่าย? 
    global df
    global gold_data
    global conversion_rates
    df['XAU_USD_rate'] =  gold_data
    df['XAU_THB_rate'] =  gold_data*conversion_rates
    print('df na ja')
    print(df)



def store_to_data_warehouse():

    client = bigquery.Client()
    dataset_ref = client.dataset(Config.dataset_id)
    global df
    #raw = get_data(Config.url)
    record = [(
        df['Date'],
        df['BTC_USD_rate'],
        df['BTC_THB_rate'], 
        df['XAU_USD_rate'], 
        df['XAU_THB_rate']
        )]
    
    table_ref = dataset_ref.table(Config.table_name)
    table = client.get_table(table_ref)
    result = client.insert_rows(table, record)
    print("result na ja")
    print(result)
    return result


default_args = {
    'owner': 'Patcharajak',
    'email': ['france.2551@hotmail.com'],
    'email_on_failure': True,
    'retries': 0,

}

with DAG(
    "demo_etl_btc_gold",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@hourly",
    tags=["demo"]
) as dag:

    
    t1 = PythonOperator(
        task_id="get_bitcoin_data",
        python_callable=get_bitcoin_data,
        op_kwargs={
            "url_bitcion": Config.url_bitcion,
        },
    )
    t2 = PythonOperator(
        task_id="get_gold_data",
        python_callable=get_gold_data,
        op_kwargs={
            "token_gold": Config.token_gold,
        },
    )

    t3 = PythonOperator(
        task_id="get_conversion_rates",
        python_callable=get_conversion_rates,
        op_kwargs={
            "conversion_rate_url": Config.conversion_rate_url,
        },
    )

    t4 = PythonOperator(
        task_id="merge_data",
        python_callable=merge_data,
        )

    t5 = PythonOperator(
        task_id="store_to_data_warehouse",
        python_callable=store_to_data_warehouse,
    )

    [t1, t2, t3] >> t4 >> t5
