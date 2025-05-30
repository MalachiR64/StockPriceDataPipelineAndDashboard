from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.stockPriceUpdater import main as stockPriceUpdater_main

def stock_price_updater():
    stockPriceUpdater_main()  # Call your stockPriceUpdater.py script

default_args = {
    'owner': 'airflow',
    'retries': 1,
    "schedule_interval": '@hourly', 
    'retry_delay': timedelta(minutes=5),
    'start_date':datetime(2024, 1, 1)
    
    }

with DAG(
    dag_id='hourly_stock_price_update',
    default_args=default_args,
    
) as dag:
    task1 = PythonOperator(
        task_id='update_stock_prices',
        python_callable=stock_price_updater,
    )

task1