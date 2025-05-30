from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.tickergenerator import main as tickergenerator_main
from scripts.main import main as populate_main
from scripts.uploadToAzureBlobAndSQL import main as upload_main


def ticker_generator():
    tickergenerator_main()  # Call your stockPriceUpdater.py script

def api_call():
    populate_main()  # Call your tickergenerator.py script

def upload_to_azure():
    upload_main()  # Call your uploadToAzureBlobAndSQL.py script
# Define the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    "schedule_interval" :'@monthly', 
    'retry_delay': timedelta(minutes=5),
    'start_date':datetime(2025, 5, 1)
    
    }

with DAG(
    dag_id='monthly_stock_update',
    default_args=default_args,
    
) as dag:

    task1 = PythonOperator(
        task_id='create_tickers_and_files',
        python_callable=ticker_generator,
    )

    task2 = PythonOperator(
        task_id='populate_json_and_csv',
        python_callable=api_call,
    )

    task3 = PythonOperator(
        task_id='upload_to_azure',
        python_callable=upload_to_azure,
    )



# Set task dependencies
task1 >> task2 >> task3
