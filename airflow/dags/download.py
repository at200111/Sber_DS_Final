from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.base import BaseHook

from datetime import datetime
import sys
sys.path.append("./src/")
from offer_scraper import OfferScraper

def dowload_raw_data(**kwargs):
    scraper = OfferScraper()
    scraper.full_scrap(search_depth='day')

    scraper.finish_scrap()

with DAG(dag_id="full_download", start_date=datetime(2023, 1, 1), catchup=False, schedule_interval="*/60 * * * *") as dag:
    download_op = PythonOperator(task_id=f"dowload_raw_data", python_callable=dowload_raw_data, do_xcom_push=False)