from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.base import BaseHook

import sys
import psycopg2
from datetime import datetime
sys.path.append("./src/")
from offer_parser import OfferParser


def get_conn_credentials(conn_id) -> BaseHook.get_connection:
    return BaseHook.get_connection(conn_id)  

def parse_data(**kwargs):
    conn = get_conn_credentials('cian_db')
    pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn.host, conn.port, conn.login, conn.password, conn.schema
    connection_string = 'postgresql://'+ pg_username + ':' + pg_pass + '@' + pg_hostname + ':' + str(pg_port) + '/' + pg_db
    offers = OfferParser()    
    offers.parse_from_dir(limit=1000)            
    offers.dump_to_db(connection_string)

def prepare(prepare_sql):
    conn = get_conn_credentials('cian_db')
    pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn.host, conn.port, conn.login, conn.password, conn.schema
    pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)
    
    try:
        cursor = pg_conn.cursor()            
        cursor.execute(prepare_sql)
        pg_conn.commit()
        cursor.close()
    finally:            
        pg_conn.close()

def prepare_offers_detail(**kwargs):
    prepare_sql = '''
        merge into offers_detail master
        using (select * from offer_temp) as child
        on master.offer_id = child.id and master.prop = child.prop
        when matched then update set val = child.val
        when not matched then
        insert (offer_id, prop, val)
        values (child.id, child.prop, child.val);'''
    
    prepare(prepare_sql)

def prepare_offers_detail_bti(**kwargs):
    prepare_sql = '''
        merge into offers_detail master
        using (select * from bti_temp) as child
        on master.offer_id = child.id and master.prop = child.prop
        when matched then update set val = child.val
        when not matched then
        insert (offer_id, prop, val)
        values (child.id, child.prop, child.val);'''    
    
    prepare(prepare_sql)
    
def prepare_offers(**kwargs):
    prepare_sql = '''
        merge into offers master
        using (select id from offer_temp group by id) as child
        on master.offer_id = child.id
        when matched then update set modifydate = current_timestamp
        when not matched then
        insert (offer_id, modifydate)
        values (child.id, current_timestamp);'''        
    
    prepare(prepare_sql)
  

with DAG(dag_id="process", start_date=datetime(2023, 1, 1), catchup=False, schedule_interval="*/10 * * * *") as dag:
    parse_op = PythonOperator(task_id=f"parse_data", python_callable=parse_data, do_xcom_push=False)
    prepare_offers_detail_op = PythonOperator(task_id=f"prepare_offers_detail", python_callable=prepare_offers_detail, do_xcom_push=False)
    prepare_offers_detail_bti_op = PythonOperator(task_id=f"prepare_offers_detail_bti", python_callable=prepare_offers_detail_bti, do_xcom_push=False)
    prepare_offers_op = PythonOperator(task_id=f"prepare_offers", python_callable=prepare_offers, do_xcom_push=False)

    parse_op >> prepare_offers_detail_op >> prepare_offers_detail_bti_op >> prepare_offers_op
    