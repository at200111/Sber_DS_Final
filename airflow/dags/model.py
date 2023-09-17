from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.base import BaseHook

import sys
import psycopg2
import pandas as pd
from datetime import datetime
from catboost import CatBoostRegressor, Pool, metrics
sys.path.append("./src/")
from offer_models import OfferDF
from offer_models import CatBoostModel

def get_conn_credentials(conn_id) -> BaseHook.get_connection:
    return BaseHook.get_connection(conn_id)  

def train_predict():
    conn = get_conn_credentials('cian_db')
    pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn.host, conn.port, conn.login, conn.password, conn.schema
    connection_string = 'postgresql://'+ pg_username + ':' + pg_pass + '@' + pg_hostname + ':' + str(pg_port) + '/' + pg_db

    train = OfferDF(connection_string, sql_path = './src/train.sql')
    train_data = train.read_sql()

    test = OfferDF(connection_string, sql_path = './src/test.sql')
    test_data = test.read_sql()

    train_data = train_data.drop(test_data.index)

    X_train = pd.concat([train_data[train.int_cols], train_data[train.float_cols], train_data[train.bool_cols], train_data[train.cat_cols]], axis=1)
    y_train = train_data[train.target]
    
    X_test = pd.concat([test_data[test.int_cols], test_data[test.float_cols], test_data[test.bool_cols], test_data[test.cat_cols]], axis=1)
    y_test = test_data[test.target]
    
    train_dataset = Pool(X_train, y_train, cat_features=train.cat_cols) 
    cat = CatBoostModel()
    cat.fit(train_dataset)
    cat.predict(X_test, y_test)

with DAG(dag_id="model", start_date=datetime(2023, 1, 1), catchup=False, schedule_interval="*/120 * * * *") as dag:  
    model_op = PythonOperator(task_id=f"train_predict", python_callable=train_predict, do_xcom_push=False)  