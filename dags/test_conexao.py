from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def log_query_result(*args, **kwargs):
    result = kwargs['ti'].xcom_pull(task_ids='test_connection')
    print(f"Query Result: {result}")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 26),
}

with DAG("teste_mssql_rds", default_args=default_args, schedule_interval=None, catchup=False) as dag:
    test_query = MsSqlOperator(
        task_id="test_connection",
        mssql_conn_id="mssql_aws_rds",
        sql="USE atracacao_e_carga; SELECT 'Hello, World!';",
        do_xcom_push=True,
    )

    log_result = PythonOperator(
        task_id='log_result',
        python_callable=log_query_result,
        provide_context=True,
    )

    test_query >> log_result
