from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from scripts.captura_dados import captura_dados
from scripts.processamento import processar_dados
from scripts.limpeza_dados import limpar_e_validar_dados
from scripts.gerar_relatorio import gerar_relatorio
from scripts.descompactar_zip import descompactar_zip

def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)

dag = DAG(
    'atracacoes_e_cargas',
    start_date=datetime(2025, 2, 15),
    schedule_interval='@montly',
    catchup=False
)

descompactacao = TaskGroup("group_descompactacao", dag=dag)
upload_aws_s3 = TaskGroup("group_upload_aws_s3", dag=dag)

task_descompactar_arquivo_2021 = PythonOperator(
    task_id='descompactar_arquivo_2021',
    python_callable=descompactar_zip,
    op_args=[2021],
    task_group=descompactacao,
    dag=dag 
)

task_descompactar_arquivo_2022 = PythonOperator(
    task_id='descompactar_arquivo_2022',
    python_callable=descompactar_zip,
    op_args=[2022],
    task_group=descompactacao,
    dag=dag 
)

task_descompactar_arquivo_2023 = PythonOperator(
    task_id='descompactar_arquivo_2023',
    python_callable=descompactar_zip,
    op_args=[2023],
    task_group=descompactacao,
    dag=dag 
)

task_captura_dados = PythonOperator(
    task_id='captura_dados',
    python_callable=captura_dados,
    dag=dag
)

task_processamento_de_dados = PythonOperator(
    task_id='processar_dados',
    python_callable=processar_dados,
    dag=dag
)

task_limpar_e_validar_dados = PythonOperator(
    task_id='limpar_e_validar',
    python_callable=limpar_e_validar_dados,
    dag=dag
)

task_gerar_relatorio = PythonOperator(
    task_id='gerar_relatorio',
    python_callable=gerar_relatorio,
    dag=dag
)

task_upload_atracacao_to_s3 = PythonOperator(
        task_id='upload_atracacao_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': '/usr/local/airflow/dags/datalake/business/atracacao.parquet',
            'key': 'dags/datalake/business/atracacao.parquet',
            'bucket_name': 'bucket-fiec-2'
        },
        task_group=upload_aws_s3,
        dag=dag
)

task_upload_carga_to_s3 = PythonOperator(
        task_id='upload_carga_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': '/usr/local/airflow/dags/datalake/business/carga.parquet',
            'key': 'dags/datalake/business/carga.parquet',
            'bucket_name': 'bucket-fiec-2'
        },
        task_group=upload_aws_s3,
        dag=dag
)

descompactacao >> task_captura_dados
task_captura_dados >> task_processamento_de_dados >> task_limpar_e_validar_dados >> task_gerar_relatorio
task_gerar_relatorio >> upload_aws_s3