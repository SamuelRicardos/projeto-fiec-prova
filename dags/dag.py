from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.utils.task_group import TaskGroup

from scripts.captura_dados import captura_dados
from scripts.processamento import processar_dados
from scripts.limpeza_dados import limpar_e_validar_dados
from scripts.gerar_relatorio import gerar_relatorio
from scripts.descompactar_zip import descompactar_zip


dag = DAG(
    'atracacoes_e_cargas',
    start_date=datetime(2025, 2, 15),
    schedule_interval=None,
    catchup=False
)

descompactacao = TaskGroup("group_descompactacao", dag=dag)

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

descompactacao >> task_captura_dados
task_captura_dados >> task_processamento_de_dados >> task_limpar_e_validar_dados >> task_gerar_relatorio