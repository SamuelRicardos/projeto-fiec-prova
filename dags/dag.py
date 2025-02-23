from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.email_operator import EmailOperator

from scripts.captura_dados import captura_dados
from scripts.processamento import processar_dados
from scripts.limpeza_dados import limpar_e_validar_dados
from scripts.gerar_relatorio import gerar_relatorio
from scripts.descompactar_zip import descompactar_zip_2021
from scripts.descompactar_zip import descompactar_zip_2022
from scripts.descompactar_zip import descompactar_zip_2023


dag = DAG(
    'atracacoes_e_cargas',
    start_date=datetime(2025, 2, 15),
    schedule_interval=None,
    catchup=False
)

send_email = TaskGroup("group_send_email", dag=dag)
descompactacao = TaskGroup("group_descompactacao", dag=dag)

task_descompactar_arquivo_2021 = PythonOperator(
    task_id='descompactar_arquivo_2021',
    python_callable=descompactar_zip_2021,
    task_group=descompactacao,
    dag=dag 
)

task_descompactar_arquivo_2022 = PythonOperator(
    task_id='descompactar_arquivo_2022',
    python_callable=descompactar_zip_2022,
    task_group=descompactacao,
    dag=dag 
)

task_descompactar_arquivo_2023 = PythonOperator(
    task_id='descompactar_arquivo_2023',
    python_callable=descompactar_zip_2023,
    task_group=descompactacao,
    dag=dag 
)


send_email_alert = EmailOperator(
    task_id='send_email_alert',
    to='samuelric4rdo@gmail.com',
    subject='🚨 Alerta: Erro no Relatório da DAG {{ dag.dag_id }}',
    html_content="""
    <div style="font-family: Arial, sans-serif; border: 1px solid #d9534f; padding: 16px; border-radius: 8px;">
        <h2 style="color: #d9534f;">⚠️ Erro ao Gerar Relatório</h2>
        <p style="font-size: 16px;">Houve um erro durante a execução da DAG: <strong>{{ dag.dag_id }}</strong></p>
        <p>Por favor, verifique os logs no Airflow para mais detalhes.</p>
        <hr>
        <p style="font-size: 14px; color: #555;">📅 Data do erro: <strong>{{ ts }}</strong></p>
        <p style="font-size: 14px; color: #555;">🔁 Tentativa: <strong>{{ try_number }}</strong></p>
    </div>
    """,
    task_group=send_email,
    trigger_rule="one_failed",
    dag=dag
)

send_email_success = EmailOperator(
    task_id='send_email_success',
    to='samuelric4rdo@gmail.com',
    subject='✅ Sucesso: Relatório Gerado pela DAG {{ dag.dag_id }}',
    html_content="""
    <div style="font-family: Arial, sans-serif; border: 1px solid #28a745; padding: 16px; border-radius: 8px;">
        <h2 style="color: #28a745;">🎉 Relatório Gerado com Sucesso!</h2>
        <p style="font-size: 16px;">A DAG <strong>{{ dag.dag_id }}</strong> foi executada com sucesso e o relatório foi gerado.</p>
        <p>Você pode verificar os resultados diretamente no ambiente do Airflow.</p>
        <hr>
        <p style="font-size: 14px; color: #555;">📅 Data de execução: <strong>{{ ts }}</strong></p>
        <p style="font-size: 14px; color: #555;">🔁 Execução número: <strong>{{ dag_run.run_id }}</strong></p>
    </div>
    """,
    task_group=send_email,
    trigger_rule="all_success",
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
task_gerar_relatorio >> send_email