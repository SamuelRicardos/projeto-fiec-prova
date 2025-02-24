from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd
import unidecode

from scripts.captura_dados import captura_dados
from scripts.processamento import processar_dados
from scripts.limpeza_dados import limpar_e_validar_dados
from scripts.gerar_relatorio import gerar_relatorio
from scripts.descompactar_zip import descompactar_zip

def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)

def formatar_nome_coluna(col):
    col = unidecode.unidecode(col)
    
    col_adjusted = col.replace(" de ", "_").replace(" em ", "_").replace(" para ", "_").replace(" da ", "_").replace(" do ", "_")
    
    col_adjusted = col_adjusted.replace(" ", "_")
    
    col_adjusted = ''.join(c if c.isalnum() or c == "_" else "_" for c in col_adjusted)
    
    col_adjusted = col_adjusted.strip('_')

    return col_adjusted

def criar_tabela_sql(df: pd.DataFrame, tabela: str, hook: MsSqlHook) -> None:
    create_table_query = f"CREATE TABLE [{tabela}] ("

    for col, dtype in zip(df.columns, df.dtypes):

        col_adjusted = formatar_nome_coluna(col)
        
        col_escaped = f"[{col_adjusted}]"
        
        if dtype == 'object':
            column_type = 'VARCHAR(MAX)'
        elif dtype == 'int64':
            column_type = 'INT'
        elif dtype == 'float64':
            column_type = 'FLOAT'
        elif dtype == 'datetime64[ns]':
            column_type = 'DATETIME'
        else:
            column_type = 'VARCHAR(MAX)'

        create_table_query += f"{col_escaped} {column_type}, "

    create_table_query = create_table_query.rstrip(", ") + ")"
    hook.run(create_table_query)
    print(f"Tabela [{tabela}] criada com sucesso!")

def inserir_parquet_sql_server(parquet_file: str, tabela: str) -> None:
    df = pd.read_parquet(parquet_file)
    hook = MsSqlHook(mssql_conn_id='mssql_aws_rds')

    table_exists_query = f"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tabela}'"
    result = hook.get_first(table_exists_query)

    if not result:
        print(f"Tabela [{tabela}] não existe. Criando a tabela...")
        criar_tabela_sql(df, tabela, hook)

    df.columns = [formatar_nome_coluna(col) for col in df.columns]

    hook.insert_rows(table=tabela, rows=df.values.tolist(), target_fields=df.columns.tolist())
    print(f"Dados do arquivo Parquet inseridos na tabela [{tabela}] com sucesso!")

def gerar_relatorio_e_inserir_sql(parquet_file: str):
    tabela_destino_atracacao = "atracacao_fato"
    tabela_destino_carga = "carga_fato"
    
    inserir_parquet_sql_server(parquet_file, tabela_destino_atracacao)
    inserir_parquet_sql_server(parquet_file, tabela_destino_carga)

dag = DAG(
    'atracacoes_e_cargas',
    start_date=datetime(2025, 2, 15),
    schedule_interval='@monthly',
    catchup=False
)

descompactacao = TaskGroup("group_descompactacao", dag=dag)
upload_aws_s3 = TaskGroup("group_upload_aws_s3", dag=dag)
inserir_aws_rds = TaskGroup("group_upload_aws_rds", dag=dag)

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

task_inserir_atracacao_aws_sql = PythonOperator(
    task_id='inserir_atracacao_sql',
    python_callable=gerar_relatorio_e_inserir_sql,
    op_args=['/usr/local/airflow/dags/datalake/business/atracacao.parquet'],
    task_group=inserir_aws_rds,
    dag=dag
)

task_inserir_carga_aws_sql = PythonOperator(
    task_id='inserir_carga_sql',
    python_callable=gerar_relatorio_e_inserir_sql,
    op_args=['/usr/local/airflow/dags/datalake/business/carga.parquet'],
    task_group=inserir_aws_rds,
    dag=dag
)

descompactacao >> task_captura_dados
task_captura_dados >> task_processamento_de_dados >> task_limpar_e_validar_dados >> task_gerar_relatorio
task_gerar_relatorio >> [upload_aws_s3,  inserir_aws_rds]