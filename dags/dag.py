from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator

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

# send_email = TaskGroup("group_send_email", dag=dag)
descompactacao = TaskGroup("group_descompactacao", dag=dag)
group_database = TaskGroup('group_database', dag=dag)

task_descompactar_arquivo_2021 = PythonOperator(
    task_id='descompactar_arquivo_2021',
    python_callable=descompactar_zip,
    op_args=[2021],  # Passando o ano 2021 como argumento
    task_group=descompactacao,
    dag=dag 
)

task_descompactar_arquivo_2022 = PythonOperator(
    task_id='descompactar_arquivo_2022',
    python_callable=descompactar_zip,
    op_args=[2022],  # Passando o ano 2022 como argumento
    task_group=descompactacao,
    dag=dag 
)

task_descompactar_arquivo_2023 = PythonOperator(
    task_id='descompactar_arquivo_2023',
    python_callable=descompactar_zip,
    op_args=[2023],  # Passando o ano 2023 como argumento
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

create_atracacao_table = PostgresOperator(
    task_id="create_atracacao_table",
    postgres_conn_id='postgres',
    sql='''
        CREATE TABLE IF NOT EXISTS atracacao (
            IDAtracacao VARCHAR PRIMARY KEY,
            "Tipo de Navegação da Atracação" VARCHAR,
            CDTUP VARCHAR,
            "Nacionalidade do Armador" VARCHAR,
            IDBerco VARCHAR,
            "FlagMCOperacaoAtracacao" VARCHAR,
            Berço VARCHAR,
            Terminal VARCHAR,
            "Porto Atracação" VARCHAR,
            Município VARCHAR,
            "Apelido Instalação Portuária" VARCHAR,
            UF VARCHAR,
            "Complexo Portuário" VARCHAR,
            SGUF VARCHAR,
            "Tipo da Autoridade Portuária" VARCHAR,
            "Região Geográfica" VARCHAR,
            "Data Atracação" DATE,
            "No da Capitania" VARCHAR,
            "Data Chegada" DATE,
            "No do IMO" VARCHAR,
            "Data Desatracação" DATE,
            "TEsperaAtracacao" INT,
            "Data Início Operação" DATE,
            "TEsperaInicioOp" INT,
            "Data Término Operação" DATE,
            "TOperacao" INT,
            "Ano da data de início da operação" INT,
            "TEsperaDesatracacao" INT,
            "Mês da data de início da operação" INT,
            "TAtracado" INT,
            "Tipo de Operação" VARCHAR,
            "TEstadia" INT
        );
    ''',
    task_group=group_database,
    dag=dag
)

create_carga_table = PostgresOperator(
    task_id="create_carga_table",
    postgres_conn_id='postgres',
    sql='''
        CREATE TABLE IF NOT EXISTS carga (
            IDCarga VARCHAR PRIMARY KEY,
            "FlagTransporteViaInterioir" VARCHAR,
            IDAtracacao VARCHAR,
            "Percurso Transporte em vias Interiores" VARCHAR,
            Origem VARCHAR,
            "Percurso Transporte Interiores" VARCHAR,
            Destino VARCHAR,
            "STNaturezaCarga" VARCHAR,
            CDMercadoria VARCHAR,
            "STSH2" VARCHAR,
            "Tipo Operação da Carga" VARCHAR,
            "STSH4" VARCHAR,
            "Carga Geral Acondicionamento" VARCHAR,
            "Natureza da Carga" VARCHAR,
            "ConteinerEstado" VARCHAR,
            Sentido VARCHAR,
            "Tipo Navegação" VARCHAR,
            TEU INT,
            "FlagAutorizacao" VARCHAR,
            QTCarga INT,
            "FlagCabotagem" VARCHAR,
            "VLPesoCargaBruta" INT,
            "FlagCabotagemMovimentacao" VARCHAR,
            "Ano da data de início da operação da atracação" INT,
            "FlagConteinerTamanho" VARCHAR,
            "Mês da data de início da operação da atracação" INT,
            "FlagLongoCurso" VARCHAR,
            "Porto Atracação" VARCHAR,
            "FlagMCOperacaoCarga" VARCHAR,
            SGUF VARCHAR,
            "FlagOffshore" VARCHAR,
            "Peso líquido da carga" INT
        );
    ''',
    task_group=group_database,
    dag=dag
)

insert_atracacao_data = PostgresOperator(
    task_id="insert_atracacao_data",
    postgres_conn_id='postgres',
    sql='''
        INSERT INTO atracacao (
            IDAtracacao, "Tipo de Navegação da Atracação", CDTUP, "Nacionalidade do Armador",
            IDBerco, "FlagMCOperacaoAtracacao", Berço, Terminal, "Porto Atracação", Município,
            "Apelido Instalação Portuária", UF, "Complexo Portuário", SGUF, "Tipo da Autoridade Portuária",
            "Região Geográfica", "Data Atracação", "No da Capitania", "Data Chegada", "No do IMO", 
            "Data Desatracação", "TEsperaAtracacao", "Data Início Operação", "TEsperaInicioOp", 
            "Data Término Operação", "TOperacao", "Ano da data de início da operação", 
            "TEsperaDesatracacao", "Mês da data de início da operação", "TAtracado", "Tipo de Operação", 
            "TEstadia"
        ) VALUES 
        (
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_id') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_tipo_navegacao') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_cdtu') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_nacionalidade') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_idberco') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_flagmc_operacao') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_berco') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_terminal') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_porto') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_municipio') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_apelido_instalacao') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_uf') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_complexo_portuario') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_sguf') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_tipo_autoridade') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_regiao_geografica') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_data_atracacao') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_no_capitania') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_data_chegada') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_no_imo') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_data_desatracacao') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_tespera_atracacao') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_data_inicio_operacao') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_tespera_inicioop') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_data_termino_operacao') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_toperacao') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_ano_inicio_operacao') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_tespera_desatracacao') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_mes_inicio_operacao') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_tatracado') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_tipo_operacao') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='atracacao_testadia') }}
        );
    ''',
    task_group=group_database,
    dag=dag
)

insert_carga_data = PostgresOperator(
    task_id="insert_carga_data",
    postgres_conn_id='postgres',
    sql='''
        INSERT INTO carga (
            IDCarga, "FlagTransporteViaInterioir", IDAtracacao, "Percurso Transporte em vias Interiores", 
            Origem, "Percurso Transporte Interiores", Destino, "STNaturezaCarga", CDMercadoria, "STSH2",
            "Tipo Operação da Carga", "STSH4", "Carga Geral Acondicionamento", "Natureza da Carga", 
            "ConteinerEstado", Sentido, "Tipo Navegação", TEU, "FlagAutorizacao", QTCarga, 
            "FlagCabotagem", "VLPesoCargaBruta", "FlagCabotagemMovimentacao", 
            "Ano da data de início da operação da atracação", "FlagConteinerTamanho", 
            "Mês da data de início da operação da atracação", "FlagLongoCurso", "Porto Atracação", 
            "FlagMCOperacaoCarga", SGUF, "FlagOffshore", "Peso líquido da carga"
        ) VALUES 
        (
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_id') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_flag') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_id_atracacao') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_percurso_transporte_interiores') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_origem') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_percurso_transporte_vias_interiores') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_destino') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_stnaturezacarga') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_cdmercadoria') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_stsh2') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_tipo_operacao_carga') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_stsh4') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_carga_geral_acondicionamento') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_natureza_da_carga') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_conteiner_estado') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_sentido') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_tipo_navegacao') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_teu') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_flagautorizacao') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_qtcarga') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_flagcabotagem') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_vlpesocargabruta') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_flagcabotagemmovimentacao') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_ano_data_inicio_operacao') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_flagconteinersize') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_mes_inicio_operacao') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_flaglongocurso') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_porto_atracacao') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_flagmcoperacaocarga') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_sguf') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_flagoffshore') }},
            {{ ti.xcom_pull(task_ids='gerar_relatorio', key='carga_peso_liquido') }}
        );
    ''',
    task_group=group_database,
    dag=dag
)

descompactacao >> task_captura_dados
task_captura_dados >> task_processamento_de_dados >> task_limpar_e_validar_dados >> task_gerar_relatorio
task_gerar_relatorio >> group_database