import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Inicializa o SparkSession
spark = SparkSession.builder \
    .appName("Gerar Relatório de Dados com Spark e Pandas") \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .getOrCreate()

# Caminhos dos diretórios
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../datalake'))
TRUSTED_PATH = os.path.join(BASE_PATH, 'trusted')
BUSINESS_PATH = os.path.join(BASE_PATH, 'business')

os.makedirs(BUSINESS_PATH, exist_ok=True)

# Definindo as colunas para cada tabela
atracacao_cols = [
    'IDAtracacao', 'Tipo de Navegação da Atracação', 'CDTUP', 'Nacionalidade do Armador',
    'IDBerco', 'FlagMCOperacaoAtracacao', 'Berço', 'Terminal', 'Porto Atracação', 'Município',
    'Apelido Instalação Portuária', 'UF', 'Complexo Portuário', 'SGUF',
    'Tipo da Autoridade Portuária', 'Região Geográfica', 'Data Atracação', 'No da Capitania',
    'Data Chegada', 'No do IMO', 'Data Desatracação', 'TEsperaAtracacao', 'Data Início Operação',
    'TEsperaInicioOp', 'Data Término Operação', 'TOperacao', 'Ano da data de início da operação',
    'TEsperaDesatracacao', 'Mês da data de início da operação', 'TAtracado', 'Tipo de Operação',
    'TEstadia'
]

carga_cols = [
    'IDCarga', 'FlagTransporteViaInterioir', 'IDAtracacao', 'Percurso Transporte em vias Interiores',
    'Origem', 'Percurso Transporte Interiores', 'Destino', 'STNaturezaCarga', 'CDMercadoria', 
    'STSH2', 'Tipo Operação da Carga', 'STSH4', 'Carga Geral Acondicionamento', 'Natureza da Carga',
    'ConteinerEstado', 'Sentido', 'Tipo Navegação', 'TEU', 'FlagAutorizacao', 'QTCarga',
    'FlagCabotagem', 'VLPesoCargaBruta', 'FlagCabotagemMovimentacao', 'Ano da data de início da operação da atracação',
    'FlagConteinerTamanho', 'Mês da data de início da operação da atracação', 'FlagLongoCurso', 
    'Porto Atracação', 'FlagMCOperacaoCarga', 'SGUF', 'FlagOffshore', 'Peso líquido da carga'
]

def gerar_relatorio():
    dfs_atracacao = []
    dfs_carga = []

    for arquivo in os.listdir(TRUSTED_PATH):
        if arquivo.endswith('.parquet'):
            trusted_path = os.path.join(TRUSTED_PATH, arquivo)

            print(f'🔍 Processando {arquivo}...')

            try:
                # Lê o arquivo Parquet com Spark
                df_spark = spark.read.parquet(trusted_path)

                # Filtra as colunas para Atracacao ou Carga e processa
                if 'IDAtracacao' in df_spark.columns and 'Tipo de Navegação da Atracação' in df_spark.columns:
                    df_atracacao = df_spark.select([col(c) for c in atracacao_cols if c in df_spark.columns])
                    dfs_atracacao.append(df_atracacao)
                elif 'IDCarga' in df_spark.columns and 'FlagTransporteViaInterioir' in df_spark.columns:
                    df_carga = df_spark.select([col(c) for c in carga_cols if c in df_spark.columns])
                    dfs_carga.append(df_carga)
                else:
                    print(f'⚠️ Estrutura desconhecida em {arquivo}, ignorando.')
                    continue

                # Consolidando os dados da tabela de Atracacao
                if dfs_atracacao:
                    df_atracacao_final = dfs_atracacao[0]
                    for df in dfs_atracacao[1:]:
                        df_atracacao_final = df_atracacao_final.union(df)

                    # Salva na camada business
                    df_atracacao_final_pd = df_atracacao_final.toPandas()
                    business_file_path = os.path.join(BUSINESS_PATH, 'atracacao.parquet')
                    df_atracacao_final_pd.to_parquet(business_file_path, index=False, engine='pyarrow')
                    print(f'✅ Arquivo consolidado "atracacao.parquet" salvo na camada business.')
                    dfs_atracacao.clear()

                # Consolidando os dados da tabela de Carga
                if dfs_carga:
                    df_carga_final = dfs_carga[0]
                    for df in dfs_carga[1:]:
                        df_carga_final = df_carga_final.union(df)

                    # Salva na camada business
                    df_carga_final_pd = df_carga_final.toPandas()
                    business_file_path = os.path.join(BUSINESS_PATH, 'carga.parquet')
                    df_carga_final_pd.to_parquet(business_file_path, index=False, engine='pyarrow')
                    print(f'✅ Arquivo consolidado "carga.parquet" salvo na camada business.')
                    dfs_carga.clear()

                # Move o arquivo para a camada business e depois o remove da trusted
                os.rename(trusted_path, os.path.join(BUSINESS_PATH, arquivo))
                print(f'🗑️ {arquivo} movido para a pasta "business" e removido da pasta "trusted".')

            except Exception as e:
                print(f'⚠️ Erro ao processar {arquivo}: {e}')

if __name__ == "__main__":
    gerar_relatorio()
