import os
import pandas as pd
from pyspark.sql import SparkSession

# Inicializa o SparkSession
spark = SparkSession.builder \
    .appName("Limpeza e Validação de Dados com Spark e Pandas") \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .getOrCreate()

# Caminhos de diretórios
BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../datalake'))
RAW_PATH = os.path.join(BASE_PATH, 'raw')       # Camada 'raw'
TRUSTED_PATH = os.path.join(BASE_PATH, 'trusted')  # Camada 'trusted'

# Cria a pasta 'trusted' caso não exista
os.makedirs(TRUSTED_PATH, exist_ok=True)

def limpar_e_validar_dados():
    for arquivo in os.listdir(RAW_PATH):
        if arquivo.endswith('.parquet'):
            raw_path = os.path.join(RAW_PATH, arquivo)
            trusted_path = os.path.join(TRUSTED_PATH, arquivo)

            print(f"🔍 Processando {arquivo}...")

            try:
                # Lê o arquivo Parquet com o Spark
                df_spark = spark.read.parquet(raw_path)

                # Remove duplicados e valores nulos com o Spark
                df_spark_clean = df_spark.dropDuplicates().dropna()

                # Converte para DataFrame Pandas
                df_pandas = df_spark_clean.toPandas()

                # Salva o DataFrame limpo na camada 'trusted' em formato Parquet
                df_pandas.to_parquet(trusted_path, index=False)

                print(f"{arquivo} limpo e salvo na camada 'trusted'.")

                # Remove o arquivo original da pasta 'raw'
                os.remove(raw_path)
                print(f"{arquivo} removido da pasta 'raw'.")

            except Exception as e:
                print(f"Erro ao processar {arquivo}: {e}")

if __name__ == "__main__":
    limpar_e_validar_dados()
