import os
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Limpeza e Validação de Dados com Spark e Pandas") \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .getOrCreate()

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../datalake'))
RAW_PATH = os.path.join(BASE_PATH, 'raw')
TRUSTED_PATH = os.path.join(BASE_PATH, 'trusted')

def limpar_e_validar_dados():
    for arquivo in os.listdir(RAW_PATH):
        if arquivo.endswith('.parquet'):
            raw_path = os.path.join(RAW_PATH, arquivo)
            trusted_path = os.path.join(TRUSTED_PATH, arquivo)

            print(f"Processando {arquivo}...")

            try:

                df_spark = spark.read.parquet(raw_path)

                df_spark_clean = df_spark.dropDuplicates().dropna()

                df_pandas = df_spark_clean.toPandas()

                df_pandas.to_parquet(trusted_path, index=False)

                print(f"{arquivo} limpo e salvo na camada 'trusted'.")

                os.remove(raw_path)
                print(f"{arquivo} removido da pasta 'raw'.")

            except Exception as e:
                print(f"Erro ao processar {arquivo}: {e}")

if __name__ == "__main__":
    limpar_e_validar_dados()
