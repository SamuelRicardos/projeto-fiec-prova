import os
import shutil
from pyspark.sql import SparkSession

BASE_PATH = os.path.abspath(os.path.dirname(__file__))
RESERVA_PATH = os.path.join(BASE_PATH, '../../data')
LANDING_PATH = os.path.join(BASE_PATH, '../datalake', 'landing')

ARQUIVOS_PERMITIDOS = {
    "2021Atracacao.txt", "2021Carga.txt",
    "2022Atracacao.txt", "2022Carga.txt",
    "2023Atracacao.txt", "2023Carga.txt"
}

spark = SparkSession.builder \
    .appName("Captura de dados") \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .getOrCreate()

def processar_arquivo_com_spark(caminho_arquivo: str):
    if caminho_arquivo.endswith('.txt'):
        print(f"Processando o arquivo {caminho_arquivo} com Spark...")
        
        df = spark.read.option("delimiter", ";").csv(caminho_arquivo, header=True)
        
        colunas_disponiveis = df.columns
        total_colunas = len(colunas_disponiveis)
        
        print(f"O arquivo {caminho_arquivo} possui {total_colunas} colunas.")
        print(f"Colunas disponíveis: {colunas_disponiveis}")

        if df.count() == 0:
            print(f"O arquivo {caminho_arquivo} está vazio.")
            return False
        else:
            print(f"O arquivo {caminho_arquivo} contém dados válidos.")
            return True
    else:
        print(f"O arquivo {caminho_arquivo} não é um arquivo .txt válido para processamento.")
        return False

def captura_dados():
    arquivos_txt = [f for f in os.listdir(RESERVA_PATH) if f in ARQUIVOS_PERMITIDOS]

    if not arquivos_txt:
        print("Nenhum arquivo permitido encontrado na pasta 'reserva'.")
        return 0

    for arquivo in arquivos_txt:
        origem = os.path.join(RESERVA_PATH, arquivo)
        destino = os.path.join(LANDING_PATH, arquivo)

        try:
            if processar_arquivo_com_spark(origem):
                shutil.move(origem, destino)
                print(f"Arquivo {arquivo} movido para a camada 'landing'.")
            else:
                print(f"Arquivo {arquivo} não foi movido devido a erro no processamento.")
        except Exception as e:
            print(f"Erro ao mover {arquivo}: {e}")

    return len(arquivos_txt)

if __name__ == "__main__":
    captura_dados()