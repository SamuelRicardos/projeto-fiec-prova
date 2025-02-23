import os
import shutil
from pyspark.sql import SparkSession

# Caminhos de diretórios
BASE_PATH = os.path.abspath(os.path.dirname(__file__))
RESERVA_PATH = os.path.join(BASE_PATH, '../../data')
LANDING_PATH = os.path.join(BASE_PATH, '../datalake', 'landing')

# Arquivos permitidos para processamento
ARQUIVOS_PERMITIDOS = {
    "2021Atracacao.txt", "2021Carga.txt",
    "2022Atracacao.txt", "2022Carga.txt",
    "2023Atracacao.txt", "2023Carga.txt"
}

# Inicializa o SparkSession
spark = SparkSession.builder \
    .appName("Captura de Dados e Processamento com Spark") \
    .getOrCreate()

def processar_arquivo_com_spark(caminho_arquivo: str):
    """
    Função para processar o arquivo .txt movido com Spark.
    Pode ser usada para validar, analisar ou visualizar os dados.
    """
    if caminho_arquivo.endswith('.txt'):
        print(f"Processando o arquivo {caminho_arquivo} com Spark...")

        # Lê o arquivo .txt com o Spark
        df = spark.read.option("delimiter", ";").csv(caminho_arquivo, header=True)

        # Exibe as primeiras linhas do DataFrame para visualização
        df.show()

        # Aqui você pode adicionar qualquer transformação ou validação que desejar
        # Exemplo: Verificar se o arquivo contém dados válidos
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

    # Lista os arquivos permitidos na pasta RESERVA
    arquivos_txt = [f for f in os.listdir(RESERVA_PATH) if f in ARQUIVOS_PERMITIDOS]

    if not arquivos_txt:
        print("Nenhum arquivo permitido encontrado na pasta 'reserva'.")
        return 0

    # Cria a pasta 'landing' se não existir
    os.makedirs(LANDING_PATH, exist_ok=True)

    # Move os arquivos da pasta 'reserva' para a pasta 'landing'
    for arquivo in arquivos_txt:
        origem = os.path.join(RESERVA_PATH, arquivo)
        destino = os.path.join(LANDING_PATH, arquivo)

        try:
            # Processa o arquivo antes de movê-lo, com Spark
            if processar_arquivo_com_spark(origem):
                # Só move o arquivo se o processamento for bem-sucedido
                shutil.move(origem, destino)
                print(f"Arquivo {arquivo} movido para a camada 'landing'.")
            else:
                print(f"Arquivo {arquivo} não foi movido devido a erro no processamento.")
        except Exception as e:
            print(f"Erro ao mover {arquivo}: {e}")

    return len(arquivos_txt)


if __name__ == "__main__":
    captura_dados()