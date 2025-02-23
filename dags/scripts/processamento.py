import pandas as pd
import os

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../datalake'))
LANDING_PATH = os.path.join(BASE_PATH, 'landing')
RAW_PATH = os.path.join(BASE_PATH, 'raw')

# Garantir que a pasta raw existe
os.makedirs(RAW_PATH, exist_ok=True)

# Lista de arquivos esperados
arquivos_validos = {
    "2021Atracacao.txt",
    "2021Carga.txt",
    "2022Atracacao.txt",
    "2022Carga.txt",
    "2023Atracacao.txt",
    "2023Carga.txt"
}

def processar_dados():
    arquivos_txt = [f for f in os.listdir(LANDING_PATH) if f in arquivos_validos]

    if not arquivos_txt:
        print("Nenhum arquivo válido encontrado na pasta 'landing'.")
        return

    for arquivo in arquivos_txt:
        caminho_arquivo = os.path.join(LANDING_PATH, arquivo)
        caminho_parquet = os.path.join(RAW_PATH, arquivo.replace('.txt', '.parquet'))

        try:
            # Ler arquivo CSV, tratando colunas com tipo misto
            df = pd.read_csv(caminho_arquivo, delimiter=";", low_memory=False, dtype=str)

            # Forçar conversão de colunas problemáticas para string
            colunas_problema = ['FlagConteinerTamanho', 'Nº da Capitania']
            for col in colunas_problema:
                if col in df.columns:
                    df[col] = df[col].astype(str)

            # Converter para formato Parquet
            df.to_parquet(caminho_parquet, engine="pyarrow", index=False)
            print(f"Arquivo {arquivo} convertido e movido para a camada 'raw'.")

            # Remover o arquivo original da pasta landing
            os.remove(caminho_arquivo)

        except Exception as e:
            print(f"Erro ao processar {arquivo}: {e}")