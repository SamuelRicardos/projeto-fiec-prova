import os
import pandas as pd

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../datalake'))

def limpar_e_validar_dados():
    raw_dir = os.path.join(BASE_PATH, 'raw')
    trusted_dir = os.path.join(BASE_PATH, 'trusted')

    os.makedirs(trusted_dir, exist_ok=True)

    for arquivo in os.listdir(raw_dir):
        if arquivo.endswith('.parquet'):
            raw_path = os.path.join(raw_dir, arquivo)
            trusted_path = os.path.join(trusted_dir, arquivo)

            print(f"🔍 Processando {arquivo}...")
            
            try:
                df = pd.read_parquet(raw_path, engine="pyarrow")

                df = df.drop_duplicates().dropna()

                df.to_parquet(trusted_path, engine="pyarrow", index=False)
                print(f" {arquivo} limpo e salvo na camada 'trusted'.")
                
                os.remove(raw_path)
                print(f" {arquivo} removido da pasta 'raw'.")
                
            except Exception as e:
                print(f" Erro ao processar {arquivo}: {e}")
