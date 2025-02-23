import os
import shutil

BASE_PATH = os.path.abspath(os.path.dirname(__file__))
RESERVA_PATH = os.path.join(BASE_PATH, '../../data')
LANDING_PATH = os.path.join(BASE_PATH, '../datalake', 'landing')


ARQUIVOS_PERMITIDOS = {
    "2021Atracacao.txt", "2021Carga.txt",
    "2022Atracacao.txt", "2022Carga.txt",
    "2023Atracacao.txt", "2023Carga.txt"
}

def captura_dados():

    arquivos_txt = [f for f in os.listdir(RESERVA_PATH) if f in ARQUIVOS_PERMITIDOS]

    if not arquivos_txt:
        print("Nenhum arquivo permitido encontrado na pasta 'reserva'.")
        return 0

    os.makedirs(LANDING_PATH, exist_ok=True)

    for arquivo in arquivos_txt:
        origem = os.path.join(RESERVA_PATH, arquivo)
        destino = os.path.join(LANDING_PATH, arquivo)

        try:

            shutil.move(origem, destino)
            print(f"Arquivo {arquivo} movido para a camada 'landing'.")
        except Exception as e:
            print(f"Erro ao mover {arquivo}: {e}")

    return len(arquivos_txt)


if __name__ == "__main__":
    captura_dados()