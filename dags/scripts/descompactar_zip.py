import requests
import zipfile
import os
from io import BytesIO

RESERVA_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data')

zip_url_2021 = 'https://web3.antaq.gov.br/ea/txt/2021.zip'
zip_url_2022 = 'https://web3.antaq.gov.br/ea/txt/2022.zip'
zip_url_2023 = 'https://web3.antaq.gov.br/ea/txt/2023.zip'

def descompactar_zip_2021():

    response = requests.get(zip_url_2021)

    if response.status_code == 200:
        print("Arquivo zip recebido com sucesso!")
        
        with zipfile.ZipFile(BytesIO(response.content)) as zf:

            print("Conteúdo do arquivo zip:")
            for file_name in zf.namelist():
                print(file_name)
                
                file_path = os.path.join(RESERVA_PATH, file_name)
                if not os.path.exists(file_path):

                    if file_name.endswith('.txt'):
                        with zf.open(file_name) as file:

                            file_content = file.read()

                            with open(file_path, 'wb') as output_file:
                                output_file.write(file_content)
                            
                            print(f"Arquivo {file_name} salvo em {file_path}")
                else:
                    print(f"Arquivo {file_name} já existe em {file_path}, pulando extração.")
    else:
        print(f"Falha ao baixar o arquivo. Status: {response.status_code}")

def descompactar_zip_2022():

    response = requests.get(zip_url_2022)

    if response.status_code == 200:
        print("Arquivo zip recebido com sucesso!")
        
        with zipfile.ZipFile(BytesIO(response.content)) as zf:

            print("Conteúdo do arquivo zip:")
            for file_name in zf.namelist():
                print(file_name)
                
                file_path = os.path.join(RESERVA_PATH, file_name)
                if not os.path.exists(file_path):

                    if file_name.endswith('.txt'):
                        with zf.open(file_name) as file:

                            file_content = file.read()

                            with open(file_path, 'wb') as output_file:
                                output_file.write(file_content)
                            
                            print(f"Arquivo {file_name} salvo em {file_path}")
                else:
                    print(f"Arquivo {file_name} já existe em {file_path}, pulando extração.")
    else:
        print(f"Falha ao baixar o arquivo. Status: {response.status_code}")

def descompactar_zip_2023():

    response = requests.get(zip_url_2023)

    if response.status_code == 200:
        print("Arquivo zip recebido com sucesso!")
        
        with zipfile.ZipFile(BytesIO(response.content)) as zf:

            print("Conteúdo do arquivo zip:")
            for file_name in zf.namelist():
                print(file_name)
                
                file_path = os.path.join(RESERVA_PATH, file_name)
                if not os.path.exists(file_path):

                    if file_name.endswith('.txt'):
                        with zf.open(file_name) as file:

                            file_content = file.read()

                            with open(file_path, 'wb') as output_file:
                                output_file.write(file_content)
                            
                            print(f"Arquivo {file_name} salvo em {file_path}")
                else:
                    print(f"Arquivo {file_name} já existe em {file_path}, pulando extração.")
    else:
        print(f"Falha ao baixar o arquivo. Status: {response.status_code}")