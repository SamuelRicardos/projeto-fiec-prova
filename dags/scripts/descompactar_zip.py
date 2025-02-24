import requests
import zipfile
import os
from io import BytesIO
from pyspark.sql import SparkSession

RESERVA_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data')

spark = SparkSession.builder \
    .appName("Descompactar zip") \
    .getOrCreate()

def descompactar_zip(ano: int) -> None:

    zip_url = f'https://web3.antaq.gov.br/ea/txt/{ano}.zip'
    
    response = requests.get(zip_url)

    if response.status_code == 200:
        print(f"Arquivo zip do ano {ano} recebido com sucesso!")

        with zipfile.ZipFile(BytesIO(response.content)) as zf:
            print("Conteúdo do arquivo zip:")

            for file_name in zf.namelist():
                print(f"Extraindo: {file_name}")
                
                file_path = os.path.join(RESERVA_PATH, file_name)
                if not os.path.exists(file_path):

                    with zf.open(file_name) as file:
                        file_content = file.read()

                        with open(file_path, 'wb') as output_file:
                            output_file.write(file_content)
                            
                        print(f"Arquivo {file_name} salvo em {file_path}")

                    if file_name.endswith('.txt'):

                        df = spark.read.option("delimiter", ";").csv(file_path, header=True)

                        df.show()

                else:
                    print(f"Arquivo {file_name} já existe em {file_path}, pulando extração.")
    else:
        print(f"Falha ao baixar o arquivo para o ano {ano}. Status: {response.status_code}")
