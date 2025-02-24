import os
import pandas as pd

BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), '../datalake'))

def gerar_relatorio():
    trusted_dir = os.path.join(BASE_PATH, 'trusted')
    business_dir = os.path.join(BASE_PATH, 'business')
    os.makedirs(business_dir, exist_ok=True)
    
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

    
    dfs_atracacao = []
    dfs_carga = []

    for arquivo in os.listdir(trusted_dir):
        if arquivo.endswith('.parquet'):
            trusted_path = os.path.join(trusted_dir, arquivo)

            print(f'Processando {arquivo}...')

            try:
                df = pd.read_parquet(trusted_path, engine='pyarrow')

                if 'IDAtracacao' in df.columns and 'Tipo de Navegação da Atracação' in df.columns:
                    df = df[[col for col in atracacao_cols if col in df.columns]]
                    dfs_atracacao.append(df)
                elif 'IDCarga' in df.columns and 'FlagTransporteViaInterioir' in df.columns:
                    df = df[[col for col in carga_cols if col in df.columns]]
                    dfs_carga.append(df)
                else:
                    print(f'Estrutura desconhecida em {arquivo}, ignorando.')
                    continue

                os.remove(trusted_path)
                print(f'{arquivo} removido da pasta "trusted".')

            except Exception as e:
                print(f'Erro ao processar {arquivo}: {e}')

    
    if dfs_atracacao:
        df_atracacao_final = pd.concat(dfs_atracacao, ignore_index=True)
        df_atracacao_final.to_parquet(os.path.join(business_dir, 'atracacao.parquet'), index=False, engine='pyarrow')
        print('Arquivo consolidado "atracacao.parquet" salvo na camada business.')

    if dfs_carga:
        df_carga_final = pd.concat(dfs_carga, ignore_index=True)
        df_carga_final.to_parquet(os.path.join(business_dir, 'carga.parquet'), index=False, engine='pyarrow')
        print('Arquivo consolidado "carga.parquet" salvo na camada business.')
