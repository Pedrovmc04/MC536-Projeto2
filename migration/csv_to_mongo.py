import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import os
from dotenv import load_dotenv
import numpy as np
import re # Importa a biblioteca de expressões regulares

# Carrega as variáveis do arquivo .env para o ambiente
load_dotenv()

# --- CONFIGURAÇÃO DO MONGODB ---
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME')

# Diretório onde os arquivos CSV estão localizados
INPUT_DIR = 'migration/csv_data'

def import_paises_from_csv(mongo_db):
    """
    Importa dados de países e indicadores dos arquivos CSV para a coleção 'paises' no MongoDB.
    """
    print("Iniciando importação da coleção 'paises' a partir dos CSVs...")

    try:
        # Carregar o CSV principal de países
        df_paises = pd.read_csv(os.path.join(INPUT_DIR, 'paises.csv'))

        # Mapeamento dos arquivos de indicadores para os nomes dos campos no MongoDB
        indicator_files = {
            'idh': 'idh',
            'acesso_eletricidade': 'acesso_eletricidade',
            'acesso_energia_renovavel': 'acesso_energia_renovavel',
            'investimento_energia_limpa': 'investimento_energia_limpa',
            'acesso_combustivel_limpo': 'acesso_combustivel_limpo',
            'energia_renovavel_per_capita': 'geracao_energia_renovavel_per_capita'
        }

        # Pré-carregar e agrupar todos os dados dos indicadores por 'id_pais'
        indicator_data = {}
        for file_key, mongo_field in indicator_files.items():
            df_indicator = pd.read_csv(os.path.join(INPUT_DIR, f'indicador_{file_key}.csv'))
            indicator_data[mongo_field] = df_indicator.groupby('id_pais').apply(lambda x: x[['ano', 'valor']].to_dict('records')).to_dict()

        for _, row in df_paises.iterrows():
            id_pais, cod_pais_raw, nome_pais = row['id_pais'], row['code'], row['nome']
            
            # --- INÍCIO DA CORREÇÃO ---
            # Verifica se o código do país é nulo antes de processar
            if pd.isna(cod_pais_raw):
                print(f"  AVISO: 'cod_pais' nulo para o país '{nome_pais}'. Pulando...")
                continue
            
            # Limpa o cod_pais para remover caracteres inválidos, mantendo apenas letras maiúsculas e _
            cod_pais_clean = re.sub(r'[^A-Z_]', '', str(cod_pais_raw))

            # Se o código ficar vazio após a limpeza, pula o registro
            if not cod_pais_clean:
                print(f"  AVISO: 'cod_pais' ('{cod_pais_raw}') inválido para o país '{nome_pais}'. Pulando após limpeza.")
                continue
            # --- FIM DA CORREÇÃO ---
            
            print(f"  Montando documento para o país: {nome_pais} ({cod_pais_clean})")

            # Usa o valor limpo 'cod_pais_clean' para montar o documento
            pais_doc = {
                'nome': nome_pais,
                'cod_pais': cod_pais_clean 
            }
            
            key_name_map = {
                'idh': 'indice',
                'investimento_energia_limpa': 'valor_dolar',
                'geracao_energia_renovavel_per_capita': 'geracao_watts',
                'acesso_eletricidade': 'porcentagem',
                'acesso_energia_renovavel': 'porcentagem',
                'acesso_combustivel_limpo': 'porcentagem'
            }

            for mongo_field, data_dict in indicator_data.items():
                if id_pais in data_dict:
                    records = data_dict[id_pais]
                    value_key_name = key_name_map.get(mongo_field, 'valor')
                    
                    pais_doc[mongo_field] = [
                        {'ano': int(r['ano']), value_key_name: float(r['valor'])} for r in records
                    ]
            
            # Inserir/Atualizar o documento no MongoDB usando o código limpo
            mongo_db.paises.update_one(
                {'cod_pais': cod_pais_clean},
                {'$set': pais_doc},
                upsert=True
            )
        
        print("Importação da coleção 'paises' concluída com sucesso!")

    except Exception as e:
        print(f"ERRO durante a importação de 'paises': {e}")


def import_usinas_from_csv(mongo_db):
    """
    Importa dados de usinas e unidades geradoras dos CSVs para a coleção 'usinas' no MongoDB.
    """
    print("\nIniciando importação da coleção 'usinas' a partir dos CSVs...")
    
    try:
        df_usinas = pd.read_csv(os.path.join(INPUT_DIR, 'usinas.csv'))
        df_unidades = pd.read_csv(os.path.join(INPUT_DIR, 'unidades_geradoras.csv'))
        unidades_agrupadas = df_unidades.groupby('id_usina')

        for _, usina_row in df_usinas.iterrows():
            ceg = usina_row['ceg']
            if pd.isna(ceg):
                print(f"  AVISO: Usina '{usina_row['nome_usina']}' sem código CEG. Pulando...")
                continue
                
            print(f"  Montando documento para a usina: {usina_row['nome_usina']} ({ceg})")

            usina_doc = {
                'nome_usina': usina_row['nome_usina'],
                'ceg': ceg,
                'cod_pais': usina_row['cod_pais'],
                'tipo_usina': usina_row['tipo_usina'],
                'modalidade_operacao': usina_row['modalidade_operacao'],
                'agente_proprietario': usina_row['agente_proprietario'],
                'estado': {
                    'nome': usina_row['estado_nome'],
                    'cod_estado': usina_row['cod_estado']
                },
                'subsistema': {
                    'nome': usina_row['subsistema_nome'],
                    'cod_subsistema': usina_row['cod_subsistema']
                },
                'unidades_geradoras': []
            }
            
            def format_date(d):
                return d.split(' ')[0] if pd.notna(d) else None

            id_usina = usina_row['id_usina']
            if id_usina in unidades_agrupadas.groups:
                grupo_unidades = unidades_agrupadas.get_group(id_usina)
                for _, unidade_row in grupo_unidades.iterrows():
                    usina_doc['unidades_geradoras'].append({
                        'cod_equipamento': unidade_row['cod_equipamento'],
                        'nome_unidade': unidade_row['nome_unidade'],
                        'num_unidade': int(unidade_row['num_unidade']) if pd.notna(unidade_row['num_unidade']) else None,
                        'potencia_efetiva_mw': float(unidade_row['potencia_efetiva']) if pd.notna(unidade_row['potencia_efetiva']) else 0.0,
                        'data_entrada_teste': format_date(unidade_row['data_entrada_teste']),
                        'data_entrada_operacao': format_date(unidade_row['data_entrada_operacao']),
                        'data_desativacao': format_date(unidade_row['data_desativacao']),
                        'combustivel': unidade_row['combustivel']
                    })
            
            mongo_db.usinas.update_one(
                {'ceg': ceg},
                {'$set': usina_doc},
                upsert=True
            )
            
        print("Importação da coleção 'usinas' concluída com sucesso!")
        
    except Exception as e:
        print(f"ERRO durante a importação de 'usinas': {e}")


if __name__ == '__main__':
    mongo_client = None
    try:
        print("Conectando ao MongoDB Atlas...")
        mongo_client = MongoClient(MONGO_URI)
        mongo_client.admin.command('ping')
        mongo_db = mongo_client[MONGO_DB_NAME]
        print("Conexão com MongoDB Atlas bem-sucedida.")

        import_paises_from_csv(mongo_db)
        import_usinas_from_csv(mongo_db)

    except (Exception, ConnectionFailure) as error:
        print(f"Ocorreu um erro: {error}")
    
    finally:
        if mongo_client:
            mongo_client.close()
            print("\nConexão com MongoDB fechada.")