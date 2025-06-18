import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import os
from dotenv import load_dotenv
import numpy as np
import re # Import regular expression library

# Load environment variables from the .env file into the environment
load_dotenv()

# --- MONGODB CONFIGURATION ---
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME')

# Directory where the CSV files are located
INPUT_DIR = 'migration/csv_data'

def import_paises_from_csv(mongo_db):
    """
    Imports country data and indicators from CSV files into the 'paises' collection in MongoDB.
    """
    print("Starting import of 'paises' collection from CSVs...")

    try:
        # Load the main countries CSV
        df_paises = pd.read_csv(os.path.join(INPUT_DIR, 'paises.csv'))

        # Mapping of indicator files to MongoDB field names
        indicator_files = {
            'idh': 'idh',
            'acesso_eletricidade': 'acesso_eletricidade',
            'acesso_energia_renovavel': 'acesso_energia_renovavel',
            'investimento_energia_limpa': 'investimento_energia_limpa',
            'acesso_combustivel_limpo': 'acesso_combustivel_limpo',
            'energia_renovavel_per_capita': 'geracao_energia_renovavel_per_capita'
        }

        # Preload and group all indicator data by 'id_pais'
        indicator_data = {}
        for file_key, mongo_field in indicator_files.items():
            df_indicator = pd.read_csv(os.path.join(INPUT_DIR, f'indicador_{file_key}.csv'))
            indicator_data[mongo_field] = df_indicator.groupby('id_pais').apply(lambda x: x[['ano', 'valor']].to_dict('records')).to_dict()

        for _, row in df_paises.iterrows():
            id_pais, cod_pais_raw, nome_pais = row['id_pais'], row['code'], row['nome']
            
            # --- START OF CORRECTION ---
            # Check if the country code is null before processing
            if pd.isna(cod_pais_raw):
                print(f"  WARNING: 'cod_pais' is null for country '{nome_pais}'. Skipping...")
                continue
            
            # Clean the cod_pais to remove invalid characters, keeping only uppercase letters and _
            cod_pais_clean = re.sub(r'[^A-Z_]', '', str(cod_pais_raw))

            # If the code becomes empty after cleaning, skip the record
            if not cod_pais_clean:
                print(f"  WARNING: 'cod_pais' ('{cod_pais_raw}') is invalid for country '{nome_pais}'. Skipping after cleaning.")
                continue
            # --- END OF CORRECTION ---
            
            print(f"  Building document for country: {nome_pais} ({cod_pais_clean})")

            # Use the cleaned 'cod_pais_clean' value to build the document
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
            
            # Insert/Update the document in MongoDB using the cleaned code
            mongo_db.paises.update_one(
                {'cod_pais': cod_pais_clean},
                {'$set': pais_doc},
                upsert=True
            )
        
        print("Import of 'paises' collection completed successfully!")

    except Exception as e:
        print(f"ERROR during import of 'paises': {e}")


def import_usinas_from_csv(mongo_db):
    """
    Imports power plant and generating unit data from CSVs into the 'usinas' collection in MongoDB.
    """
    print("\nStarting import of 'usinas' collection from CSVs...")
    
    try:
        # Load the main power plants CSV
        df_usinas = pd.read_csv(os.path.join(INPUT_DIR, 'usinas.csv'))
        # Load all generating units at once
        df_unidades = pd.read_csv(os.path.join(INPUT_DIR, 'unidades_geradoras.csv'))
        unidades_agrupadas = df_unidades.groupby('id_usina')

        for _, usina_row in df_usinas.iterrows():
            ceg = usina_row['ceg']
            if pd.isna(ceg):
                print(f"  WARNING: Power plant '{usina_row['nome_usina']}' has no CEG code. Skipping...")
                continue
                
            print(f"  Building document for power plant: {usina_row['nome_usina']} ({ceg})")

            # Build the document for the power plant
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
            
        print("Import of 'usinas' collection completed successfully!")
        
    except Exception as e:
        print(f"ERROR during import of 'usinas': {e}")


if __name__ == '__main__':
    mongo_client = None
    try:
        print("Connecting to MongoDB Atlas...")
        mongo_client = MongoClient(MONGO_URI)
        mongo_client.admin.command('ping')
        mongo_db = mongo_client[MONGO_DB_NAME]
        print("Connection to MongoDB Atlas successful.")

        import_paises_from_csv(mongo_db)
        import_usinas_from_csv(mongo_db)

    except (Exception, ConnectionFailure) as error:
        print(f"An error occurred: {error}")
    
    finally:
        if mongo_client:
            mongo_client.close()
            print("\nMongoDB connection closed.")