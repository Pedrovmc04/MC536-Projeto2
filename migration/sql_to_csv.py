# sql_to_csv.py

import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables from the .env file into the environment
load_dotenv()

# --- POSTGRESQL CONFIGURATION ---
PG_CONFIG = {
    'dbname': os.getenv('PG_DBNAME'),
    'user': os.getenv('PG_USER'),
    'password': os.getenv('PG_PASSWORD'),
    'host': os.getenv('PG_HOST'),
    'port': os.getenv('PG_PORT', '5432')
}

# Directory to save CSV files
OUTPUT_DIR = 'migration/csv_data'

def export_paises_to_csv(pg_conn):
    """
    Exports data from the Pais table and its related indicator tables to CSV files.
    """
    print("Starting export of countries and indicators to CSV...")

    # Export the main countries table
    print("  Exporting 'paises.csv'...")
    sql_paises = "SELECT id_pais, code, nome FROM Pais"
    df_paises = pd.read_sql(sql_paises, pg_conn)
    df_paises.to_csv(os.path.join(OUTPUT_DIR, 'paises.csv'), index=False)

    # Mapping of indicator tables
    indicator_tables = {
        'idh': ('indice', 'idh'),
        'acesso_eletricidade': ('porcentagem', 'acesso_eletricidade'),
        'acesso_energia_renovavel': ('porcentagem', 'acesso_energia_renovavel'),
        'investimento_energia_limpa': ('valor_dolar', 'investimento_energia_limpa'),
        'acesso_combustivel_limpo': ('porcentagem', 'acesso_energia_limpa_cozinha'),
        'energia_renovavel_per_capita': ('geracao_watts', 'geracao_energia_renovavel_per_capita')
    }

    # Export each indicator table
    for table_name, (value_column, mongo_field) in indicator_tables.items():
        print(f"  Exporting 'indicador_{table_name}.csv'...")
        sql_indicator = f"SELECT id_pais, ano, {value_column} FROM {table_name}"
        df_indicator = pd.read_sql(sql_indicator, pg_conn)
        
        # Renames the value column to a standardized name for easier import
        df_indicator.rename(columns={value_column: 'valor'}, inplace=True)
        
        df_indicator.to_csv(os.path.join(OUTPUT_DIR, f'indicador_{table_name}.csv'), index=False)

    print("Export of countries and indicators completed!")


def export_usinas_to_csv(pg_conn):
    """
    Exports data from Power Plants and Generating Units to CSV files.
    """
    print("\nStarting export of power plants and generating units to CSV...")

    # Export main power plant data
    print("  Exporting 'usinas.csv'...")
    sql_usinas = """
    SELECT 
        u.id_usina,
        u.nome AS nome_usina,
        u.ceg,
        u.tipo AS tipo_usina,
        u.modalidade_operacao,
        ap.nome AS agente_proprietario,
        e.nome AS estado_nome,
        e.cod_estado,
        s.nome AS subsistema_nome,
        s.cod_subsistema,
        p.code AS cod_pais
    FROM Usina u
    LEFT JOIN Agente_Proprietario ap ON u.id_agente_proprietario = ap.id_agente
    LEFT JOIN Estado e ON u.id_estado = e.id_estado
    LEFT JOIN Subsistema_estado se ON e.id_estado = se.id_estado
    LEFT JOIN Subsistema s ON se.id_subsistema = s.id_subsistema
    LEFT JOIN Pais p ON s.id_pais = p.id_pais
    WHERE p.code = 'BRA';
    """
    df_usinas = pd.read_sql(sql_usinas, pg_conn)
    df_usinas.to_csv(os.path.join(OUTPUT_DIR, 'usinas.csv'), index=False)

    # Export all generating units at once
    print("  Exporting 'unidades_geradoras.csv'...")
    sql_unidades = "SELECT * FROM Unidade_Geradora"
    df_unidades = pd.read_sql(sql_unidades, pg_conn)
    df_unidades.to_csv(os.path.join(OUTPUT_DIR, 'unidades_geradoras.csv'), index=False)
    
    print("Export of power plants and generating units completed!")


if __name__ == '__main__':
    # Create the output directory if it does not exist
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    pg_conn = None
    try:
        print("Connecting to PostgreSQL...")
        pg_conn = psycopg2.connect(**PG_CONFIG)
        print("PostgreSQL connection successful.")

        # Execute the exports
        export_paises_to_csv(pg_conn)
        export_usinas_to_csv(pg_conn)

    except (Exception, psycopg2.Error) as error:
        print(f"An error occurred: {error}")
    
    finally:
        if pg_conn:
            pg_conn.close()
            print("\nPostgreSQL connection closed.")