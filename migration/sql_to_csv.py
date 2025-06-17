# sql_to_csv.py

import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env para o ambiente
load_dotenv()

# --- CONFIGURAÇÃO DO POSTGRESQL ---
PG_CONFIG = {
    'dbname': os.getenv('PG_DBNAME'),
    'user': os.getenv('PG_USER'),
    'password': os.getenv('PG_PASSWORD'),
    'host': os.getenv('PG_HOST'),
    'port': os.getenv('PG_PORT', '5432')
}

# Diretório para salvar os arquivos CSV
OUTPUT_DIR = 'migration/csv_data'

def export_paises_to_csv(pg_conn):
    """
    Exporta dados da tabela Pais e suas tabelas de indicadores relacionadas para arquivos CSV.
    """
    print("Iniciando exportação de países e indicadores para CSV...")

    # Exportar a tabela principal de países
    print("  Exportando 'paises.csv'...")
    sql_paises = "SELECT id_pais, code, nome FROM Pais"
    df_paises = pd.read_sql(sql_paises, pg_conn)
    df_paises.to_csv(os.path.join(OUTPUT_DIR, 'paises.csv'), index=False)

    # Mapeamento das tabelas de indicadores
    indicator_tables = {
        'idh': ('indice', 'idh'),
        'acesso_eletricidade': ('porcentagem', 'acesso_eletricidade'),
        'acesso_energia_renovavel': ('porcentagem', 'acesso_energia_renovavel'),
        'investimento_energia_limpa': ('valor_dolar', 'investimento_energia_limpa'),
        'acesso_combustivel_limpo': ('porcentagem', 'acesso_energia_limpa_cozinha'),
        'energia_renovavel_per_capita': ('geracao_watts', 'geracao_energia_renovavel_per_capita')
    }

    # Exportar cada tabela de indicador
    for table_name, (value_column, mongo_field) in indicator_tables.items():
        print(f"  Exportando 'indicador_{table_name}.csv'...")
        sql_indicator = f"SELECT id_pais, ano, {value_column} FROM {table_name}"
        df_indicator = pd.read_sql(sql_indicator, pg_conn)
        
        # Renomeia a coluna de valor para um nome padronizado para facilitar a importação
        df_indicator.rename(columns={value_column: 'valor'}, inplace=True)
        
        df_indicator.to_csv(os.path.join(OUTPUT_DIR, f'indicador_{table_name}.csv'), index=False)

    print("Exportação de países e indicadores concluída!")


def export_usinas_to_csv(pg_conn):
    """
    Exporta dados de Usinas e Unidades Geradoras para arquivos CSV.
    """
    print("\nIniciando exportação de usinas e unidades geradoras para CSV...")

    # Exportar dados principais das usinas
    print("  Exportando 'usinas.csv'...")
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

    # Exportar todas as unidades geradoras de uma vez
    print("  Exportando 'unidades_geradoras.csv'...")
    sql_unidades = "SELECT * FROM Unidade_Geradora"
    df_unidades = pd.read_sql(sql_unidades, pg_conn)
    df_unidades.to_csv(os.path.join(OUTPUT_DIR, 'unidades_geradoras.csv'), index=False)
    
    print("Exportação de usinas e unidades concluída!")


if __name__ == '__main__':
    # Cria o diretório de output se não existir
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    pg_conn = None
    try:
        print("Conectando ao PostgreSQL...")
        pg_conn = psycopg2.connect(**PG_CONFIG)
        print("Conexão com PostgreSQL bem-sucedida.")

        # Executar as exportações
        export_paises_to_csv(pg_conn)
        export_usinas_to_csv(pg_conn)

    except (Exception, psycopg2.Error) as error:
        print(f"Ocorreu um erro: {error}")
    
    finally:
        if pg_conn:
            pg_conn.close()
            print("\nConexão com PostgreSQL fechada.")