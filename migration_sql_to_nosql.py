import psycopg2
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from datetime import datetime
import os
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env para o ambiente
load_dotenv()

# --- CONFIGURAÇÃO DOS BANCOS DE DADOS ---
# Altere estes valores para corresponder à sua configuração local

# Configuração do PostgreSQL
PG_CONFIG = {
    'dbname': os.getenv('PG_DBNAME'),
    'user': os.getenv('PG_USER'),
    'password': os.getenv('PG_PASSWORD'),
    'host': os.getenv('PG_HOST'),
    'port': os.getenv('PG_PORT', '5432')
}

# Configuração do MongoDB Atlas
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME')

def migrate_paises(pg_cursor, mongo_db):
    """
    Migra os dados da tabela Pais e suas tabelas de indicadores relacionadas para a coleção 'paises' no MongoDB.
    """
    print("Iniciando migração da coleção 'paises'...")
    
    # Mapeamento das tabelas de indicadores para os nomes dos campos no MongoDB
    indicator_tables = {
        'idh': ('indice', 'idh'),
        'acesso_eletricidade': ('porcentagem', 'acesso_eletricidade'),
        'investimento_energia_limpa': ('valor_dolar', 'investimento_energia_limpa'),
        'acesso_combustivel_limpo': ('porcentagem', 'acesso_energia_limpa_cozinha'),
        'energia_renovavel_per_capita': ('geracao_watts', 'geracao_energia_renovavel_per_capta')
    }

    try:
        # 1. Buscar todos os países da tabela principal
        pg_cursor.execute("SELECT id_pais, code, nome FROM Pais")
        paises = pg_cursor.fetchall()
        print(f"Encontrados {len(paises)} países para migrar.")

        for id_pais, cod_pais, nome_pais in paises:
            
            if not cod_pais:
                print(f"  AVISO: Código de país nulo para '{nome_pais}'. Pulando...")
                continue
            
            print(f"  Migrando dados para o país: {nome_pais} ({cod_pais})")
            
            # 2. Montar o documento base do país
            pais_doc = {
                'nome': nome_pais,
                'cod_pais': cod_pais
            }

            # 3. Para cada indicador, buscar os dados e embutir no documento
            for table_name, (value_column, mongo_field) in indicator_tables.items():
                # SQL injection é evitado usando placeholders (%s)
                query = f"SELECT ano, {value_column} FROM {table_name} WHERE id_pais = %s ORDER BY ano"
                pg_cursor.execute(query, (id_pais,))
                indicator_data = pg_cursor.fetchall()
                
                # O nome do campo de valor muda conforme a coleção de destino
                key_name_map = {
                    'idh': 'indice',
                    'investimento_energia_limpa': 'valor_dolar',
                    'geracao_energia_renovavel_per_capta': 'geracao_watts' # renomeando de kwh para watts
                }
                value_key_name = key_name_map.get(mongo_field, 'porcentagem')


                indicator_list = []
                for ano, valor in indicator_data:
                    
                    indicator_list.append({
                        'ano': ano,
                        value_key_name: float(valor)
                    })
                
                if indicator_list:
                    pais_doc[mongo_field] = indicator_list

            # 4. Inserir/Atualizar o documento no MongoDB
            # upsert=True garante que se o script rodar de novo, ele atualiza em vez de duplicar
            mongo_db.paises.update_one(
                {'cod_pais': cod_pais},
                {'$set': pais_doc},
                upsert=True
            )
        
        print("Migração da coleção 'paises' concluída com sucesso!")

    except Exception as e:
        print(f"ERRO durante a migração de 'paises': {e}")


def migrate_usinas(pg_cursor, mongo_db):
    """
    Migra dados das usinas e suas unidades geradoras para a coleção 'usinas' no MongoDB.
    """
    print("\nIniciando migração da coleção 'usinas'...")
    
    try:
        # 1. Query SQL complexa para juntar todas as informações necessárias das usinas
        sql_query = """
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
        pg_cursor.execute(sql_query)
        usinas = pg_cursor.fetchall()
        print(f"Encontradas {len(usinas)} usinas para migrar.")

        def format_date_as_string(d):
            if d and isinstance(d, (datetime)):
                return d.isoformat().split('T')[0] # Pega apenas a parte da data
            return None # Retorna None se a data for nula
        
        for usina_data in usinas:
            (id_usina, nome_usina, ceg, tipo_usina, modalidade_operacao, agente, 
             est_nome, est_cod, sub_nome, sub_cod, cod_pais) = usina_data
            
            if not ceg:
                print(f"  AVISO: Usina '{nome_usina}' sem código CEG. Pulando...")
                continue

            print(f"  Migrando dados para a usina: {nome_usina} ({ceg})")
            
            # 2. Montar o documento base da usina
            usina_doc = {
                'nome_usina': nome_usina,
                'ceg': ceg,
                'cod_pais': cod_pais,
                'tipo_usina': tipo_usina,
                'modalidade_operacao': modalidade_operacao,
                'agente_proprietario': agente,
                'estado': {
                    'nome': est_nome,
                    'cod_estado': est_cod
                },
                'subsistema': {
                    'nome': sub_nome,
                    'cod_subsistema': sub_cod
                },
                'unidades_geradoras': []
            }

            # 3. Buscar as unidades geradoras associadas
            pg_cursor.execute("SELECT * FROM Unidade_Geradora WHERE id_usina = %s", (id_usina,))
            unidades = pg_cursor.fetchall()
            
            for unidade_data in unidades:
                (id_ug, cod_equip, nome_ug, num_ug, dt_teste, dt_op, dt_desativ, pot_efetiva, combustivel, id_u) = unidade_data
                
                usina_doc['unidades_geradoras'].append({
                    'cod_equipamento': cod_equip,
                    'nome_unidade': nome_ug,
                    'num_unidade': num_ug,
                    'potencia_efetiva_mw': float(pot_efetiva) if pot_efetiva else 0.0,
                    'data_entrada_teste': format_date_as_string(dt_teste),
                    'data_entrada_operacao': format_date_as_string(dt_op),
                    'data_desativacao': format_date_as_string(dt_desativ),
                    'combustivel': combustivel
                })
            
            # 4. Inserir/Atualizar o documento no MongoDB
            mongo_db.usinas.update_one(
                {'ceg': ceg},
                {'$set': usina_doc},
                upsert=True
            )

        print("Migração da coleção 'usinas' concluída com sucesso!")

    except Exception as e:
        print(f"ERRO durante a migração de 'usinas': {e}")


if __name__ == '__main__':
    pg_conn = None
    mongo_client = None
    try:
        # Conectar ao PostgreSQL
        print("Conectando ao PostgreSQL...")
        pg_conn = psycopg2.connect(**PG_CONFIG)
        pg_cursor = pg_conn.cursor()
        print("Conexão com PostgreSQL bem-sucedida.")

        # Conectar ao MongoDB usando a URI
        print("Conectando ao MongoDB Atlas...")
        mongo_client = MongoClient(MONGO_URI)
        # A linha a seguir força uma conexão para verificar se está ativa
        mongo_client.admin.command('ping')
        mongo_db = mongo_client[MONGO_DB_NAME]
        print("Conexão com MongoDB Atlas bem-sucedida.")

        # Executar as migrações
        migrate_paises(pg_cursor, mongo_db)
        migrate_usinas(pg_cursor, mongo_db)

    except (Exception, psycopg2.Error, ConnectionFailure) as error:
        print(f"Ocorreu um erro: {error}")
    
    finally:
        # Fechar as conexões
        if pg_conn:
            pg_cursor.close()
            pg_conn.close()
            print("\nConexão com PostgreSQL fechada.")
        if mongo_client:
            mongo_client.close()
            print("Conexão com MongoDB fechada.")