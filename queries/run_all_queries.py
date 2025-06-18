import os
import pandas as pd
from pymongo import MongoClient
from typing import List, Dict, Any
from dotenv import load_dotenv

# Load environment variables from the .env file into the environment
load_dotenv()

# --- MONGODB CONFIGURATION ---
MONGO_URI = os.getenv('MONGO_URI')
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME')

def execute_and_save_queries(db: Any):
    """
    Executes a list of predefined queries and saves the results to CSV files.
    """
    # --- Definition of all 8 Queries ---

    # Query 1: Comparison of Electricity Access - Brazil vs. Global Average
    q1_pipeline = [
        {"$unwind": "$acesso_eletricidade"},
        {"$group": {
            "_id": "$acesso_eletricidade.ano",
            "media_global": {"$avg": "$acesso_eletricidade.porcentagem"},
            "dados_brasil": {
                "$push": {
                    "$cond": [{"$eq": ["$nome", "Brazil"]}, "$acesso_eletricidade.porcentagem", "$$REMOVE"]
                }
            }
        }},
        {"$project": {
            "_id": 0, "ano": "$_id",
            "media_global": {"$round": ["$media_global", 2]},
            "acesso_brasil": {"$arrayElemAt": ["$dados_brasil", 0]}
        }},
        {"$sort": {"ano": 1}}
    ]

    # Query 2: Top 10 Countries with Highest Renewable Energy Access (2020)
    q2_pipeline = [
        {"$match": {"acesso_energia_renovavel": {"$exists": True, "$ne": []}}},
        {"$unwind": "$acesso_energia_renovavel"},
        {"$match": {"acesso_energia_renovavel.ano": 2020}},
        {"$sort": {"acesso_energia_renovavel.porcentagem": -1}},
        {"$limit": 10},
        {"$project": {
            "_id": 0, "pais": "$nome", "ano": "$acesso_energia_renovavel.ano",
            "pct_renovavel": "$acesso_energia_renovavel.porcentagem"
        }}
    ]

    # Query 3: Correlation Between HDI and Renewable Energy Generation Per Capita
    q3_pipeline = [
    {"$unwind": "$idh"},
    {"$unwind": "$geracao_energia_renovavel_per_capita"},
    {"$match": {"idh.ano": {"$exists": True}, "geracao_energia_renovavel_per_capita.ano": {"$exists": True}}},
    {"$redact": {
        "$cond": {
            "if": {"$eq": ["$idh.ano", "$geracao_energia_renovavel_per_capita.ano"]},
            "then": "$$DESCEND", "else": "$$PRUNE"
        }
    }},
    {"$group": {
        "_id": "$idh.ano",
        "idh_valores": {"$push": "$idh.indice"},
        "geracao_valores": {"$push": "$geracao_energia_renovavel_per_capita.geracao_watts"},
        "sum_idh": {"$sum": "$idh.indice"},
        "sum_geracao": {"$sum": "$geracao_energia_renovavel_per_capita.geracao_watts"},
        "count": {"$sum": 1}
    }},
    {"$project": {
        "_id": 0,
        "ano": "$_id",
        "idh_valores": 1,
        "geracao_valores": 1,
        "mean_idh": {"$divide": ["$sum_idh", "$count"]},
        "mean_geracao": {"$divide": ["$sum_geracao", "$count"]}
    }},
    {"$addFields": {
        "covariance": {
            "$reduce": {
                "input": {"$zip": {"inputs": ["$idh_valores", "$geracao_valores"]}},
                "initialValue": 0,
                "in": {
                    "$add": [
                        "$$value",
                        {
                            "$multiply": [
                                {"$subtract": [{"$arrayElemAt": ["$$this", 0]}, "$mean_idh"]},
                                {"$subtract": [{"$arrayElemAt": ["$$this", 1]}, "$mean_geracao"]}
                            ]
                        }
                    ]
                }
            }
        },
        "stdDevIdh": {
            "$sqrt": {
                "$reduce": {
                    "input": "$idh_valores",
                    "initialValue": 0,
                    "in": {
                        "$add": [
                            "$$value",
                            {"$pow": [{"$subtract": ["$$this", "$mean_idh"]}, 2]}
                        ]
                    }
                }
            }
        },
        "stdDevGeracao": {
            "$sqrt": {
                "$reduce": {
                    "input": "$geracao_valores",
                    "initialValue": 0,
                    "in": {
                        "$add": [
                            "$$value",
                            {"$pow": [{"$subtract": ["$$this", "$mean_geracao"]}, 2]}
                        ]
                    }
                }
            }
        }
    }},
    {"$project": {
        "ano": 1,
        "correlacao_idh_geracao": {
            "$cond": [
                {"$or": [{"$eq": ["$stdDevIdh", 0]}, {"$eq": ["$stdDevGeracao", 0]}]},
                0,
                {"$divide": ["$covariance", {"$multiply": ["$stdDevIdh", "$stdDevGeracao"]}]}
            ]
        }
    }},
    {"$sort": {"ano": 1}}
]

    # Query 4: Agents with Multiple Power Plants in Brazil
    q4_pipeline = [
        {"$group": {"_id": "$agente_proprietario", "total_usinas": {"$sum": 1}}},
        {"$match": {"total_usinas": {"$gt": 1}}},
        {"$project": {"_id": 0, "agente": "$_id", "total_usinas": "$total_usinas"}},
        {"$sort": {"total_usinas": -1}}
    ]

    # Query 5: Number of Power Plants by Fuel Type in Brazil
    q5_pipeline = [
        {"$unwind": "$unidades_geradoras"},
        {"$group": {"_id": "$unidades_geradoras.combustivel", "usinas_distintas": {"$addToSet": "$_id"}}},
        {"$project": {"_id": 0, "combustivel": "$_id", "qtd_usinas": {"$size": "$usinas_distintas"}}},
        {"$sort": {"qtd_usinas": -1}}
    ]

    # Query 6: Total Generation Capacity by State in Brazil
    q6_pipeline = [
        {"$unwind": "$unidades_geradoras"},
        {"$group": {"_id": "$estado.nome", "capacidade_total_mw": {"$sum": "$unidades_geradoras.potencia_efetiva_mw"}}},
        {"$project": {"_id": 0, "estado": "$_id", "capacidade_total_mw": {"$round": ["$capacidade_total_mw", 2]}}},
        {"$sort": {"capacidade_total_mw": -1}}
    ]
    
    # Query 7: Percentage of Renewable Power Plants by State
    q7_pipeline = [
        {"$facet": {
            "total_por_estado": [{"$group": {"_id": "$estado.nome", "total": {"$sum": 1}}}],
            "renovaveis_por_estado": [
                {"$match": {"unidades_geradoras.combustivel": {"$in": ['HÍDRICA', 'EÓLICA', 'SOLAR', 'BIOMASSA']}}},
                {"$group": {"_id": "$estado.nome", "renovaveis": {"$sum": 1}}}
            ]
        }},
        {"$project": {"all_data": {"$concatArrays": ["$total_por_estado", "$renovaveis_por_estado"]}}},
        {"$unwind": "$all_data"},
        {"$group": {
            "_id": "$all_data._id",
            "total": {"$sum": "$all_data.total"},
            "renovaveis": {"$sum": "$all_data.renovaveis"}
        }},
        {"$project": {
            "_id": 0, "estado": "$_id", "total_usinas": "$total",
            "usinas_renovaveis": {"$ifNull": ["$renovaveis", 0]}
        }},
        {"$project": {
            "estado": 1, "total_usinas": 1, "usinas_renovaveis": 1,
            "perc_renovaveis": {
                "$cond": [
                    {"$eq": ["$total_usinas", 0]}, 0,
                    {"$round": [{"$multiply": [{"$divide": ["$usinas_renovaveis", "$total_usinas"]}, 100]}, 2]}
                ]
            }
        }},
        {"$sort": {"perc_renovaveis": -1}}
    ]

    # Query 8: Renewable Capacity by State vs. National Investment
    q8_pipeline = [
        {"$facet": {
            "capacidade_por_estado": [
                {"$match": {"unidades_geradoras.combustivel": {"$in": ['HÍDRICA', 'EÓLICA', 'SOLAR', 'BIOMASSA']}}},
                {"$unwind": "$unidades_geradoras"},
                {"$match": {"unidades_geradoras.combustivel": {"$in": ['HÍDRICA', 'EÓLICA', 'SOLAR', 'BIOMASSA']}}},
                {"$group": {"_id": "$estado.nome", "capacidade_renovavel_mw": {"$sum": "$unidades_geradoras.potencia_efetiva_mw"}}}
            ],
            "total_nacional": [
                {"$match": {"unidades_geradoras.combustivel": {"$in": ['HÍDRICA', 'EÓLICA', 'SOLAR', 'BIOMASSA']}}},
                {"$unwind": "$unidades_geradoras"},
                {"$match": {"unidades_geradoras.combustivel": {"$in": ['HÍDRICA', 'EÓLICA', 'SOLAR', 'BIOMASSA']}}},
                {"$group": {"_id": None, "total_mw": {"$sum": "$unidades_geradoras.potencia_efetiva_mw"}}}
            ]
        }},
        {"$unwind": "$total_nacional"},
        {"$unwind": "$capacidade_por_estado"},
        {"$lookup": {
            "from": "paises", "pipeline": [{"$match": {"nome": "Brazil"}}], "as": "dados_brasil"
        }},
        {"$unwind": "$dados_brasil"},
        {"$project": {
            "_id": 0, "estado": "$capacidade_por_estado._id",
            "capacidade_renovavel_mw": {"$round": ["$capacidade_por_estado.capacidade_renovavel_mw", 2]},
            "percentual_do_total_nacional": {
                "$cond": [
                    {"$eq": ["$total_nacional.total_mw", 0]}, 0,  # Avoid division by zero
                    {"$round": [{"$multiply": [{"$divide": ["$capacidade_por_estado.capacidade_renovavel_mw", "$total_nacional.total_mw"]}, 100]}, 2]}
                ]
            },
            "investimento_nacional_total_usd": {"$sum": "$dados_brasil.investimento_energia_limpa.valor_dolar"}
        }},
        {"$sort": {"capacidade_renovavel_mw": -1}}
    ]

    # List of queries to execute
    queries_to_run = [
        ("1_comparacao_acesso_eletricidade", "paises", q1_pipeline),
        ("2_top10_paises_energia_renovavel", "paises", q2_pipeline),
        ("3_correlacao_idh_geracao_renovavel", "paises", q3_pipeline),
        ("4_agentes_com_multiplas_usinas", "usinas", q4_pipeline),
        ("5_usinas_por_combustivel", "usinas", q5_pipeline),
        ("6_capacidade_por_estado", "usinas", q6_pipeline),
        ("7_percentual_usinas_renovaveis_estado", "usinas", q7_pipeline),
        ("8_analise_capacidade_vs_investimento", "usinas", q8_pipeline)
    ]

    # Create directory for results if it does not exist
    output_dir = "queries/query_results"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Execute each query and save the result
    for filename, collection_name, pipeline in queries_to_run:
        print(f"Running query: {filename}...")
        
        collection = db[collection_name]
        results = list(collection.aggregate(pipeline))

        if results:
            df = pd.DataFrame(results)
            output_path = os.path.join(output_dir, f"{filename}.csv")
            df.to_csv(output_path, index=False, encoding='utf-8')
            print(f"Results saved to: {output_path}\n")
        else:
            print("Query returned no results.\n")

def main():
    """
    Main function to connect to MongoDB and start executing queries.
    """
    try:
        print("Connecting to MongoDB Atlas...")
        mongo_client = MongoClient(MONGO_URI)
        mongo_client.admin.command('ping')
        mongo_db = mongo_client[MONGO_DB_NAME]
        print("Connection to MongoDB Atlas successful.")

        execute_and_save_queries(mongo_db)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'client' in locals() and mongo_client:
            mongo_client.close()
            print("MongoDB connection closed.")

if __name__ == "__main__":
    main()