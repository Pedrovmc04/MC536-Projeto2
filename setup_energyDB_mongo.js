/*
 * ====================================================================
 * Creation and Standardization Script for the 'db_energia' Database
 * ====================================================================
 *
 * This script performs the following actions:
 * 1. Selects the 'db_energia' database.
 * 2. Removes existing collections ('paises', 'usinas') for a clean setup.
 * 3. Creates the 'paises' collection with schema validation.
 * 4. Creates the 'usinas' collection with schema validation, including embedded objects and arrays.
 * 5. Creates essential indexes to optimize query performance.
 */

// 1. Connect to the desired database (will be created if it does not exist)
use("db_energia");

// 2. Remove existing collections to ensure a clean creation
print("Removing existing collections...");
db.paises.drop();
db.usinas.drop();

print("Starting the creation and standardization of collections...");

// 3. Creation of the 'paises' collection with validation
try {
  db.createCollection("paises", {
    validationAction: "error", // Rejects documents that fail validation
    validationLevel: "strict",  // Applies validation to all inserts and updates
    validator: {
      $jsonSchema: {
        bsonType: "object",
        title: "Country Document Validation",
        required: [ "nome", "cod_pais" ],
        properties: {
          nome: {
            bsonType: "string",
            description: "Country name (required)."
          },
          cod_pais: {
            bsonType: "string",
            description: "ISO 3166-1 alpha-3 country code (required).",
            pattern: "^[A-Z_]+$"
          },
          idh: {
            bsonType: "array",
            description: "Array of documents for the Human Development Index.",
            items: {
              bsonType: "object",
              required: [ "ano", "indice" ],
              properties: {
                ano: { bsonType: "int", description: "Year of the record." },
                indice: { bsonType: "double", description: "Value of the HDI." }
              }
            }
          },
          investimento_energia_limpa: {
            bsonType: "array",
            description: "Array of documents for clean energy investment.",
            items: {
              bsonType: "object",
              required: ["ano", "valor_dolar"],
              properties: {
                ano: { bsonType: "int" },
                valor_dolar: { bsonType: "double" }
              }
            }
          },
          acesso_eletricidade: {
            bsonType: "array",
            description: "Array of documents for electricity access.",
            items: {
                bsonType: "object",
                required: ["ano", "porcentagem"],
                properties: {
                    ano: { bsonType: "int" },
                    porcentagem: { bsonType: "double" }
                }
            }
          },
          acesso_energia_renovavel: {
            bsonType: "array",
            description: "Array of documents for renewable energy access.",
            items: {
                bsonType: "object",
                required: ["ano", "porcentagem"],
                properties: {
                    ano: { bsonType: "int" },
                    porcentagem: { bsonType: "double" }
                }
            }
          },
          acesso_combustivel_limpo: {
            bsonType: "array",
            description: "Array of documents for clean cooking fuel access.",
            items: {
                bsonType: "object",
                required: ["ano", "porcentagem"],
                properties: {
                    ano: { bsonType: "int" },
                    porcentagem: { bsonType: "double" }
                }
            }
          },
          geracao_energia_renovavel_per_capita: {
            bsonType: "array",
            description: "Array of documents for renewable energy generation per capita.",
            items: {
                bsonType: "object",
                required: ["ano", "geracao_watts"],
                properties: {
                    ano: { bsonType: "int" },
                    geracao_watts: { bsonType: "double" }
                }
            }
          }
        }
      }
    }
  });
  print("-> Collection 'paises' successfully created with schema validation.");
} catch (e) {
  print("Error creating the 'paises' collection: " + e);
}


// 4. Creation of the 'usinas' collection with validation
try {
  db.createCollection("usinas", {
    validationAction: "error",
    validationLevel: "strict",
    validator: {
      $jsonSchema: {
        bsonType: "object",
        title: "Power Plant Document Validation",
        required: [ "nome_usina", "ceg", "cod_pais" ],
        properties: {
          nome_usina: { bsonType: "string" },
          ceg: { bsonType: "string" },
          cod_pais: {
            bsonType: "string",
            description: "Reference code for the country where the plant is located.",
            pattern: "^[A-Z_]+$"
          },
          tipo_usina: { bsonType: "string" },
          agente_proprietario: { bsonType: "string" },
          modalidade_operacao: { bsonType: "string" },
          estado: {
            bsonType: "object",
            description: "Embedded object with state information.",
            required: ["nome", "cod_estado"],
            properties: {
              nome: { bsonType: "string" },
              cod_estado: { bsonType: "string" }
            }
          },
          subsistema: {
            bsonType: "object",
            description: "Embedded object with subsystem information.",
            required: ["nome", "cod_subsistema"],
            properties: {
              nome: { bsonType: "string" },
              cod_subsistema: { bsonType: "string" }
            }
          },
          unidades_geradoras: {
            bsonType: "array",
            description: "Array of embedded documents, one for each generating unit.",
            items: {
              bsonType: "object",
              required: ["cod_equipamento", "potencia_efetiva_mw"],
              properties: {
                cod_equipamento: { bsonType: "string" },
                nome_unidade: { bsonType: "string" },
                num_unidade: { 
                  bsonType: ["int", "null"], // Allow integer or null
                  description: "Number of the generating unit, can be null."
                },
                potencia_efetiva_mw: { bsonType: "double" },
                data_entrada_teste: { 
                    bsonType: ["string", "null"], // Allow null
                    pattern: "^\\d{4}-\\d{2}-\\d{2}$",
                    description: "Date in YYYY-MM-DD format"
                },
                data_entrada_operacao: {
                    bsonType: ["string", "null"], // Allow null
                    pattern: "^\\d{4}-\\d{2}-\\d{2}$",
                    description: "Date in YYYY-MM-DD format"
                },
                data_desativacao: {
                    bsonType: ["string", "null"], // Allow null
                    pattern: "^\\d{4}-\\d{2}-\\d{2}$",
                    description: "Date in YYYY-MM-DD format or null"
                },
                combustivel: { bsonType: "string" }
              }
            }
          }
        }
      }
    }
  });
  print("-> Collection 'usinas' successfully created with schema validation.");
} catch (e) {
  print("Error creating the 'usinas' collection: " + e);
}


// 5. Creation of Indexes for performance optimization
print("\nCreating indexes...");
try {
    // Indexes for the 'paises' collection
    db.paises.createIndex({ "cod_pais": 1 }, { unique: true });
    db.paises.createIndex({ "idh.ano": 1 });
    db.paises.createIndex({ "investimento_energia_limpa.ano": 1 });
    db.paises.createIndex({ "acesso_eletricidade.ano": 1 });
    db.paises.createIndex({ "acesso_energia_renovavel.ano": 1 });
    db.paises.createIndex({ "acesso_combustivel_limpo.ano": 1 });
    db.paises.createIndex({ "geracao_energia_renovavel_per_capita.ano": 1 });

    // Indexes for the 'usinas' collection
    db.usinas.createIndex({ "ceg": 1 }, { unique: true });
    db.usinas.createIndex({ "cod_pais": 1 }); // Essential for $lookup
    db.usinas.createIndex({ "tipo_usina": 1 });
    db.usinas.createIndex({ "estado.cod_estado": 1 });
    db.usinas.createIndex({ "subsistema.cod_subsistema": 1 });

    print("-> Indexes successfully created.");
} catch (e) {
    print("Error creating indexes: " + e);
}

print("\nSetup of the 'db_energia' database completed!");