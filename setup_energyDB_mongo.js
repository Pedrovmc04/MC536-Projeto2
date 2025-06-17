/*
 * ====================================================================
 * Script de Criação e Padronização para o Banco de Dados 'db_energia'
 * ====================================================================
 *
 * Este script executa as seguintes ações:
 * 1. Seleciona o banco de dados 'db_energia'.
 * 2. Remove as coleções existentes ('paises', 'usinas') para um setup limpo.
 * 3. Cria a coleção 'paises' com validação de esquema (schema validation).
 * 4. Cria a coleção 'usinas' com validação de esquema, incluindo objetos e arrays embutidos.
 * 5. Cria os índices essenciais para otimizar a performance das consultas.
 *
 */

// 1. Conectar ao banco de dados desejado (será criado se não existir)
use("db_energia");

// 2. Remover coleções existentes para garantir uma criação limpa
print("Removendo coleções existentes...");
db.paises.drop();
db.usinas.drop();

print("Iniciando a criação e padronização das coleções...");

// 3. Criação da coleção 'paises' com validação
try {
  db.createCollection("paises", {
    validationAction: "error", // Rejeita documentos que não passem na validação
    validationLevel: "strict",  // Aplica a validação a todas as inserções e atualizações
    validator: {
      $jsonSchema: {
        bsonType: "object",
        title: "Validação de Documento de País",
        required: [ "nome", "cod_pais" ],
        properties: {
          nome: {
            bsonType: "string",
            description: "Nome do país (obrigatório)."
          },
          cod_pais: {
            bsonType: "string",
            description: "Código ISO 3166-1 alpha-3 do país (obrigatório).",
            pattern: "^[A-Z_]+$"
          },
          idh: {
            bsonType: "array",
            description: "Array de documentos para o Índice de Desenvolvimento Humano.",
            items: {
              bsonType: "object",
              required: [ "ano", "indice" ],
              properties: {
                ano: { bsonType: "int", description: "Ano do registro." },
                indice: { bsonType: "double", description: "Valor do IDH." }
              }
            }
          },
          investimento_energia_limpa: {
            bsonType: "array",
            description: "Array de documentos de investimento em energia limpa.",
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
            description: "Array de documentos de acesso à eletricidade.",
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
            description: "Array de documentos de acesso à energia renovável.",
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
            description: "Array de documentos de acesso à energia limpa na cozinha.",
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
            description: "Array de documentos de geração de energia renovável per capita.",
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
  print("-> Coleção 'paises' criada com sucesso e com validação de esquema.");
} catch (e) {
  print("Erro ao criar a coleção 'paises': " + e);
}


// 4. Criação da coleção 'usinas' com validação
try {
  db.createCollection("usinas", {
    validationAction: "error",
    validationLevel: "strict",
    validator: {
      $jsonSchema: {
        bsonType: "object",
        title: "Validação de Documento de Usina",
        required: [ "nome_usina", "ceg", "cod_pais" ],
        properties: {
          nome_usina: { bsonType: "string" },
          ceg: { bsonType: "string" },
          cod_pais: {
            bsonType: "string",
            description: "Código de referência ao país onde a usina está localizada.",
            pattern: "^[A-Z_]+$"
          },
          tipo_usina: { bsonType: "string" },
          agente_proprietario: { bsonType: "string" },
          modalidade_operacao: { bsonType: "string" },
          estado: {
            bsonType: "object",
            description: "Objeto embutido com informações do estado.",
            required: ["nome", "cod_estado"],
            properties: {
              nome: { bsonType: "string" },
              cod_estado: { bsonType: "string" }
            }
          },
          subsistema: {
            bsonType: "object",
            description: "Objeto embutido com informações do subsistema.",
            required: ["nome", "cod_subsistema"],
            properties: {
              nome: { bsonType: "string" },
              cod_subsistema: { bsonType: "string" }
            }
          },
          unidades_geradoras: {
            bsonType: "array",
            description: "Array de documentos embutidos, um para cada unidade geradora.",
            items: {
              bsonType: "object",
              required: ["cod_equipamento", "potencia_efetiva_mw"],
              properties: {
                cod_equipamento: { bsonType: "string" },
                nome_unidade: { bsonType: "string" },
                num_unidade: { 
                  bsonType: ["int", "null"], // Permitir inteiro ou nulo
                  description: "Número da unidade geradora, pode ser nulo."
                },
                potencia_efetiva_mw: { bsonType: "double" },
                data_entrada_teste: { 
                    bsonType: ["string", "null"], // Permitir nulo
                    pattern: "^\\d{4}-\\d{2}-\\d{2}$",
                    description: "Data no formato YYYY-MM-DD"
                },
                data_entrada_operacao: {
                    bsonType: ["string", "null"], // Permitir nulo
                    pattern: "^\\d{4}-\\d{2}-\\d{2}$",
                    description: "Data no formato YYYY-MM-DD"
                },
                data_desativacao: {
                    bsonType: ["string", "null"], // Permitir nulo
                    pattern: "^\\d{4}-\\d{2}-\\d{2}$",
                    description: "Data no formato YYYY-MM-DD ou nulo"
                },
                combustivel: { bsonType: "string" }
              }
            }
          }
        }
      }
    }
  });
  print("-> Coleção 'usinas' criada com sucesso e com validação de esquema.");
} catch (e) {
  print("Erro ao criar a coleção 'usinas': " + e);
}


// 5. Criação dos Índices para otimização de performance
print("\nCriando índices...");
try {
    // Índices para a coleção 'paises'
    db.paises.createIndex({ "cod_pais": 1 }, { unique: true });
    db.paises.createIndex({ "idh.ano": 1 })
    db.paises.createIndex({ "investimento_energia_limpa.ano": 1 });
    db.paises.createIndex({ "acesso_eletricidade.ano": 1 });
    db.paises.createIndex({ "acesso_energia_renovavel.ano": 1 });
    db.paises.createIndex({ "acesso_combustivel_limpo.ano": 1 });
    db.paises.createIndex({ "geracao_energia_renovavel_per_capita.ano": 1 });

    // Índices para a coleção 'usinas'
    db.usinas.createIndex({ "ceg": 1 }, { unique: true });
    db.usinas.createIndex({ "cod_pais": 1 }); // Essencial para o $lookup
    db.usinas.createIndex({ "tipo_usina": 1 });
    db.usinas.createIndex({ "estado.cod_estado": 1 });
    db.usinas.createIndex({ "subsistema.cod_subsistema": 1 });

    print("-> Índices criados com sucesso.");
} catch (e) {
    print("Erro ao criar os índices: " + e);
}

print("\nSetup do banco de dados 'db_energia' concluído!");