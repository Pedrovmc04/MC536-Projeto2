# MC536: MongoDB Database Project - Renewable Energy and Development Indicators

**Students:**
This project was developed by
* [Luiz Eduardo Silva Salustriano](https://github.com/LuizSalu) RA 183139
* [Pedro Victor Menezes Carneiro](https://github.com/Pedrovmc04) RA 183789
* [Tiago Perrupato Antunes](https://github.com/tiagoperrupato) RA 194058

## Overview

This project involves migrating a relational database about energy and socioeconomic indicators to a NoSQL database. The activity proposed three distinct scenarios, and based on **Scenario B**, we opted to use **MongoDB**, a document-oriented database.

This repository contains:
1. A **detailed justification** for choosing MongoDB.
2. The **logical** and **physical models** for the new database.
3. **Setup and Usage** of the database
4. The **migration scripts** and data population scripts.
5. **Five non-trivial queries** performed in MongoDB.

---

## Table of Contents

1. [Overview](#overview)
2. [Project Structure](#project-structure)
3. [Justification for Choosing MongoDB for Scenario B](#1-justification-for-choosing-mongodb-for-scenario-b)
    - [Technical Analysis](#technical-analysis)
        - [File Storage Format](#file-storage-format)
        - [Query Language and Processing](#query-language-and-processing)
        - [Transaction Processing and Control](#transaction-processing-and-control)
        - [Recovery and Security Mechanisms](#recovery-and-security-mechanisms)
        - [Scalability and Availability](#scalability-and-availability)
4. [Logical Model](#2-logical-model)
    - [Collection `paises`](#collection-paises)
    - [Collection `usinas`](#collection-usinas)
5. [Setup and Usage](#3-setup-and-usage)
    - [Prerequisites](#prerequisites)
    - [Database Setup](#database-setup)
    - [Data Population](#data-population)
    - [Running the Queries](#running-the-queries)
    - [Data Preprocessing](#data-preprocessing)
6. [Physical Model and Population](#4-physical-model-and-population)
7. [Queries Explained](#5-queries-explained)
    - [Query 1: Comparison of Electricity Access - Brazil vs. Global Average](#query-1-comparison-of-electricity-access---brazil-vs-global-average)
    - [Query 2: Top 10 Countries with Highest Renewable Energy Access (2020)](#query-2-top-10-countries-with-highest-renewable-energy-access-2020)
    - [Query 3: Correlation Between HDI and Renewable Energy Generation Per Capita](#query-3-correlation-between-hdi-and-renewable-energy-generation-per-capita)
    - [Query 4: Agents with Multiple Power Plants in Brazil](#query-4-agents-with-multiple-power-plants-in-brazil)
    - [Query 5: Number of Power Plants by Fuel Type in Brazil](#query-5-number-of-power-plants-by-fuel-type-in-brazil)
    - [Query 6: Total Generation Capacity by State in Brazil](#query-6-total-generation-capacity-by-state-in-brazil)
    - [Query 7: Percentage of Renewable Power Plants by State](#query-7-percentage-of-renewable-power-plants-by-state)
    - [Query 8: Renewable Capacity by State vs. National Investment](#query-8-renewable-capacity-by-state-vs-national-investment)

---

## Project Structure

```
MC536-Projeto2/
├── README.md
├── setup_energyDB_mongo.js
├── migration/
│   ├── csv_to_mongo.py
│   ├── sql_to_csv.py
│   ├── csv_data/
│   │   ├── indicador_acesso_combustivel_limpo.csv
│   │   ├── indicador_acesso_eletricidade.csv
│   │   ├── indicador_acesso_energia_renovavel.csv
│   │   ├── indicador_energia_renovavel_per_capita.csv
│   │   ├── indicador_idh.csv
│   │   ├── indicador_investimento_energia_limpa.csv
│   │   ├── paises.csv
│   │   ├── unidades_geradoras.csv
│   │   └── usinas.csv
├── models/
│   └── Logical_model.png
├── queries/
│   ├── run_all_queries.py
│   ├── query_results/
│   │   ├── 1_comparacao_acesso_eletricidade.csv
│   │   ├── 2_top10_paises_energia_renovavel.csv
│   │   ├── 3_correlacao_idh_geracao_renovavel.csv
│   │   ├── 4_agentes_com_multiplas_usinas.csv
│   │   ├── 5_usinas_por_combustivel.csv
│   │   ├── 6_capacidade_por_estado.csv
│   │   ├── 7_percentual_usinas_renovaveis_estado.csv
│   │   └── 8_analise_capacidade_vs_investimento.csv
```

---

## 1. Justification for Choosing MongoDB for Scenario B

**Scenario B** describes a system that needs to handle semi-structured data, schema flexibility, high access volume, and horizontal scalability.

> **Scenario B:** *Your challenge is to develop a system for storing semi-structured data that can vary significantly in its properties. The data model must allow the inclusion of new fields without requiring schema changes or migrations. The volume of simultaneous accesses is high, especially through APIs that manipulate complete entities (with all their aggregated information). There is a requirement for horizontal scalability and support for automatic replication and partitioning.*

**MongoDB** was chosen because it perfectly aligns with these requirements.

### Technical Analysis

#### File Storage Format
* **MongoDB:** Stores data in **BSON** (Binary JSON), which are flexible documents. Each document can have a different structure, allowing the inclusion of new fields without needing to alter a predefined schema (`schema-on-read`).
* **Alignment with the Scenario:** This directly addresses the need for "flexible data structure" and "adding new fields without requiring schema changes." For example, if a new socioeconomic indicator is added for a country, it can be inserted into the corresponding document without affecting others.

#### Query Language and Processing
* **MongoDB:** Uses **MongoDB Query Language (MQL)**, a rich and intuitive language based on JSON. For complex queries, it offers the **Aggregation Framework**, a powerful pipeline system for transforming and combining data.
* **Alignment with the Scenario:** The document model allows a "complete entity" (a country with all its indicators, or a power plant with its generating units) to be stored in a single document. This makes CRUD operations extremely fast, as it avoids the need for complex `JOINs`, meeting the requirements for "low latency in CRUD operations" and "manipulating complete entities."

#### Transaction Processing and Control
* **MongoDB:** Provides support for **multi-document ACID transactions** since version 4.2, ensuring consistency in operations involving multiple documents. However, the data model encourages atomic operations on single documents, which are more performant and sufficient for most use cases, such as updating a power plant or a country.
* **Alignment with the Scenario:** The ability to perform atomic operations on entire documents ensures consistency for most operations, while full ACID transaction support is available for more complex cases, offering an ideal balance between performance and consistency.

#### Recovery and Security Mechanisms
* **Recovery:** MongoDB uses a **journaling** mechanism (write-ahead logging) that ensures data durability in case of failures. Operations are first recorded in the journal and then applied to the data files.
* **Security:** Offers a robust security system with **Role-Based Access Control (RBAC)**, encryption in transit (TLS/SSL) and at rest (Encryption at Rest), and auditing.
* **Alignment with the Scenario:** These mechanisms ensure data reliability and security, essential requirements for any production system with high access volume.

#### Scalability and Availability
* **MongoDB:** Designed for **horizontal scalability** through **sharding** (automatic partitioning of data across multiple servers). High availability is ensured through **replica sets** (redundant copies of data).
* **Alignment with the Scenario:** This is MongoDB's strongest point for Scenario B. The ability to scale horizontally and the native support for replication and load balancing directly meet the requirements for "high scalability and fault tolerance" and "support for replication, partitioning, and load balancing."

---

## 2. Logical Model

We abandoned the normalized relational model in favor of a denormalized document model, focusing on grouping data that is accessed together. We created two main collections: `paises` (countries) and `usinas` (power plants).

The Logical Model can be found in `models/Logical_model.png`, but below is an explanation of the BSON structure of it.

### Collection `paises`
Stores information about each country and nests all its socioeconomic and energy indicators in arrays of subdocuments. This allows all data for a country to be retrieved in a single read operation.

*Example structure of a document:*
```json
{
  "_id": "ObjectId('...')",
  "nome": "Brazil",
  "cod_pais": "BRA",
  "indicadores": {
    "idh": [
      { "ano": 2000, "indice": 0.669 },
      { "ano": 2001, "indice": 0.676 }
    ],
    "acesso_eletricidade": [
      { "ano": 2000, "porcentagem": 93.36 },
      { "ano": 2001, "porcentagem": 94.02 }
    ],
    // ... other indicators
  }
}
```

### Collection `usinas`
Stores information about each power plant in Brazil. Data from other tables, such as `agente_proprietario`, `estado`, `subsistema`, and `unidades_geradoras`, were nested within the power plant document.

*Example structure of a document:*
```json
{
  "_id": "ObjectId('...')",
  "nome": "Usina Angra 1",
  "tipo": "NUCLEAR",
  "agente_proprietario": {
    "nome": "ELETRONUCLEAR"
  },
  "estado": {
    "nome": "Rio de Janeiro",
    "cod_estado": "RJ"
  },
  "subsistema": {
    "nome": "SUDESTE",
    "cod_estado": "SE"
  },
  "unidades_geradoras": [
    {
      "potencia_efetiva_mw": 640,
      "combustivel": "NUCLEAR"
      // other indicators
    }
  ],
  "pais": {
    "nome": "Brazil",
    "cod_pais": "BRA"
  }
}
```

---

## 3. Setup and Usage

### Prerequisites

1. **Python 3.x:** Ensure Python 3 is installed.
2. **MongoDB:** Install and run a MongoDB server (e.g., version 4.4 or higher).
3. **pip:** Python package installer.
4. **Required Python Libraries:** Install necessary libraries:
    ```bash
    pip install pymongo pandas python-dotenv
    ```
5. **Clone Repository:** Clone this repository to your local machine.
    ```bash
    git clone <repository-url>
    cd MC536-Projeto2
    ```
6. **CSV Files:** Ensure the CSV files are present in the `migration/csv_data` directory.

---

### Database Setup

1. **Start MongoDB:** Ensure your MongoDB server is running locally or remotely. You can start MongoDB using Docker or directly on your machine:
    ```bash
    mongod
    ```
    Or, if using Docker:
    ```bash
    docker run -d -p 27017:27017 --name mongodb mongo
    ```

2. **Create Database and Collections:** Use the provided script `setup_energyDB_mongo.js` to create the database and collections:
    ```bash
    mongosh < setup_energyDB_mongo.js
    ```
    This script will:
    - Create the `db_energia` database.
    - Drop existing collections (`paises`, `usinas`) for a clean setup.
    - Create the `paises` and `usinas` collections with schema validation.
    - Define indexes to optimize queries.

    Or you can use any method to create the database on Mongo from the script provided.
---

### Data Population

1. **Export Data from SQL:** If there are .csv files in the `migration/csv_data` folder, as described in the project structure, you should skip this, otherwise rerun our Project 1 with the SQL database, and then use the script `migration/sql_to_csv.py` to export data from the relational database to `.csv` files. Ensure the CSV files are saved in the `migration/csv_data` directory.

2. **Import Data into MongoDB:** Use the script `migration/csv_to_mongo.py` to read the `.csv` files, transform the data, and insert it into the MongoDB collections:
    ```bash
    python migration/csv_to_mongo.py
    ```
    This script will:
    - Read the CSV files from `/data`.
    - Apply the nesting logic described in the logical model.
    - Insert the transformed data into the `paises` and `usinas` collections.

Remember to insert your PostgresSQL and MongoDB configuration parameters in the files.

---

### Running the Queries

1. **Execute Queries:** Use the script `queries/run_all_queries.py` to execute all predefined queries and save the results as `.csv` files:
    ```bash
    python queries/run_all_queries.py
    ```
    This script will:
    - Connect to the MongoDB database.
    - Execute the 8 analytical queries.
    - Save the results in the `queries/query_results` directory.

2. **Inspect Results:** Open the `.csv` files in the `queries/query_results` directory to inspect the query outputs.

---

### Data Preprocessing

The preprocessing steps are embedded within the `migration/csv_to_mongo.py` script. These steps include:
- Transforming flat relational data into nested BSON documents.
- Cleaning and normalizing data to fit the MongoDB schema.
- Ensuring consistency between the `paises` and `usinas` collections.

---

Let me know if you need further adjustments or additional details!

## 4. Physical Model and Population

The physical model is implemented through scripts that create and populate the collections in MongoDB.

- `migration/sql_to_csv.py`: Script (from the previous project) to export data from the relational database to `.csv` files.
- `migration/csv_to_mongo.py`: Python script that reads `.csv` files, transforms the data by applying the nesting logic described in the logical model, and inserts them into the `paises` and `usinas` collections in MongoDB.
- `setup_energyDB_mongo.js`: JavaScript script to be executed in `mongosh`. It creates the collections (`paises`, `usinas`) and defines indexes to optimize the most common queries (e.g., `db.paises.createIndex({ "nome": 1 })`). This script represents the "database creation script."

---

## 5. Queries Explained

The output/results generated by executing the analytical Mongo queries in the `run_all_queries.py` file are saved as CSV files in the `/results` directory for easy inspection:

* `1_comparacao_acesso_eletricidade.csv`
* `2_top10_paises_energia_renovavel.csv`
* `3_correlacao_idh_geracao_renovavel.csv`
* `4_agentes_com_multiplas_usinas.csv`
* `5_usinas_por_combustivel.csv`
* `6_capacidade_por_estado.csv`
* `7_percentual_usinas_renovaveis_estado.csv`
* `8_analise_capacidade_vs_investimento.csv`

Below are the 8 queries implemented in this project, along with their objectives and explanations:

---

### **Query 1: Comparison of Electricity Access - Brazil vs. Global Average**
**Objective:** Analyze the evolution of electricity access in Brazil compared to the global average, year by year.

**Explanation:**
- The query calculates the average electricity access percentage globally for each year.
- It also extracts Brazil's electricity access percentage for the same years.
- The results are sorted by year.

---

### **Query 2: Top 10 Countries with Highest Renewable Energy Access (2020)**
**Objective:** Identify the top 10 countries with the highest renewable energy access in 2020.

**Explanation:**
- Filters countries with renewable energy access data for the year 2020.
- Sorts the countries by renewable energy access percentage in descending order.
- Limits the results to the top 10 countries.

---

### **Query 3: Correlation Between HDI and Renewable Energy Generation Per Capita**
**Objective:** Calculate the statistical correlation between the Human Development Index (HDI) and renewable energy generation per capita, year by year.

**Explanation:**
- Unwinds the `idh` and `geracao_energia_renovavel_per_capita` arrays.
- Matches documents where the year of HDI and renewable energy generation are the same.
- Calculates the covariance and standard deviations to compute the correlation coefficient.

---

### **Query 4: Agents with Multiple Power Plants in Brazil**
**Objective:** Identify agents that own more than one power plant in Brazil.

**Explanation:**
- Groups power plants by their agent owner (`agente_proprietario`).
- Counts the total number of power plants for each agent.
