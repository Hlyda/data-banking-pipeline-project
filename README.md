
# 🏦 data-banking-pipeline-project

_Banking real-time data pipeline with PostgreSQL, Kafka, Debezium, MinIO, Snowflake, dbt, Airflow & ML model (MLOps)._

## 📌 Project Overview

This project builds an end-to-end **real-time banking data pipeline** with integrated **MLOps**.

We simulate customers, accounts and transactions in PostgreSQL, capture changes via CDC with Debezium + Kafka, land raw data in MinIO, load and transform it in Snowflake with dbt (Bronze → Silver → Gold), then use the **Gold features** to train and serve a Machine Learning model. Apache Airflow orchestrates ingestion, transformations and ML workflows, with CI/CD ensuring continuous delivery.

## 🏗️ Architecture

**Pipeline Flow**

1. **PostgreSQL (Source)** – OLTP banking database (customers, accounts, transactions).
2. **Debezium + Kafka (Ingestion)** – CDC on PostgreSQL WAL, streaming changes to Kafka topics.
3. **MinIO / S3 (Data Lake)** – Raw events stored as objects (JSON/Avro).
4. **Snowflake (Warehouse)** – Raw → Bronze → Silver → Gold tables.
5. **dbt (Transformations)** – Cleans data, builds dimensions/facts, and Gold feature tables for ML.
6. **ML Model (MLOps)** – Uses Gold features for training & inference.
7. **Apache Airflow + CI/CD (Automation)** – Orchestrates CDC → Lake → Warehouse → dbt → ML pipelines, plus automated tests & deployment.

## ⚡ Tech Stack

- **PostgreSQL** – Source transactional database (OLTP).
- **Apache Kafka + Debezium** – Real-time CDC and streaming from PostgreSQL.
- **MinIO (S3-compatible)** – Object storage for raw banking events.
- **Snowflake** – Cloud data warehouse (Bronze / Silver / Gold layers).
- **dbt** – SQL transformations, tests, snapshots (SCD2 if needed).
- **Python (Faker + ML libs)** – Data simulation + ML training/inference.
- **Apache Airflow** – Workflow orchestration (data + ML).
- **Docker & docker-compose** – Containerized local environment.
- **Git & GitHub Actions** – CI/CD for dbt tests, unit tests and deployment.

## 📂 Repository Structure

```text
banking-ml-modern-datastack/
├── .github/
│   └── workflows/                 # CI/CD pipelines (tests, dbt, ML)
├── banking_dbt/                   # dbt project (Snowflake)
│   ├── models/
│   │   ├── staging/               # Bronze / Silver staging models
│   │   ├── marts/                 # Facts, dimensions, feature store (Gold)
│   │   └── sources.yml
│   ├── snapshots/                 # SCD Type 2 snapshots (optional)
│   └── dbt_project.yml
├── data-generator/                # Synthetic data generation
│   └── faker_generator.py
├── ml/                            # Machine Learning
│   ├── train_model.py
│   └── predict_model.py
├── docker/                        # Airflow DAGs & configuration
│   └── dags/
│       ├── cdc_to_minio.py
│       ├── minio_to_snowflake.py
│       ├── dbt_transformations.py
│       └── ml_pipeline.py
├── kafka-debezium/                # Kafka & Debezium connectors
│   └── generate_and_post_connector.py
├── postgres/                      # PostgreSQL schema & seeds
│   └── schema.sql
├── docker-compose.yml
├── dockerfile-airflow.dockerfile
├── requirements.txt
└── README.md
``` 



## ⚙️ Step-by-Step Implementation

### 1. Data Simulation (PostgreSQL Source)
- Generate synthetic banking data (customers, accounts, transactions) with `faker_generator.py`.
- Insert into PostgreSQL so it behaves like a real OLTP system (constraints, ACID).

### 2. Kafka + Debezium CDC (Ingestion)
- Configure Debezium PostgreSQL connector to capture changes from the source DB.
- Stream CDC events into Kafka topics, then persist them to MinIO (raw layer).

### 3. Data Lake & Snowflake Ingestion (Stockage)
- Store raw JSON events in MinIO buckets (Raw Data).
- Use Airflow DAGs to load MinIO files into Snowflake Bronze tables.

### 4. dbt Transformations (Transformation / Features)
- Build staging models (Bronze → Silver) to clean and standardize the data.
- Build marts and feature tables (Gold) for Machine Learning.

### 5. ML Training & Inference (MLOps)
- Train a ML model (e.g. fraud detection / risk scoring) using Gold features.
- Save the model artifact and expose an inference script used in Airflow.

### 6. Airflow Orchestration & CI/CD (Automatisation)
- Orchestrate CDC ingestion, Snowflake loads, dbt runs, and ML training/inference in Airflow DAGs.
- Use GitHub Actions to run tests (Python + dbt), and deploy updated DAGs / dbt models on push or merge.

## 📊 Final Deliverables

By the end of this project, we deliver:

- A fully automated **CDC pipeline** from PostgreSQL → Kafka/Debezium → MinIO → Snowflake (Bronze/Silver/Gold).
- A set of **dbt models** (staging, marts, feature tables) implementing the banking warehouse and feature store.
- **Airflow DAGs** orchestrating ingestion, transformations, ML training and inference.
- A trained **Machine Learning model** (e.g. fraud detection / risk scoring) using Gold features, plus prediction scores stored back in Snowflake.
- **CI/CD workflows** (GitHub Actions) for testing (Python + dbt) and deploying pipeline and ML changes.


## 🙌 Inspirations

This project is inspired by several existing modern data stack projects, in particular:

- **Banking Modern Data Stack** by Jaya Chandra Kadiveti (PostgreSQL, Kafka, Debezium, MinIO, Snowflake, dbt, Airflow, CI/CD).  
- Other public end‑to‑end data engineering and MLOps projects (real‑time CDC, Snowflake + dbt + Airflow architectures, and fraud‑detection use cases).

The goal was not to copy these repositories, but to extend the architecture with an explicit **MLOps layer** (feature store + ML training & inference) and adapt it to our own academic project.

## 👥 Project Team

This project was designed and implemented by:

- **Liza HAMADENE**
- **Linda Hind SELAB**
- **Célia Ait-Ouarab**
- **Sid Ahmed LATREUCH**

As part of a group project on real‑time banking data pipelines and MLOps.
