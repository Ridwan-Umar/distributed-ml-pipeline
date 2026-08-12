# ⚙️ Distributed ML Data Pipeline & Model Serving

> **Production-grade distributed ETL + model serving system built with PySpark, Kafka, Airflow, Docker, and Kubernetes — processing 1M+ records for reproducible ML training datasets.**

![PySpark](https://img.shields.io/badge/PySpark-3.x-E25A1C?logo=apache-spark)
![Kafka](https://img.shields.io/badge/Kafka-3.x-231F20?logo=apache-kafka)
![Airflow](https://img.shields.io/badge/Airflow-2.x-017CEE?logo=apache-airflow)
![Docker](https://img.shields.io/badge/Docker-24.x-2496ED?logo=docker)
![Kubernetes](https://img.shields.io/badge/Kubernetes-1.28-326CE5?logo=kubernetes)
![Status](https://img.shields.io/badge/Status-In%20Development-yellow)

---

## 🧠 Project Overview

An end-to-end **distributed data engineering and ML serving platform** that processes **1M+ records** through a multi-stage ETL pipeline. The system integrates streaming ingestion via **Apache Kafka**, batch orchestration via **Apache Airflow**, Spark-based transformations, and containerized model inference on **Kubernetes**.

---

## ✨ Key Features

- ✅ **Spark/PySpark ETL** — ingestion, filtering, joins, aggregations, deduplication, validation
- ✅ **1M+ record throughput** with distributed compute
- ✅ **Kafka streaming ingestion** for real-time data feeds
- ✅ **Airflow DAGs** for scheduled ETL and model-training workflows
- ✅ **Docker containerization** of all pipeline services
- ✅ **Kubernetes deployment** with dependency management & health checks
- ✅ **Data quality checks** at each pipeline stage

---

## 🗂️ Project Structure

```
distributed-ml-pipeline/
│
├── spark/
│   ├── etl_pipeline.py             # Main PySpark ETL job
│   ├── transformations.py          # Filtering, joins, aggregations
│   ├── deduplication.py            # Record deduplication logic
│   ├── feature_generation.py       # ML feature extraction from Spark DFs
│   └── data_quality.py             # Schema & null checks, validation rules
│
├── kafka/
│   ├── producer.py                 # Kafka message producer (data ingestion)
│   ├── consumer.py                 # Kafka consumer → Spark Structured Streaming
│   └── topic_config.yaml           # Topic partition & retention settings
│
├── airflow/
│   ├── dags/
│   │   ├── etl_dag.py              # Daily ETL orchestration DAG
│   │   ├── training_dag.py         # Scheduled model training DAG
│   │   └── quality_check_dag.py    # Data quality monitoring DAG
│   └── plugins/
│       └── spark_submit_operator.py
│
├── serving/
│   ├── app.py                      # FastAPI model inference server
│   ├── model_loader.py             # Versioned model loading
│   └── predict.py                  # Inference endpoint logic
│
├── docker/
│   ├── Dockerfile.spark            # Spark worker image
│   ├── Dockerfile.serving          # Model serving image
│   └── docker-compose.yml          # Local dev orchestration
│
├── k8s/
│   ├── spark-deployment.yaml       # Spark driver/executor manifests
│   ├── serving-deployment.yaml     # Inference service manifest
│   ├── kafka-deployment.yaml       # Kafka broker manifest
│   └── airflow-deployment.yaml     # Airflow scheduler manifest
│
├── scala/
│   └── SparkJob.scala              # Scala Spark job (alternative entrypoint)
│
├── configs/
│   ├── pipeline_config.yaml
│   └── spark_config.yaml
│
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 🏗️ Architecture Overview

```
  Real-time Feed                 Batch Sources
       │                              │
       ▼                              ▼
 Kafka Producer              File / DB Ingestion
       │                              │
       ▼                              │
 Kafka Topic ─────────────────────────┘
       │
       ▼
 PySpark Structured Streaming / Batch ETL
  (Filter → Join → Aggregate → Deduplicate → Validate → Feature Gen)
       │
       ▼
 Feature Store / Data Lake (Parquet / Delta)
       │
  ┌────┴────────────┐
  │                 │
  ▼                 ▼
Airflow DAG     Model Training
(Scheduling)    (ML Pipeline)
                    │
                    ▼
             Model Registry
                    │
                    ▼
          Kubernetes Inference Service
          (FastAPI + Docker Container)
```

---

## ⚙️ Tech Stack

| Layer | Tools |
|---|---|
| Distributed Compute | Apache Spark, PySpark, Scala |
| Stream Ingestion | Apache Kafka |
| Orchestration | Apache Airflow |
| Containerization | Docker, Docker Compose |
| Deployment | Kubernetes (K8s) |
| Serving | FastAPI |
| Language | Python 3.10+, Scala 2.13 |

---

## 🚀 Getting Started (Local Dev)

```bash
# Clone the repository
git clone https://github.com/Ridwan-Umar/distributed-ml-pipeline.git
cd distributed-ml-pipeline

# Start all services locally
docker-compose -f docker/docker-compose.yml up -d

# Submit Spark ETL job
spark-submit spark/etl_pipeline.py --config configs/pipeline_config.yaml

# Deploy to Kubernetes
kubectl apply -f k8s/

# Check running pods
kubectl get pods -n ml-pipeline
```

---

## 📌 Status

> 🚧 **This project is currently under active development.**
> ETL pipeline and Kafka integration are being implemented.

---

## 👤 Author

**Ridwan Umar** — [GitHub](https://github.com/Ridwan-Umar)
