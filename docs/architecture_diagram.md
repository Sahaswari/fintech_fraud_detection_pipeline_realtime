# 🏗️ System Architecture Diagram

## FinTech Fraud Detection Pipeline — Lambda Architecture

```mermaid
flowchart TB
    subgraph INGESTION["🔵 DATA INGESTION LAYER"]
        direction LR
        PRODUCER["🏦 Transaction Producer<br/><i>Python (kafka-python)</i><br/>───────────────<br/>• Generates 2 TPS<br/>• 5% fraud injection<br/>• High-value & impossible travel"]
        KAFKA["📨 Apache Kafka<br/><i>Confluent 7.4.0</i><br/>───────────────<br/>• Topic: transactions<br/>• ZooKeeper managed<br/>• Broker: kafka:29092"]
        PRODUCER -->|"JSON messages"| KAFKA
    end

    subgraph HOT_PATH["🔴 HOT PATH — Real-Time (Speed Layer)"]
        direction LR
        SPARK["⚡ Spark Structured Streaming<br/><i>PySpark 3.5.0</i><br/>───────────────<br/>• 10-second micro-batches<br/>• Watermark: 1 minute<br/>• Checkpoint recovery"]
        RULES["🔍 Fraud Detection Rules<br/>───────────────<br/>1. High-Value: amount &gt; $5,000<br/>2. Impossible Travel:<br/>   same user, different country,<br/>   within 10 minutes"]
        SPARK --> RULES
    end

    subgraph STORAGE["🟢 SERVING LAYER — Data Storage"]
        direction TB
        PG["🐘 PostgreSQL 15<br/><i>ACID-Compliant</i><br/>───────────────<br/>• valid_transactions<br/>• fraud_alerts<br/>• daily_reconciliation<br/>• reconciliation_overview (view)"]
        PARQUET["📦 Parquet Data Warehouse<br/><i>PyArrow Engine</i><br/>───────────────<br/>• valid_transactions/date=YYYY-MM-DD/<br/>• fraud_archive/date=YYYY-MM-DD/<br/>• Date-partitioned storage"]
    end

    subgraph COLD_PATH["🔵 COLD PATH — Batch (Batch Layer)"]
        direction TB
        AIRFLOW["🌀 Apache Airflow 2.7.1<br/>───────────────<br/>DAG: fraud_detection_reconciliation<br/>Schedule: Every 6 hours"]
        subgraph DAG_TASKS["DAG Task Pipeline"]
            direction LR
            T1["📥 Extract<br/>Transaction Data"]
            T2["🔄 Transform<br/>& Analyze"]
            T3["🏗️ Export to<br/>Data Warehouse"]
            T4["📋 Generate<br/>Reconciliation Report"]
            T1 --> T2 --> T3 --> T4
        end
        AIRFLOW --> DAG_TASKS
    end

    subgraph ANALYTICS["📊 ANALYTICS & REPORTING"]
        direction LR
        REPORT_GEN["📈 Analytics Report Generator<br/><i>Matplotlib + Seaborn</i><br/>───────────────<br/>• Fraud by category<br/>• Fraud type distribution<br/>• Time-based patterns<br/>• Geographic patterns<br/>• Warehouse status"]
        OUTPUTS["📁 Report Outputs<br/>───────────────<br/>• fraud_by_category.png<br/>• fraud_types.png<br/>• time_patterns.png<br/>• fraud_by_location.png<br/>• fraud_report_*.txt"]
        REPORT_GEN --> OUTPUTS
    end

    subgraph INFRA["🐳 INFRASTRUCTURE (Docker Compose)"]
        direction LR
        ZK["ZooKeeper<br/>:2181"]
        KF["Kafka<br/>:9092"]
        PGC["PostgreSQL<br/>:5432"]
        AW["Airflow Webserver<br/>:8080"]
        AS["Airflow Scheduler"]
        ZK ~~~ KF ~~~ PGC ~~~ AW ~~~ AS
    end

    %% Data Flow Connections
    KAFKA ==>|"Streaming subscribe"| SPARK
    RULES ==>|"Valid transactions"| PG
    RULES ==>|"Fraud alerts"| PG
    PG ==>|"Batch read (6h)"| T1
    T3 ==>|"Parquet write"| PARQUET
    T4 ==>|"Store reconciliation"| PG
    PG ==>|"Query data"| REPORT_GEN
    PARQUET -.->|"Warehouse status"| REPORT_GEN

    %% Styling
    classDef ingestion fill:#1a73e8,stroke:#1557b0,color:#fff
    classDef hotpath fill:#d93025,stroke:#b31412,color:#fff
    classDef coldpath fill:#1967d2,stroke:#1557b0,color:#fff
    classDef storage fill:#0d652d,stroke:#0b4a22,color:#fff
    classDef analytics fill:#e37400,stroke:#b35c00,color:#fff
    classDef infra fill:#5f6368,stroke:#3c4043,color:#fff

    class INGESTION ingestion
    class HOT_PATH hotpath
    class COLD_PATH coldpath
    class STORAGE storage
    class ANALYTICS analytics
    class INFRA infra
```

---

## 📐 Data Flow Summary

| Path | Flow | Latency |
|------|------|---------|
| **Hot Path** | Producer → Kafka → Spark Streaming → PostgreSQL | ~2-3 seconds |
| **Cold Path** | PostgreSQL → Airflow ETL → Parquet Warehouse → Reconciliation Report | Every 6 hours |
| **Analytics** | PostgreSQL + Parquet → Report Generator → PNG/TXT reports | On-demand |

---

## 🔄 Lambda Architecture Layers

### Speed Layer (Hot Path)
- **Purpose:** Real-time fraud detection with sub-second latency
- **Technology:** Kafka + Spark Structured Streaming
- **Output:** Immediate writes to `valid_transactions` and `fraud_alerts` tables

### Batch Layer (Cold Path)
- **Purpose:** Comprehensive reconciliation and archival
- **Technology:** Apache Airflow orchestrating 4-task ETL DAG
- **Output:** Date-partitioned Parquet warehouse + reconciliation reports in PostgreSQL

### Serving Layer
- **Purpose:** Unified query interface for both real-time and batch results
- **Technology:** PostgreSQL (structured data) + Parquet files (columnar warehouse)
- **Consumers:** Analytics Report Generator, Airflow DAG, direct SQL queries

---

## 🐳 Container Network

All services communicate over `fraud-detection-network` (Docker bridge):

```
┌──────────────────────────────────────────────────────────────┐
│                   fraud-detection-network                    │
│                                                              │
│  ┌───────────┐   ┌──────────┐   ┌──────────────────────┐   │
│  │ ZooKeeper │◄──│  Kafka   │   │     PostgreSQL       │   │
│  │   :2181   │   │  :9092   │   │       :5432          │   │
│  └───────────┘   │  :29092  │   └──────────────────────┘   │
│                  └──────────┘            ▲                   │
│                       ▲                 │                    │
│                       │           ┌─────┴──────────────┐    │
│              ┌────────┘           │  Airflow Webserver  │   │
│              │                    │       :8080         │   │
│     ┌────────┴────────┐          └─────────────────────┘    │
│     │ Spark Streaming │          ┌─────────────────────┐    │
│     │   (external)    │          │  Airflow Scheduler   │   │
│     └─────────────────┘          └─────────────────────┘    │
│                                                              │
│     ┌─────────────────┐                                     │
│     │   Transaction   │          ./data/warehouse/          │
│     │    Producer     │          ├── valid_transactions/     │
│     │   (external)    │          │   └── date=YYYY-MM-DD/   │
│     └─────────────────┘          └── fraud_archive/         │
│                                      └── date=YYYY-MM-DD/   │
└──────────────────────────────────────────────────────────────┘
```
