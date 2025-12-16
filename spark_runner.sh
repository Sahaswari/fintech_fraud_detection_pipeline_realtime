#!/bin/bash

# Spark Fraud Detection Job Runner

echo "🚀 Starting Spark Fraud Detection Stream..."
echo "================================================"

# Install required packages first
pip install pyspark kafka-python psycopg2-binary

# Download required JARs if not present
if [ ! -d "jars" ]; then
    mkdir -p jars
    cd jars
    
    echo "📦 Downloading required JARs..."
    
    # Kafka connector
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar
    
    # Kafka clients
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar
    
    # PostgreSQL driver
    wget -q https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
    
    # Commons pool
    wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar
    
    cd ..
    echo "✓ JARs downloaded"
fi

# Run Spark job
spark-submit \
    --master local[*] \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 \
    --conf spark.sql.streaming.checkpointLocation=/tmp/checkpoint \
    --conf spark.driver.memory=2g \
    --conf spark.executor.memory=2g \
    spark_jobs/fraud_detection_stream.py

echo "Stream processing completed."