@echo off
REM Spark Fraud Detection Job Runner (Windows)

echo 🚀 Starting Spark Fraud Detection Stream...
echo ================================================

REM Install required packages first
pip install pyspark kafka-python psycopg2-binary

REM Download required JARs if not present
if not exist "jars" (
    mkdir jars
    pushd jars

    echo 📦 Downloading required JARs...

    REM Kafka connector
    curl -sLO https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar

    REM Kafka clients
    curl -sLO https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar

    REM PostgreSQL driver
    curl -sLO https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

    REM Commons pool
    curl -sLO https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

    popd
    echo ✓ JARs downloaded
)

REM Run Spark job
spark-submit ^
    --master local[*] ^
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 ^
    --conf spark.sql.streaming.checkpointLocation=/tmp/checkpoint ^
    --conf spark.driver.memory=2g ^
    --conf spark.executor.memory=2g ^
    spark_jobs/fraud_detection_stream.py

echo Stream processing completed.
