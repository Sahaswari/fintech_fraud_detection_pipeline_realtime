"""
Fraud Detection Pipeline - Spark Structured Streaming
Real-time fraud detection processing
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, current_timestamp, 
    expr, count, sum as spark_sum, lit, when, lag
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DoubleType, TimestampType
)
from pyspark.sql.window import Window
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FraudDetectionStream:
    def __init__(self):
        """Initialize Spark Session with Kafka support"""
        self.spark = SparkSession.builder \
            .appName("FraudDetectionStream") \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                   "org.postgresql:postgresql:42.6.0") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Database connection properties
        self.db_properties = {
            "user": "fraud_user",
            "password": "fraud_pass",
            "driver": "org.postgresql.Driver",
            "url": "jdbc:postgresql://postgres:5432/fraud_detection"
        }
        
        logger.info("✓ Spark Session initialized")
    
    def define_schema(self):
        """Define schema for incoming transaction data"""
        return StructType([
            StructField("transaction_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("merchant_category", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("location", StringType(), True)
        ])
    
    def read_kafka_stream(self):
        """Read transaction stream from Kafka"""
        schema = self.define_schema()
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "transactions") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON and extract fields
        transactions = df.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")
        
        # Convert timestamp string to TimestampType
        transactions = transactions.withColumn(
            "timestamp",
            col("timestamp").cast(TimestampType())
        )
        
        logger.info("✓ Kafka stream configured")
        return transactions
    
    def detect_high_value_fraud(self, transactions):
        """Detect high-value transactions (> $5000)"""
        high_value_fraud = transactions.filter(col("amount") > 5000)
        
        high_value_fraud = high_value_fraud.withColumn(
            "fraud_type", lit("HIGH_VALUE")
        ).withColumn(
            "fraud_reason", 
            expr("concat('Transaction amount $', CAST(amount AS STRING), ' exceeds $5000 threshold')")
        ).withColumn(
            "detected_at", current_timestamp()
        )
        
        return high_value_fraud
    
    def detect_impossible_travel(self, transactions):
        """
        Detect impossible travel: same user in different countries within 10 minutes
        Uses watermarking and windowing
        """
        # Define window for impossible travel detection (10 minutes)
        windowed = transactions \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                col("user_id"),
                window(col("timestamp"), "10 minutes")
            ) \
            .agg(
                count("*").alias("transaction_count"),
                expr("collect_list(location)").alias("locations"),
                expr("collect_list(struct(timestamp, transaction_id, merchant_category, amount, location))").alias("transactions")
            )
        
        # Detect if user has transactions in multiple countries
        impossible_travel = windowed.filter(
            expr("size(array_distinct(locations)) > 1")
        )
        
        # Explode transactions to create individual fraud records
        impossible_travel = impossible_travel \
            .select(
                col("user_id"),
                expr("explode(transactions)").alias("trans")
            ) \
            .select(
                col("trans.transaction_id").alias("transaction_id"),
                col("user_id"),
                col("trans.timestamp").alias("timestamp"),
                col("trans.merchant_category").alias("merchant_category"),
                col("trans.amount").alias("amount"),
                col("trans.location").alias("location"),
                lit("IMPOSSIBLE_TRAVEL").alias("fraud_type"),
                lit("User had transactions in multiple countries within 10 minutes").alias("fraud_reason"),
                current_timestamp().alias("detected_at")
            )
        
        return impossible_travel
    
    def write_to_postgres(self, df, table_name, mode="append"):
        """Write DataFrame to PostgreSQL"""
        def write_batch(batch_df, batch_id):
            try:
                batch_df.write \
                    .jdbc(
                        url=self.db_properties["url"],
                        table=table_name,
                        mode=mode,
                        properties=self.db_properties
                    )
                logger.info(f"✓ Batch {batch_id} written to {table_name}: {batch_df.count()} records")
            except Exception as e:
                logger.error(f"❌ Error writing batch {batch_id} to {table_name}: {str(e)}")
        
        return write_batch
    
    def write_to_console(self, df, query_name):
        """Write stream to console for monitoring"""
        return df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .queryName(query_name) \
            .start()
    
    def run(self):
        """Run the fraud detection streaming pipeline"""
        logger.info("=" * 70)
        logger.info("🚀 STARTING FRAUD DETECTION STREAM PROCESSOR")
        logger.info("=" * 70)
        
        # Read transaction stream
        transactions = self.read_kafka_stream()
        
        # Detect high-value fraud
        high_value_fraud = self.detect_high_value_fraud(transactions)
        
        # Write valid (non-high-value) transactions
        valid_transactions = transactions.filter(col("amount") <= 5000)
        
        # Select only required columns for valid_transactions table
        valid_trans_output = valid_transactions.select(
            "transaction_id", "user_id", "timestamp",
            "merchant_category", "amount", "location"
        )
        
        # Start writing streams
        logger.info("📊 Starting stream queries...")
        
        # Query 1: Write valid transactions to database
        valid_query = valid_trans_output.writeStream \
            .foreachBatch(self.write_to_postgres(valid_trans_output, "valid_transactions")) \
            .outputMode("append") \
            .queryName("valid_transactions_writer") \
            .start()
        
        # Query 2: Write high-value fraud to database
        high_value_query = high_value_fraud.writeStream \
            .foreachBatch(self.write_to_postgres(high_value_fraud, "fraud_alerts")) \
            .outputMode("append") \
            .queryName("high_value_fraud_writer") \
            .start()
        
        # Query 3: Console output for monitoring (high value fraud)
        console_query = high_value_fraud.select(
            "transaction_id", "user_id", "amount", 
            "location", "fraud_type"
        ).writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .queryName("fraud_monitor") \
            .start()
        
        logger.info("✓ All stream queries started successfully")
        logger.info("🔍 Monitoring for fraud patterns...")
        logger.info("=" * 70)
        
        # Wait for termination
        try:
            valid_query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("\n⚠️  Stream stopped by user")
            self.spark.stop()


if __name__ == "__main__":
    detector = FraudDetectionStream()
    detector.run()