#!/bin/bash

# Kafka Topics Setup Script for Fraud Detection Pipeline

echo "Creating Kafka Topics for Fraud Detection System..."

# Create transactions topic (main input stream)
docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic transactions \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists \
  --config retention.ms=86400000

echo "✓ Created 'transactions' topic"

# Create fraud-alerts topic (for real-time fraud notifications)
docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic fraud-alerts \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists \
  --config retention.ms=604800000

echo "✓ Created 'fraud-alerts' topic"

# List all topics to verify
echo ""
echo "Current Kafka Topics:"
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Describe topics to show configuration
echo ""
echo "Topic Details:"
docker exec kafka kafka-topics --describe \
  --bootstrap-server localhost:9092 \
  --topic transactions

docker exec kafka kafka-topics --describe \
  --bootstrap-server localhost:9092 \
  --topic fraud-alerts

echo ""
echo "Kafka setup complete! ✓"