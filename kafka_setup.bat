@echo off
REM Kafka Topics Setup Script for Fraud Detection Pipeline (Windows)

echo ================================================
echo Creating Kafka Topics for Fraud Detection System
echo ================================================

REM Create transactions topic (main input stream)
echo.
echo Creating 'transactions' topic...
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic transactions --partitions 3 --replication-factor 1 --if-not-exists --config retention.ms=86400000

if %ERRORLEVEL% EQU 0 (
    echo [OK] Created 'transactions' topic
) else (
    echo [ERROR] Failed to create 'transactions' topic
)

REM Create fraud-alerts topic (for real-time fraud notifications)
echo.
echo Creating 'fraud-alerts' topic...
docker exec kafka kafka-topics --create --bootstrap-server localhost:9092 --topic fraud-alerts --partitions 1 --replication-factor 1 --if-not-exists --config retention.ms=604800000

if %ERRORLEVEL% EQU 0 (
    echo [OK] Created 'fraud-alerts' topic
) else (
    echo [ERROR] Failed to create 'fraud-alerts' topic
)

REM List all topics to verify
echo.
echo ================================================
echo Current Kafka Topics:
echo ================================================
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

REM Describe topics to show configuration
echo.
echo ================================================
echo Topic Details:
echo ================================================
echo.
echo --- transactions topic ---
docker exec kafka kafka-topics --describe --bootstrap-server localhost:9092 --topic transactions

echo.
echo --- fraud-alerts topic ---
docker exec kafka kafka-topics --describe --bootstrap-server localhost:9092 --topic fraud-alerts

echo.
echo ================================================
echo Kafka setup complete!
echo ================================================
pause
