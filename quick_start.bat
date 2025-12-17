@echo off
echo ================================================
echo FRAUD DETECTION PIPELINE - QUICK START
echo ================================================
echo.

echo Step 1: Validating Docker...
docker --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Docker not found. Please install Docker Desktop.
    pause
    exit /b 1
)
echo [OK] Docker found

echo.
echo Step 2: Starting Docker containers...
docker-compose up -d
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Failed to start containers
    pause
    exit /b 1
)
echo [OK] Containers started

echo.
echo Step 3: Waiting for services to be ready (30 seconds)...
timeout /t 30 /nobreak >nul

echo.
echo Step 4: Creating Kafka topics...
call kafka_setup.bat
if %ERRORLEVEL% NEQ 0 (
    echo [WARNING] Kafka topics may not be created. You may need to run kafka_setup.bat manually.
)

echo.
echo Step 5: Running validation...
python validate_setup.py
if %ERRORLEVEL% NEQ 0 (
    echo [WARNING] Some checks failed. Please review the output above.
    echo.
    echo Common issues:
    echo - Wait longer for services to start
    echo - Check Docker Desktop is running
    echo - Verify ports are not in use (5432, 8080, 9092)
)

echo.
echo ================================================
echo SETUP COMPLETE!
echo ================================================
echo.
echo Next steps:
echo 1. Open http://localhost:8080 in browser (Airflow)
echo 2. Login with admin / admin
echo 3. Add Postgres connection (see README.md)
echo.
echo To start the pipeline:
echo   Terminal 1: python producers/producer_transaction.py
echo   Terminal 2: python spark_jobs/fraud_detection_stream.py
echo.
echo To generate reports:
echo   python reports/analytics_report_generator.py
echo.
echo View detailed analysis:
echo   PROJECT_ANALYSIS_REPORT.md
echo.
pause
