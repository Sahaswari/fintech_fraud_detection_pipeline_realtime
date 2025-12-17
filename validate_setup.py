"""
Fraud Detection Pipeline - Setup Validation Script
Validates all components are ready before running the pipeline
"""

import subprocess
import sys
import socket
import psycopg2
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient
import requests
from pathlib import Path

class SetupValidator:
    def __init__(self):
        self.checks_passed = 0
        self.checks_failed = 0
        print("=" * 70)
        print("🔍 FRAUD DETECTION PIPELINE - SETUP VALIDATION")
        print("=" * 70)
        print()
    
    def print_check(self, name, status, message=""):
        """Print check result"""
        if status:
            print(f"✓ {name:<50} [PASS]")
            self.checks_passed += 1
        else:
            print(f"✗ {name:<50} [FAIL]")
            if message:
                print(f"  → {message}")
            self.checks_failed += 1
    
    def check_docker_running(self):
        """Check if Docker is running"""
        try:
            result = subprocess.run(
                ["docker", "ps"], 
                capture_output=True, 
                text=True,
                timeout=5
            )
            self.print_check("Docker is running", result.returncode == 0)
            return result.returncode == 0
        except Exception as e:
            self.print_check("Docker is running", False, str(e))
            return False
    
    def check_container(self, container_name):
        """Check if a specific container is running"""
        try:
            result = subprocess.run(
                ["docker", "ps", "--filter", f"name={container_name}", "--format", "{{.Names}}"],
                capture_output=True,
                text=True,
                timeout=5
            )
            running = container_name in result.stdout
            self.print_check(f"Container '{container_name}' running", running)
            return running
        except Exception as e:
            self.print_check(f"Container '{container_name}' running", False, str(e))
            return False
    
    def check_kafka(self):
        """Check Kafka connectivity"""
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers='localhost:9092',
                request_timeout_ms=5000
            )
            topics = admin_client.list_topics()
            admin_client.close()
            
            has_transactions = 'transactions' in topics
            self.print_check("Kafka topic 'transactions' exists", has_transactions, 
                           "Run kafka_setup.bat to create topics" if not has_transactions else "")
            return has_transactions
        except Exception as e:
            self.print_check("Kafka connectivity", False, str(e))
            return False
    
    def check_postgres(self):
        """Check PostgreSQL connectivity"""
        try:
            conn = psycopg2.connect(
                host='localhost',
                port=5432,
                database='fraud_detection',
                user='fraud_user',
                password='fraud_pass',
                connect_timeout=5
            )
            cursor = conn.cursor()
            
            # Check tables exist
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name IN ('valid_transactions', 'fraud_alerts', 'daily_reconciliation')
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
            cursor.close()
            conn.close()
            
            self.print_check("PostgreSQL connectivity", True)
            self.print_check("Table 'valid_transactions' exists", 'valid_transactions' in tables)
            self.print_check("Table 'fraud_alerts' exists", 'fraud_alerts' in tables)
            self.print_check("Table 'daily_reconciliation' exists", 'daily_reconciliation' in tables)
            
            return len(tables) == 3
        except Exception as e:
            self.print_check("PostgreSQL connectivity", False, str(e))
            return False
    
    def check_airflow(self):
        """Check Airflow webserver"""
        try:
            response = requests.get('http://localhost:8080/health', timeout=5)
            self.print_check("Airflow webserver", response.status_code == 200)
            return response.status_code == 200
        except Exception as e:
            self.print_check("Airflow webserver", False, "Not accessible at http://localhost:8080")
            return False
    
    def check_project_structure(self):
        """Check project directory structure"""
        base_path = Path('.')
        
        required_dirs = [
            'airflow/dags',
            'airflow/logs',
            'airflow/plugins',
            'data',
            'producers',
            'spark_jobs',
            'reports'
        ]
        
        required_files = [
            'docker-compose.yml',
            'init_db.sql',
            'kafka_setup.sh',
            'kafka_setup.bat',
            'requirements.txt',
            'producers/producer_transaction.py',
            'spark_jobs/fraud_detection_stream.py',
            'airflow/dags/fraud_detection_reconciliation.py',
            'reports/analytics_report_generator.py'
        ]
        
        for dir_path in required_dirs:
            exists = (base_path / dir_path).is_dir()
            self.print_check(f"Directory '{dir_path}' exists", exists)
        
        for file_path in required_files:
            exists = (base_path / file_path).is_file()
            self.print_check(f"File '{file_path}' exists", exists)
    
    def check_python_packages(self):
        """Check required Python packages"""
        required_packages = [
            'kafka-python',
            'pyspark',
            'psycopg2',
            'pandas',
            'matplotlib',
            'seaborn'
        ]
        
        for package in required_packages:
            try:
                __import__(package.replace('-', '_'))
                self.print_check(f"Python package '{package}'", True)
            except ImportError:
                self.print_check(f"Python package '{package}'", False, 
                               f"Install with: pip install {package}")
    
    def run_all_checks(self):
        """Run all validation checks"""
        print("\n1. DOCKER & CONTAINERS")
        print("-" * 70)
        self.check_docker_running()
        self.check_container("zookeeper")
        self.check_container("kafka")
        self.check_container("postgres")
        self.check_container("airflow-webserver")
        self.check_container("airflow-scheduler")
        
        print("\n2. SERVICES CONNECTIVITY")
        print("-" * 70)
        self.check_kafka()
        self.check_postgres()
        self.check_airflow()
        
        print("\n3. PROJECT STRUCTURE")
        print("-" * 70)
        self.check_project_structure()
        
        print("\n4. PYTHON DEPENDENCIES")
        print("-" * 70)
        self.check_python_packages()
        
        print("\n" + "=" * 70)
        print("VALIDATION SUMMARY")
        print("=" * 70)
        print(f"✓ Checks Passed: {self.checks_passed}")
        print(f"✗ Checks Failed: {self.checks_failed}")
        print("=" * 70)
        
        if self.checks_failed == 0:
            print("🎉 All checks passed! Your pipeline is ready to run.")
            print()
            print("Next steps:")
            print("1. python producers/producer_transaction.py")
            print("2. python spark_jobs/fraud_detection_stream.py")
            print("3. Open http://localhost:8080 for Airflow")
            return True
        else:
            print("⚠️  Some checks failed. Please fix the issues above.")
            return False


if __name__ == "__main__":
    validator = SetupValidator()
    success = validator.run_all_checks()
    sys.exit(0 if success else 1)
