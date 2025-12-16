"""
Fraud Detection Pipeline - Airflow DAG
Batch processing for transaction reconciliation and reporting
Runs every 6 hours
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import json
from pathlib import Path

# Default arguments
default_args = {
    'owner': 'fraud_detection_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'fraud_detection_reconciliation',
    default_args=default_args,
    description='6-hour batch reconciliation and fraud analysis',
    schedule_interval='0 */6 * * *',  # Every 6 hours
    catchup=False,
    tags=['fraud-detection', 'batch', 'reconciliation'],
)


def extract_transaction_data(**context):
    """
    Task 1: Extract transaction data from database
    """
    print("=" * 70)
    print("📊 TASK 1: EXTRACTING TRANSACTION DATA")
    print("=" * 70)
    
    # Get execution date
    execution_date = context['execution_date']
    start_time = execution_date - timedelta(hours=6)
    end_time = execution_date
    
    print(f"⏰ Time range: {start_time} to {end_time}")
    
    # Connect to database
    pg_hook = PostgresHook(postgres_conn_id='postgres_fraud')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Extract valid transactions
    valid_query = f"""
        SELECT 
            transaction_id, user_id, timestamp, 
            merchant_category, amount, location
        FROM valid_transactions
        WHERE timestamp >= '{start_time}' AND timestamp < '{end_time}'
    """
    
    valid_df = pd.read_sql(valid_query, conn)
    print(f"✓ Extracted {len(valid_df)} valid transactions")
    
    # Extract fraud alerts
    fraud_query = f"""
        SELECT 
            transaction_id, user_id, timestamp, merchant_category, 
            amount, location, fraud_type, fraud_reason
        FROM fraud_alerts
        WHERE timestamp >= '{start_time}' AND timestamp < '{end_time}'
    """
    
    fraud_df = pd.read_sql(fraud_query, conn)
    print(f"✓ Extracted {len(fraud_df)} fraud alerts")
    
    cursor.close()
    conn.close()
    
    # Save to XCom for next task
    context['task_instance'].xcom_push(key='valid_count', value=len(valid_df))
    context['task_instance'].xcom_push(key='fraud_count', value=len(fraud_df))
    context['task_instance'].xcom_push(key='valid_amount', value=float(valid_df['amount'].sum()))
    context['task_instance'].xcom_push(key='fraud_amount', value=float(fraud_df['amount'].sum()))
    
    # Save DataFrames as Parquet
    output_dir = Path('/opt/airflow/data/batch_output')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    batch_id = execution_date.strftime('%Y%m%d_%H%M%S')
    valid_df.to_parquet(output_dir / f'valid_transactions_{batch_id}.parquet')
    fraud_df.to_parquet(output_dir / f'fraud_alerts_{batch_id}.parquet')
    
    context['task_instance'].xcom_push(key='batch_id', value=batch_id)
    
    print("✓ Data extraction complete")
    print("=" * 70)


def transform_and_analyze(**context):
    """
    Task 2: Transform data and perform analytics
    """
    print("=" * 70)
    print("📈 TASK 2: TRANSFORMING AND ANALYZING DATA")
    print("=" * 70)
    
    # Get batch ID
    batch_id = context['task_instance'].xcom_pull(
        task_ids='extract_data', key='batch_id'
    )
    
    output_dir = Path('/opt/airflow/data/batch_output')
    
    # Load data
    valid_df = pd.read_parquet(output_dir / f'valid_transactions_{batch_id}.parquet')
    fraud_df = pd.read_parquet(output_dir / f'fraud_alerts_{batch_id}.parquet')
    
    print(f"📊 Processing batch: {batch_id}")
    
    # Analysis 1: Fraud by merchant category
    if len(fraud_df) > 0:
        fraud_by_category = fraud_df.groupby('merchant_category').agg({
            'transaction_id': 'count',
            'amount': ['sum', 'mean']
        }).round(2)
        fraud_by_category.columns = ['fraud_count', 'total_amount', 'avg_amount']
        fraud_by_category = fraud_by_category.sort_values('fraud_count', ascending=False)
        
        print("\n🚨 Fraud Analysis by Merchant Category:")
        print(fraud_by_category.to_string())
        
        # Save analysis
        fraud_by_category.to_csv(
            output_dir / f'fraud_by_category_{batch_id}.csv'
        )
    else:
        print("✓ No fraud detected in this period")
    
    # Analysis 2: Transaction volume by hour
    if len(valid_df) > 0:
        valid_df['timestamp'] = pd.to_datetime(valid_df['timestamp'])
        valid_df['hour'] = valid_df['timestamp'].dt.hour
        
        hourly_volume = valid_df.groupby('hour').agg({
            'transaction_id': 'count',
            'amount': 'sum'
        }).round(2)
        hourly_volume.columns = ['transaction_count', 'total_amount']
        
        print("\n⏰ Hourly Transaction Volume:")
        print(hourly_volume.to_string())
        
        hourly_volume.to_csv(
            output_dir / f'hourly_volume_{batch_id}.csv'
        )
    
    # Analysis 3: Fraud type breakdown
    if len(fraud_df) > 0:
        fraud_types = fraud_df.groupby('fraud_type').agg({
            'transaction_id': 'count',
            'amount': 'sum'
        }).round(2)
        fraud_types.columns = ['count', 'total_amount']
        
        print("\n🔍 Fraud Types Breakdown:")
        print(fraud_types.to_string())
        
        fraud_types.to_csv(
            output_dir / f'fraud_types_{batch_id}.csv'
        )
    
    context['task_instance'].xcom_push(key='analysis_complete', value=True)
    
    print("\n✓ Analysis complete")
    print("=" * 70)


def generate_reconciliation_report(**context):
    """
    Task 3: Generate reconciliation report
    """
    print("=" * 70)
    print("📋 TASK 3: GENERATING RECONCILIATION REPORT")
    print("=" * 70)
    
    # Get data from previous tasks
    ti = context['task_instance']
    valid_count = ti.xcom_pull(task_ids='extract_data', key='valid_count')
    fraud_count = ti.xcom_pull(task_ids='extract_data', key='fraud_count')
    valid_amount = ti.xcom_pull(task_ids='extract_data', key='valid_amount')
    fraud_amount = ti.xcom_pull(task_ids='extract_data', key='fraud_amount')
    batch_id = ti.xcom_pull(task_ids='extract_data', key='batch_id')
    
    total_count = valid_count + fraud_count
    total_amount = valid_amount + fraud_amount
    
    # Create report
    report = {
        'batch_id': batch_id,
        'execution_date': context['execution_date'].isoformat(),
        'summary': {
            'total_transactions': total_count,
            'total_amount': round(total_amount, 2),
            'valid_transactions': valid_count,
            'valid_amount': round(valid_amount, 2),
            'fraud_transactions': fraud_count,
            'fraud_amount': round(fraud_amount, 2),
            'fraud_percentage': round((fraud_count / total_count * 100) if total_count > 0 else 0, 2)
        },
        'metrics': {
            'avg_valid_transaction': round(valid_amount / valid_count, 2) if valid_count > 0 else 0,
            'avg_fraud_transaction': round(fraud_amount / fraud_count, 2) if fraud_count > 0 else 0
        }
    }
    
    # Print report
    print("\n" + "=" * 70)
    print("📊 RECONCILIATION REPORT")
    print("=" * 70)
    print(f"Batch ID: {report['batch_id']}")
    print(f"Execution Date: {report['execution_date']}")
    print("\nSUMMARY:")
    print(f"  Total Transactions: {report['summary']['total_transactions']}")
    print(f"  Total Amount: ${report['summary']['total_amount']:,.2f}")
    print(f"  Valid Transactions: {report['summary']['valid_transactions']}")
    print(f"  Valid Amount: ${report['summary']['valid_amount']:,.2f}")
    print(f"  Fraud Transactions: {report['summary']['fraud_transactions']}")
    print(f"  Fraud Amount: ${report['summary']['fraud_amount']:,.2f}")
    print(f"  Fraud Rate: {report['summary']['fraud_percentage']}%")
    print("\nMETRICS:")
    print(f"  Avg Valid Transaction: ${report['metrics']['avg_valid_transaction']:,.2f}")
    print(f"  Avg Fraud Transaction: ${report['metrics']['avg_fraud_transaction']:,.2f}")
    print("=" * 70)
    
    # Save report
    output_dir = Path('/opt/airflow/data/batch_output')
    report_file = output_dir / f'reconciliation_report_{batch_id}.json'
    
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"\n✓ Report saved to: {report_file}")
    
    # Store in database
    pg_hook = PostgresHook(postgres_conn_id='postgres_fraud')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    insert_query = """
        INSERT INTO daily_reconciliation 
        (report_date, total_transactions, total_amount, fraud_count, 
         fraud_amount, valid_count, valid_amount)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    cursor.execute(insert_query, (
        context['execution_date'].date(),
        total_count,
        total_amount,
        fraud_count,
        fraud_amount,
        valid_count,
        valid_amount
    ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print("✓ Report stored in database")
    print("=" * 70)


# Define tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_transaction_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_and_analyze',
    python_callable=transform_and_analyze,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_reconciliation_report,
    dag=dag,
)

# Define task dependencies
extract_task >> transform_task >> report_task