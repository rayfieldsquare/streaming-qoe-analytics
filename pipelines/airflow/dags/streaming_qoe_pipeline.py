from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import logging
import os

# Import our custom classes
import sys
# set BASE_DIR to the pipelines dir of the project
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
PIPELINE_DIR = os.path.join(BASE_DIR, 'pipelines')
if PIPELINE_DIR not in sys.path:
    sys.path.insert(0, PIPELINE_DIR)
from processors.data_validators import TelemetryValidator
from processors.data_transformers import TelemetryTransformer

# ============================================
# CONFIGURATION
# ============================================

# Default arguments for all tasks in this DAG
default_args = {
    'owner': 'data-team',  # Who owns this pipeline
    'depends_on_past': False,  # Don't wait for previous runs to succeed
    'email': ['alerts@rayfieldsquare.com'],  # Who gets alerts (fake email)
    'email_on_failure': True,  # Send email if task fails
    'email_on_retry': False,
    'retries': 3,  # Try 3 times if task fails
    'retry_delay': timedelta(minutes=5),  # Wait 5 min between retries
}

# File paths
DATA_DIR = os.path.join(BASE_DIR, 'data')
RAW_DATA_FILE = f'{DATA_DIR}/streaming_telemetry.csv'
CLEAN_DATA_FILE = f'{DATA_DIR}/streaming_telemetry_clean.csv'
TRANSFORMED_DATA_FILE = f'{DATA_DIR}/streaming_telemetry_transformed.csv'

# ============================================
# TASK FUNCTIONS
# ============================================

def task_1_ingest_data(**context):
    """
    Task 1: Ingest raw telemetry data.

    In reality, this would:
    - Pull data from S3, BigQuery, or Kafka
    - Handle incremental loads (only new data)

    For now, we just verify the file exists.
    """
    logging.info("📥 Task 1: Ingesting raw data...")

    if not os.path.exists(RAW_DATA_FILE):
        raise FileNotFoundError(f"Raw data file not found: {RAW_DATA_FILE}")

    # Load data to check it
    df = pd.read_csv(RAW_DATA_FILE)

    logging.info(f"✅ Successfully loaded {len(df):,} raw records")

    # Push metadata to XCom (Airflow's way of sharing data between tasks)
    context['ti'].xcom_push(key='raw_record_count', value=len(df))

    return len(df)


def task_2_validate_data(**context):
    """
    Task 2: Validate and clean data.

    This is CRITICAL - bad data in = bad insights out!
    """
    logging.info("🔍 Task 2: Validating data...")

    # Load raw data
    df = pd.read_csv(RAW_DATA_FILE)

    # Validate
    validator = TelemetryValidator()
    is_valid, clean_df, report = validator.validate_all(df)

    # Store quality metrics
    context['ti'].xcom_push(key='quality_score', value=report['data_quality_score'])
    context['ti'].xcom_push(key='clean_record_count', value=len(clean_df))

    if not is_valid:
        # Quality score too low - fail the pipeline!
        raise ValueError(
            f"Data quality score {report['data_quality_score']}% is below threshold (95%)"
        )

    # Save cleaned data
    clean_df.to_csv(CLEAN_DATA_FILE, index=False)

    logging.info(f"✅ Validation passed! Quality score: {report['data_quality_score']}%")
    logging.info(f"✅ Saved {len(clean_df):,} clean records")

    return report['data_quality_score']


def task_3_transform_data(**context):
    """
    Task 3: Transform and enrich data.

    Add all the calculated metrics and categories.
    """
    logging.info("🔧 Task 3: Transforming data...")

    # Load clean data
    df = pd.read_csv(CLEAN_DATA_FILE)

    # Transform
    transformer = TelemetryTransformer()
    transformed_df = transformer.transform_all(df)

    # Save transformed data
    transformed_df.to_csv(TRANSFORMED_DATA_FILE, index=False)

    # Push metrics
    context['ti'].xcom_push(key='transformed_record_count', value=len(transformed_df))
    context['ti'].xcom_push(key='feature_count', value=len(transformed_df.columns))

    logging.info(f"✅ Transformation complete!")
    logging.info(f"✅ Created {len(transformed_df.columns)} features")

    return len(transformed_df)


def task_4_calculate_aggregates(**context):
    """
    Task 4: Calculate daily aggregates.

    Instead of storing every single session, we also calculate
    summary statistics by day/device/country.

    WHY? Makes dashboards load faster!
    """
    logging.info("📊 Task 4: Calculating aggregates...")

    # Load transformed data
    df = pd.read_csv(TRANSFORMED_DATA_FILE, parse_dates=['timestamp'])

    # Add date column for grouping
    df['date'] = df['timestamp'].dt.date

    # Aggregate by date, device, country
    aggregates = df.groupby(['date', 'device_type', 'country_code']).agg({
        'session_id': 'count',  # Number of sessions
        'startup_time_ms': ['mean', 'median', 'std'],  # Startup stats
        'rebuffer_count': 'mean',  # Average rebuffers
        'rebuffer_ratio': 'mean',  # Average rebuffer %
        'overall_qoe_score': 'mean',  # Average QoE
        'bitrate_kbps': 'mean',  # Average bitrate
        'session_duration_sec': 'sum'  # Total watch time
    }).reset_index()

    # Flatten column names
    aggregates.columns = [
        'date', 'device_type', 'country_code',
        'session_count',
        'avg_startup_ms', 'median_startup_ms', 'std_startup_ms',
        'avg_rebuffer_count', 'avg_rebuffer_ratio',
        'avg_qoe_score', 'avg_bitrate_kbps',
        'total_watch_time_sec'
    ]

    # Save aggregates
    agg_file = f'{DATA_DIR}/streaming_qoe_daily_aggregates.csv'
    aggregates.to_csv(agg_file, index=False)

    logging.info(f"✅ Created {len(aggregates):,} aggregate rows")

    context['ti'].xcom_push(key='aggregate_count', value=len(aggregates))

    return len(aggregates)


def task_5_data_quality_report(**context):
    """
    Task 5: Generate and save data quality report.

    This creates a summary that data engineers review daily.
    """
    logging.info("📋 Task 5: Generating quality report...")

    # Pull metrics from previous tasks
    ti = context['ti']
    raw_count = ti.xcom_pull(task_ids='ingest_data', key='raw_record_count')
    clean_count = ti.xcom_pull(task_ids='validate_data', key='clean_record_count')
    quality_score = ti.xcom_pull(task_ids='validate_data', key='quality_score')
    transformed_count = ti.xcom_pull(task_ids='transform_data', key='transformed_record_count')
    feature_count = ti.xcom_pull(task_ids='transform_data', key='feature_count')

    # Calculate metrics
    data_loss_pct = ((raw_count - clean_count) / raw_count * 100) if raw_count else 0

    # Create report
    report = {
        'pipeline_run_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'raw_records': raw_count,
        'clean_records': clean_count,
        'records_dropped': raw_count - clean_count,
        'data_loss_percentage': round(data_loss_pct, 2),
        'quality_score': quality_score,
        'features_created': feature_count,
        'pipeline_status': 'SUCCESS'
    }

    # Save report
    report_file = f'{DATA_DIR}/quality_reports/report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'

    import json
    os.makedirs(os.path.dirname(report_file), exist_ok=True)
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)

    logging.info(f"✅ Quality report saved to {report_file}")
    logging.info(f"\n📊 PIPELINE SUMMARY:")
    logging.info(f"  Raw Records: {raw_count:,}")
    logging.info(f"  Clean Records: {clean_count:,}")
    logging.info(f"  Data Loss: {data_loss_pct:.2f}%")
    logging.info(f"  Quality Score: {quality_score}%")

    return report


def task_6_send_success_notification(**context):
    """
    Task 6: Send notification that pipeline succeeded.

    In production, this would:
    - Send Slack message
    - Update monitoring dashboard
    - Trigger downstream pipelines
    """
    logging.info("📧 Task 6: Sending success notification...")

    # In reality, you'd use Slack API or email here
    logging.info("✅ Pipeline completed successfully!")
    logging.info("✅ (Would send Slack notification here)")

    return "Notification sent"


# ============================================
# DEFINE THE DAG
# ============================================

# Create the DAG
dag = DAG(
    'streaming_qoe_pipeline',  # DAG name (must be unique)
    default_args=default_args,
    description='Streaming Service QoE telemetry processing pipeline',
    schedule='@daily',  # Run every day at midnight
    start_date=datetime.now() - timedelta(days=1),  # Started yesterday (so it runs now)
    catchup=False,  # Don't backfill historical runs
    tags=['streaming', 'qoe', 'analytics'],  # Tags for organization
)

# ============================================
# DEFINE TASKS
# ============================================

# Task 1: Ingest
ingest_task = PythonOperator(
    task_id='ingest_data',
    python_callable=task_1_ingest_data,
    dag=dag,
)

# Task 2: Validate
validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=task_2_validate_data,
    dag=dag,
)

# Task 3: Transform
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=task_3_transform_data,
    dag=dag,
)

# Task 4: Aggregate
aggregate_task = PythonOperator(
    task_id='calculate_aggregates',
    python_callable=task_4_calculate_aggregates,
    dag=dag,
)

# Task 5: Quality Report
quality_report_task = PythonOperator(
    task_id='generate_quality_report',
    python_callable=task_5_data_quality_report,
    dag=dag,
)

# Task 6: Notification
notify_task = PythonOperator(
    task_id='send_notification',
    python_callable=task_6_send_success_notification,
    dag=dag,
)

# ============================================
# DEFINE TASK DEPENDENCIES (THE FLOW!)
# ============================================

# This defines the order tasks run in:
# ingest → validate → transform → aggregate → quality_report → notify
#
# Using >> means "run this, THEN run that"

ingest_task >> validate_task >> transform_task >> aggregate_task >> quality_report_task >> notify_task

# Visual representation of the flow:
#
#     ┌─────────┐
#     │ Ingest  │
#     └────┬────┘
#          │
#          ▼
#     ┌─────────┐
#     │Validate │
#     └────┬────┘
#          │
#          ▼
#     ┌─────────┐
#     │Transform│
#     └────┬────┘
#          │
#          ▼
#     ┌─────────┐
#     │Aggregate│
#     └────┬────┘
#          │
#          ▼
#     ┌─────────┐
#     │ Report  │
#     └────┬────┘
#          │
#          ▼
#     ┌─────────┐
#     │ Notify  │
#     └─────────┘
