"""
Stock Report Crawler DAG

This DAG crawls stock reports daily and stores them in MongoDB.

Execution Steps:
1. Check MongoDB connection
2. Execute report crawling
3. Report results

Schedule: Daily at 00:00 UTC (09:00 KST)
"""

from datetime import timedelta
import logging
import os
import sys
import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Add module paths
sys.path.insert(0, '/opt/airflow')
sys.path.append('/opt/airflow/modules')

# Import database module
from modules.database import get_mongodb_client, test_connection

# Import report crawler module
from modules.report_crawler.crawler import StockReportCrawler

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition (must be declared globally)
dag = DAG(
    dag_id='stock_report_crawler',
    default_args=default_args,
    description='Stock Report Crawling Pipeline',
    schedule_interval='0 0 * * *',  # Daily at UTC 0:00 (09:00 KST)
    start_date=days_ago(0),  # Start from today to ensure scheduling
    catchup=True,  # Execute missed tasks on restart
    tags=['report_crawler', 'mongodb', 'selenium'],
)

# Task functions
def check_mongodb_connection(**kwargs):
    """Check MongoDB connection"""
    try:
        connection_successful = test_connection()
        if connection_successful:
            logger.info("MongoDB connection successful")
            return True
        else:
            logger.error("MongoDB connection failed")
            raise Exception("Cannot connect to MongoDB")
    except Exception as e:
        logger.error(f"MongoDB connection failed: {e}")
        raise

def crawl_stock_report(**kwargs):
    """Execute report crawling"""
    try:
        # Crawl based on the actual date when DAG is executed
        date_to_crawl = pendulum.now('Asia/Seoul').format('YYYY/MM/DD')

        logger.info(f"Starting report crawling (target date: {date_to_crawl})")

        # Get MongoDB URI from environment or use default
        mongodb_uri = os.environ.get('MONGODB_URI', 'mongodb://localhost:27017/')
        crawler = StockReportCrawler(mongodb_uri=mongodb_uri)

        # Set start_date and end_date to the same value to crawl only one day
        result = crawler.crawl_daily_report(daily=False, start_date=date_to_crawl, end_date=date_to_crawl)
        logger.info("Report crawling completed")

        # Store result in XCom
        ti = kwargs.get('ti')
        if ti:
            ti.xcom_push(key='crawl_result', value={'status': 'success', 'result': result})
        return True

    except Exception as e:
        logger.error(f"Report crawling failed: {e}")
        ti = kwargs.get('ti')
        if ti:
            ti.xcom_push(key='crawl_result', value={'status': 'error', 'error': str(e)})
        raise

def report_results(**kwargs):
    """Report crawling results"""
    try:
        logger.info("Starting report crawling results reporting")

        ti = kwargs.get('ti')
        if ti:
            crawl_result = ti.xcom_pull(task_ids='crawl_stock_report', key='crawl_result')
            if crawl_result and crawl_result.get('status') == 'success':
                logger.info(f"Report crawling completed successfully. Result: {crawl_result.get('result')}")
            elif crawl_result and crawl_result.get('status') == 'error':
                logger.error(f"Error occurred during report crawling: {crawl_result.get('error')}")
            else:
                logger.info("Cannot verify report crawling results")
        else:
            # Check results directly from MongoDB
            with get_mongodb_client() as client:
                collection = client.get_collection("stock_reports")
                if collection:
                    count = collection.count_documents({})
                    today_count = collection.count_documents({
                        "date": {"$regex": pendulum.now().strftime("%Y/%m/%d")}
                    })
                    logger.info(f"Reports stored in MongoDB: Total {count}, Today {today_count}")

        logger.info("Report crawling results reporting completed")
        return True

    except Exception as e:
        logger.error(f"Error occurred during results reporting: {e}")
        return True

# Task definitions
check_mongodb = PythonOperator(
    task_id='check_mongodb_connection',
    python_callable=check_mongodb_connection,
    dag=dag,
)

crawl_report = PythonOperator(
    task_id='crawl_stock_report',
    python_callable=crawl_stock_report,
    dag=dag,
)

report = PythonOperator(
    task_id='report_results',
    python_callable=report_results,
    dag=dag,
)

# Task dependency setup
check_mongodb >> crawl_report >> report

if __name__ == "__main__":
    dag.cli()
