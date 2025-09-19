"""
Competitor Crawler DAG

This DAG crawls competitor information for all listed companies from Wisereport
and stores the data in MongoDB.

Execution Steps:
1. Check MongoDB connection
2. Execute competitor crawling
3. Report results

Schedule: Daily at 00:00 UTC
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

# Import competitor crawler module
from modules.company_crawler.compete_company_crawler import CompetitorCompanyCrawler

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

# DAG definition
dag = DAG(
    dag_id='competitor_crawler',
    default_args=default_args,
    description='Competitor Company Crawling Pipeline',
    schedule_interval='0 0 * * *',  # Daily at UTC 0:00
    start_date=days_ago(0),
    catchup=False,
    tags=['crawler', 'mongodb', 'competitor'],
)


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


def crawl_competitor_data(**kwargs):
    """Execute competitor crawling"""
    try:
        logger.info("Starting competitor data crawling")

        # Get MongoDB URI from environment or use default
        mongodb_uri = os.environ.get('MONGODB_URI', 'mongodb://localhost:27017/')

        # Create crawler instance and run crawling
        crawler = CompetitorCompanyCrawler(mongodb_uri=mongodb_uri)
        result = crawler.crawl_competitors(test_mode=False)

        if result.get('success'):
            logger.info(f"Competitor crawling completed successfully. Processed: {result.get('successful_saves', 0)} companies")

            # Store result in XCom
            ti = kwargs.get('ti')
            if ti:
                ti.xcom_push(key='crawl_result', value={'status': 'success', 'result': result})
            return True
        else:
            error_msg = result.get('error', 'Unknown error')
            logger.error(f"Competitor crawling failed: {error_msg}")
            raise Exception(error_msg)

    except Exception as e:
        logger.error(f"Competitor crawling failed: {e}")
        ti = kwargs.get('ti')
        if ti:
            ti.xcom_push(key='crawl_result', value={'status': 'error', 'error': str(e)})
        raise


def report_results(**kwargs):
    """Report crawling results"""
    try:
        logger.info("Starting competitor crawling results reporting")

        ti = kwargs.get('ti')
        if ti:
            crawl_result = ti.xcom_pull(task_ids='crawl_competitor_data', key='crawl_result')
            if crawl_result and crawl_result.get('status') == 'success':
                logger.info("Competitor crawling completed successfully")
            elif crawl_result and crawl_result.get('status') == 'error':
                logger.error(f"Error occurred during competitor crawling: {crawl_result.get('error')}")
            else:
                logger.info("Cannot verify competitor crawling results")
        else:
            # Check results directly from MongoDB
            with get_mongodb_client() as client:
                collection = client.get_collection("competitors")
                if collection:
                    count = collection.count_documents({})
                    logger.info(f"Competitors stored in MongoDB: {count}")

        logger.info("Competitor crawling results reporting completed")
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

crawl_competitors_task = PythonOperator(
    task_id='crawl_competitor_data',
    python_callable=crawl_competitor_data,
    dag=dag,
)

report_task = PythonOperator(
    task_id='report_results',
    python_callable=report_results,
    dag=dag,
)

# Task dependencies
check_mongodb >> crawl_competitors_task >> report_task

if __name__ == "__main__":
    dag.cli()
