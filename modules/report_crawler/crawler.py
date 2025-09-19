"""
Stock Report Crawler Module

This module crawls stock research reports from financial websites using Selenium WebDriver.
It processes the data and stores it in MongoDB with duplicate prevention.

Author: Stockelper Team
License: MIT
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time

# Add module paths
sys.path.insert(0, '/opt/airflow')
sys.path.append('/opt/airflow/modules')

# Import database module
from modules.database import get_mongodb_client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StockReportCrawler:
    """
    Stock Report Crawler class for scraping financial research reports.
    """
    
    def __init__(self, mongodb_uri=None, headless=True):
        """
        Initialize the Stock Report Crawler.

        Args:
            mongodb_uri (str): MongoDB connection URI
            headless (bool): Whether to run Chrome in headless mode
        """
        self.mongodb_uri = mongodb_uri or os.environ.get("MONGODB_URI", "mongodb://localhost:27017/")
        self.headless = headless
        self.driver = None
        self.mongodb_client = None
        self.collection = None

        # Initialize MongoDB connection
        self._init_mongodb()

    def _init_mongodb(self):
        """Initialize MongoDB connection and collection."""
        try:
            self.mongodb_client = get_mongodb_client(self.mongodb_uri)
            if self.mongodb_client.connect():
                self.collection = self.mongodb_client.get_collection("stock_reports")

                # Create indexes for duplicate prevention
                self.collection.create_index([
                    ('date', 1),
                    ('company', 1),
                    ('code', 1)
                ], unique=True)

                logger.info("Successfully connected to MongoDB and created indexes.")
            else:
                logger.error("Failed to connect to MongoDB")
                self.collection = None
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            self.collection = None
    
    def setup_driver(self):
        """Setup Chrome WebDriver with appropriate options."""
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument("--headless")
        
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            logger.info("Chrome WebDriver initialized successfully.")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Chrome WebDriver: {e}")
            return False
    
    def crawl_daily_report(self, daily=True, start_date=None, end_date=None):
        """
        Crawl stock reports for the specified date range.
        
        Args:
            daily (bool): If True, crawl today's reports only
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            
        Returns:
            dict: Crawling results with statistics
        """
        if not self.setup_driver():
            return {"success": False, "error": "Failed to setup WebDriver"}
        
        if not self.collection:
            return {"success": False, "error": "MongoDB connection not available"}
        
        try:
            # Determine date range
            if daily:
                target_date = datetime.now().strftime("%Y-%m-%d")
                date_range = [target_date]
            else:
                # Parse date range if provided
                if start_date and end_date:
                    start = datetime.strptime(start_date, "%Y-%m-%d")
                    end = datetime.strptime(end_date, "%Y-%m-%d")
                    date_range = [(start + timedelta(days=x)).strftime("%Y-%m-%d") 
                                 for x in range((end - start).days + 1)]
                else:
                    # Default to last 7 days
                    date_range = [(datetime.now() - timedelta(days=x)).strftime("%Y-%m-%d") 
                                 for x in range(7)]
            
            total_reports = 0
            successful_saves = 0
            errors = []
            
            for date_str in date_range:
                logger.info(f"Crawling reports for date: {date_str}")
                
                try:
                    reports = self._crawl_reports_for_date(date_str)
                    total_reports += len(reports)
                    
                    # Save reports to MongoDB
                    for report in reports:
                        try:
                            self.collection.update_one(
                                {
                                    'date': report['date'],
                                    'company': report['company'],
                                    'code': report['code']
                                },
                                {'$set': report},
                                upsert=True
                            )
                            successful_saves += 1
                            logger.debug(f"Saved report: {report['company']} - {report['title']}")
                        except Exception as e:
                            error_msg = f"Failed to save report for {report.get('company', 'Unknown')}: {e}"
                            logger.error(error_msg)
                            errors.append(error_msg)
                
                except Exception as e:
                    error_msg = f"Failed to crawl reports for {date_str}: {e}"
                    logger.error(error_msg)
                    errors.append(error_msg)
                
                # Add delay between dates to be respectful to the server
                time.sleep(1)
            
            return {
                "success": True,
                "total_reports": total_reports,
                "successful_saves": successful_saves,
                "errors": errors,
                "date_range": date_range
            }
            
        except Exception as e:
            logger.error(f"Unexpected error during crawling: {e}")
            return {"success": False, "error": str(e)}
        
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("WebDriver closed.")
            if self.mongodb_client:
                self.mongodb_client.close()
                logger.info("MongoDB connection closed.")
    
    def _crawl_reports_for_date(self, date_str):
        """
        Crawl reports for a specific date.
        
        Args:
            date_str (str): Date in YYYY-MM-DD format
            
        Returns:
            list: List of report dictionaries
        """
        reports = []
        
        try:
            # Navigate to the reports page (placeholder URL - replace with actual)
            url = f"https://<REPORT_WEBSITE>/reports?date={date_str}"
            self.driver.get(url)
            
            # Wait for the page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CLASS_NAME, "report-list"))
            )
            
            # Find all report elements
            report_elements = self.driver.find_elements(By.CLASS_NAME, "report-item")
            
            for element in report_elements:
                try:
                    report = self._extract_report_data(element, date_str)
                    if report:
                        reports.append(report)
                except Exception as e:
                    logger.warning(f"Failed to extract report data: {e}")
                    continue
            
            logger.info(f"Found {len(reports)} reports for {date_str}")
            
        except TimeoutException:
            logger.warning(f"Timeout waiting for page to load for date {date_str}")
        except Exception as e:
            logger.error(f"Error crawling reports for {date_str}: {e}")
        
        return reports
    
    def _extract_report_data(self, element, date_str):
        """
        Extract report data from a web element.
        
        Args:
            element: Selenium WebElement containing report data
            date_str (str): Date string
            
        Returns:
            dict: Report data dictionary
        """
        try:
            # Extract report information (adjust selectors based on actual website structure)
            company = element.find_element(By.CLASS_NAME, "company-name").text.strip()
            code = element.find_element(By.CLASS_NAME, "company-code").text.strip()
            title = element.find_element(By.CLASS_NAME, "report-title").text.strip()
            
            # Try to get summary if available
            try:
                summary = element.find_element(By.CLASS_NAME, "report-summary").text.strip()
            except NoSuchElementException:
                summary = ""
            
            # Try to get report URL if available
            try:
                url_element = element.find_element(By.TAG_NAME, "a")
                report_url = url_element.get_attribute("href")
            except NoSuchElementException:
                report_url = ""
            
            # Create report dictionary
            report = {
                "date": date_str,
                "company": company,
                "code": code,
                "title": title,
                "summary": summary,
                "url": report_url,
                "crawled_at": datetime.utcnow().isoformat()
            }
            
            return report
            
        except Exception as e:
            logger.warning(f"Failed to extract report data: {e}")
            return None
    
    def get_crawl_statistics(self, date_str=None):
        """
        Get crawling statistics from MongoDB.
        
        Args:
            date_str (str): Optional date to filter statistics
            
        Returns:
            dict: Statistics dictionary
        """
        if not self.collection:
            return {"error": "MongoDB connection not available"}
        
        try:
            query = {}
            if date_str:
                query["date"] = date_str
            
            total_reports = self.collection.count_documents(query)
            
            # Get unique companies
            companies = self.collection.distinct("company", query)
            
            # Get latest crawl time
            latest_doc = self.collection.find_one(
                query, 
                sort=[("crawled_at", -1)]
            )
            latest_crawl = latest_doc["crawled_at"] if latest_doc else None
            
            return {
                "total_reports": total_reports,
                "unique_companies": len(companies),
                "latest_crawl": latest_crawl,
                "companies": companies[:10]  # First 10 companies
            }
            
        except Exception as e:
            logger.error(f"Failed to get statistics: {e}")
            return {"error": str(e)}

def main():
    """Main function for testing the crawler."""
    crawler = StockReportCrawler()
    
    # Test crawling
    result = crawler.crawl_daily_report(daily=True)
    print(f"Crawling result: {result}")
    
    # Get statistics
    stats = crawler.get_crawl_statistics()
    print(f"Statistics: {stats}")

if __name__ == "__main__":
    main()
