"""
Competitor Company Crawler Module

This module crawls competitor information for all listed companies from Wisereport.
It collects target company information along with their competitors and stores the data in MongoDB.

Author: Stockelper Team
License: MIT
"""

import os
import sys
import requests
import FinanceDataReader as fdr
from time import sleep
from datetime import datetime
import json
import logging
from tqdm import tqdm

# Add module paths
sys.path.insert(0, '/opt/airflow')
sys.path.append('/opt/airflow/modules')

# Import database module
from modules.database import get_mongodb_client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CompetitorCompanyCrawler:
    """
    Competitor Company Crawler class for scraping competitor information from Wisereport.
    """

    def __init__(self, mongodb_uri=None):
        """
        Initialize the Competitor Company Crawler.

        Args:
            mongodb_uri (str): MongoDB connection URI
        """
        self.mongodb_uri = mongodb_uri or os.environ.get("MONGODB_URI", "mongodb://localhost:27017/")
        self.mongodb_client = None
        self.collection = None
        self.collection_name = "competitors"

        # Initialize MongoDB connection
        self._init_mongodb()

    def _init_mongodb(self):
        """Initialize MongoDB connection and collection."""
        try:
            self.mongodb_client = get_mongodb_client(self.mongodb_uri)
            if self.mongodb_client.connect():
                self.collection = self.mongodb_client.get_collection(self.collection_name)
                logger.info("Successfully connected to MongoDB.")
            else:
                logger.error("Failed to connect to MongoDB")
                self.collection = None
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            self.collection = None

    def get_all_stock_codes(self):
        """
        Get all listed company stock codes using FinanceDataReader.

        Returns:
            list: List of stock codes from KOSPI, KOSDAQ, and KONEX
        """
        logger.info("Loading KOSPI, KOSDAQ, KONEX stock codes...")
        try:
            kospi = fdr.StockListing("KOSPI")
            kosdaq = fdr.StockListing("KOSDAQ")
            konex = fdr.StockListing("KONEX")
            all_stocks = [kospi, kosdaq, konex]

            # Ensure 'Code' column is string and remove missing values
            codes = [code for df in all_stocks for code in df['Code'].dropna().astype(str).tolist()]
            logger.info(f"Found {len(codes)} stock codes in total.")
            return codes
        except Exception as e:
            logger.error(f"Failed to load stock codes: {e}")
            return []

    def fetch_html(self, url, retries=3, delay=1):
        """
        Fetch HTML content from the given URL with retry mechanism.

        Args:
            url (str): URL to fetch
            retries (int): Number of retry attempts
            delay (int): Delay between retries in seconds

        Returns:
            bytes: HTML content or None if failed
        """
        for i in range(retries):
            try:
                response = requests.get(url, timeout=10)
                response.raise_for_status()  # Raise exception if not 200 OK
                return response.content
            except requests.exceptions.RequestException as e:
                logger.warning(f"URL fetch error: {url} (attempt {i+1}/{retries}): {e}")
                sleep(delay)
        return None

    def parse_company_data(self, html_content):
        """
        Parse HTML content (JSON) to extract target company and competitor information.

        Args:
            html_content (bytes): HTML content containing JSON data

        Returns:
            tuple: (target_company, competitors) where target_company is dict and competitors is list
        """
        try:
            data = json.loads(html_content)
            if not data.get("oDt_header"):
                return None, []

            target_company = None
            competitors = []

            for company in data["oDt_header"]:
                company_info = {
                    "code": company.get("CMP_CD"),
                    "name": company.get("CMP_KOR"),
                    "market_value": company.get("MKT_VAL")
                }

                if company.get("SEQ") == 1:
                    target_company = company_info
                else:
                    competitors.append(company_info)

            return target_company, competitors
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"JSON parsing error: {e}")
            return None, []

    def crawl_competitors(self, test_mode=False, limit=None):
        """
        Main crawling method to collect stock codes, crawl data, and save to DB or return JSON.

        Args:
            test_mode (bool): If True, only process limited stocks and return JSON instead of saving to DB
            limit (int): Optional limit on number of stocks to process

        Returns:
            dict: Crawling results with statistics
        """
        if not self.collection and not test_mode:
            return {"success": False, "error": "MongoDB connection not available"}

        codes = self.get_all_stock_codes()
        if not codes:
            logger.error("No stock codes found. Exiting.")
            return {"success": False, "error": "No stock codes found"}

        # Apply limit for testing or specific requirements
        if test_mode or limit:
            original_count = len(codes)
            codes = codes[:limit or 5]
            logger.info(f"Processing {len(codes)} out of {original_count} stocks (test_mode={test_mode})")

        all_results = []
        successful_saves = 0
        errors = []
        logger.info("Starting competitor information crawling...")

        try:
            for code in tqdm(codes, desc="Crawling Competitors"):
                # Wisereport competitor data API endpoint
                url = f"https://comp.wisereport.co.kr/company/ajax/cF6001.aspx?cmp_cd={code}&finGubun=MAIN&sec_cd=FG000&frq=Y"
                html_content = self.fetch_html(url)

                if not html_content:
                    error_msg = f"[{code}] Data fetch failed"
                    logger.warning(error_msg)
                    errors.append(error_msg)
                    continue

                target_company, competitors = self.parse_company_data(html_content)

                if not target_company or not target_company.get("code"):
                    error_msg = f"[{code}] Parsing failed. No valid data found"
                    logger.warning(error_msg)
                    errors.append(error_msg)
                    continue

                # Create document to save
                document = {
                    "_id": target_company["code"],
                    "target_company": target_company,
                    "competitors": competitors,
                    "last_crawled_at": datetime.utcnow().isoformat()
                }

                if test_mode:
                    all_results.append(document)
                    successful_saves += 1
                else:
                    # Update in DB (Upsert: insert if not exists, update if exists)
                    try:
                        self.collection.update_one(
                            {"_id": document["_id"]},
                            {"$set": document},
                            upsert=True
                        )
                        successful_saves += 1
                        logger.debug(f"[{code}] Successfully saved to database.")
                    except Exception as e:
                        error_msg = f"[{code}] Database save failed: {e}"
                        logger.error(error_msg)
                        errors.append(error_msg)

                sleep(0.1)  # Delay to reduce server load

            result = {
                "success": True,
                "total_processed": len(codes),
                "successful_saves": successful_saves,
                "errors": errors,
                "test_mode": test_mode
            }

            if test_mode:
                result["data"] = all_results
                logger.info("--- Crawling Results (JSON Output) ---")
                print(json.dumps(all_results, indent=2, ensure_ascii=False))
                logger.info("[TEST MODE] JSON output completed.")
            else:
                logger.info("Competitor information crawling and database saving completed for all companies.")

            return result

        except Exception as e:
            logger.error(f"Unexpected error during crawling: {e}")
            return {"success": False, "error": str(e)}

        finally:
            # Close MongoDB connection
            if self.mongodb_client:
                self.mongodb_client.close()

    def get_crawl_statistics(self):
        """
        Get crawling statistics from MongoDB.

        Returns:
            dict: Statistics dictionary
        """
        if not self.collection:
            return {"error": "MongoDB connection not available"}

        try:
            total_companies = self.collection.count_documents({})

            # Get unique target companies
            companies = self.collection.distinct("target_company.name")

            # Get latest crawl time
            latest_doc = self.collection.find_one(
                {},
                sort=[("last_crawled_at", -1)]
            )
            latest_crawl = latest_doc["last_crawled_at"] if latest_doc else None

            return {
                "total_companies": total_companies,
                "unique_companies": len(companies),
                "latest_crawl": latest_crawl,
                "sample_companies": companies[:10]  # First 10 companies
            }

        except Exception as e:
            logger.error(f"Failed to get statistics: {e}")
            return {"error": str(e)}


def main(test_mode=False):
    """
    Main function for backward compatibility and testing.

    Args:
        test_mode (bool): If True, only process first 5 stocks and output JSON instead of saving to DB
    """
    crawler = CompetitorCompanyCrawler()
    result = crawler.crawl_competitors(test_mode=test_mode)

    if result.get("success"):
        logger.info(f"Crawling completed successfully. Processed: {result.get('successful_saves', 0)} companies")
    else:
        logger.error(f"Crawling failed: {result.get('error', 'Unknown error')}")

    return result

if __name__ == "__main__":
    # For testing: main(test_mode=True)
    # For actual DB saving: main(test_mode=False) or main()
    main(test_mode=False)
