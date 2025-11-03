import requests
import time
from bs4 import BeautifulSoup
from retrying import retry
from utils.logger import setup_logger

logger = setup_logger(__name__)

class DataScraper:
    def __init__(self, rate_limit=1.0):
        self.rate_limit = rate_limit
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=2000)
    def scrape_financial_data(self, ticker, source="yahoo"):
        time.sleep(self.rate_limit)
        try:
            if source == "yahoo":
                url = f"https://finance.yahoo.com/quote/{ticker}"
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                # Extraction basique - à adapter selon les besoins
                return {"ticker": ticker, "data": "scraped_data"}
            return {}
        except Exception as e:
            logger.error(f"Failed to scrape data for {ticker}", extra={'extra_data': {'error': str(e)}})
            return {}
    
    @retry(stop_max_attempt_number=3, wait_exponential_multiplier=2000)
    def get_fed_data(self):
        time.sleep(self.rate_limit)
        try:
            # Simulation d'appel FRED API
            return {
                "current_rate": 5.25,
                "projected_change": -0.5,
                "probability": 0.7,
                "timeline": "6 months"
            }
        except Exception as e:
            logger.error("Failed to get FED data", extra={'extra_data': {'error': str(e)}})
            return {}