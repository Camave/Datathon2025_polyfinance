import pandas as pd
from datetime import datetime
from utils.s3_handler import S3Handler
from utils.json_validator import JSONValidator
from utils.logger import setup_logger

logger = setup_logger(__name__)

class PortfolioExtractor:
    def __init__(self, config):
        self.config = config
        self.s3_handler = S3Handler(config['s3_bucket'], config['s3_base_path'])
        logger.info("Portfolio Extractor initialized", extra={'extra_data': {'agent': 'portfolio_extractor'}})
    
    def extract_portfolio(self):
        try:
            # Lecture du fichier CSV du portefeuille
            portfolio_file = self.config['input_sources']['portfolio_file']
            df = self.s3_handler.read_csv(portfolio_file)
            
            # Extraction des données
            companies = []
            total_market_value = 50000000000000  # 50T USD estimation S&P 500
            
            for _, row in df.iterrows():
                # Fonction pour convertir les nombres avec virgules
                def safe_float(value):
                    if isinstance(value, str):
                        # Suppression des guillemets triples et conversion
                        cleaned_value = value.replace('"""', '').replace('"', '').replace(',', '.')
                        return float(cleaned_value) if cleaned_value else 0
                    return float(value) if value else 0
                
                weight_percent = safe_float(row.get('Weight', row.get('Percentage', 0)))
                price = safe_float(row.get('Price', 0))
                
                # Calcul du market cap basé sur le poids dans l'indice
                market_cap = total_market_value * (weight_percent / 100) if weight_percent > 0 else price * 1000000000  # Fallback
                
                company = {
                    "ticker": str(row.get('Symbol', row.get('Ticker', ''))),
                    "company_name": str(row.get('Security', row.get('Company', ''))),
                    "weight_percent": weight_percent,
                    "market_cap": market_cap,
                    "currency": "USD"
                }
                companies.append(company)
            
            # Création de l'output JSON
            output_data = {
                "extraction_date": datetime.now().strftime("%Y-%m-%d"),
                "portfolio_type": "S&P 500",
                "total_companies": len(companies),
                "companies": companies
            }
            
            # Validation
            if not JSONValidator.validate_portfolio_data(output_data):
                raise ValueError("Portfolio data validation failed")
            
            # Sauvegarde
            output_path = f"{self.config['output_destinations']['intermediate_json']}01_portfolio_data.json"
            self.s3_handler.write_json(output_path, output_data)
            
            logger.info(f"Portfolio extraction completed: {len(companies)} companies", 
                       extra={'extra_data': {'agent': 'portfolio_extractor', 'companies_count': len(companies)}})
            
            return output_data
            
        except Exception as e:
            logger.error(f"Portfolio extraction failed: {str(e)}", 
                        extra={'extra_data': {'agent': 'portfolio_extractor', 'error': str(e)}})
            raise