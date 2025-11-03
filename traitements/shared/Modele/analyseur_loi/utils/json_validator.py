import jsonschema
from utils.logger import setup_logger

logger = setup_logger(__name__)

class JSONValidator:
    @staticmethod
    def validate_portfolio_data(data):
        schema = {
            "type": "object",
            "required": ["extraction_date", "portfolio_type", "total_companies", "companies"],
            "properties": {
                "extraction_date": {"type": "string"},
                "portfolio_type": {"type": "string"},
                "total_companies": {"type": "integer"},
                "companies": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["ticker", "company_name", "weight_percent", "market_cap", "currency"],
                        "properties": {
                            "ticker": {"type": "string"},
                            "company_name": {"type": "string"},
                            "weight_percent": {"type": "number"},
                            "market_cap": {"type": "number"},
                            "currency": {"type": "string"}
                        }
                    }
                }
            }
        }
        return JSONValidator._validate(data, schema, "portfolio_data")
    
    @staticmethod
    def validate_sector_taxonomy(data):
        schema = {
            "type": "object",
            "required": ["classification_date", "total_sectors", "methodology", "sectors"],
            "properties": {
                "classification_date": {"type": "string"},
                "total_sectors": {"type": "integer"},
                "methodology": {"type": "string"},
                "sectors": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["sector_id", "sector_name", "description", "total_market_cap", "companies_count", "companies"],
                        "properties": {
                            "sector_id": {"type": "string"},
                            "sector_name": {"type": "string"},
                            "description": {"type": "string"},
                            "total_market_cap": {"type": "number"},
                            "companies_count": {"type": "integer"},
                            "companies": {"type": "array"}
                        }
                    }
                }
            }
        }
        return JSONValidator._validate(data, schema, "sector_taxonomy")
    
    @staticmethod
    def _validate(data, schema, data_type):
        try:
            jsonschema.validate(data, schema)
            logger.info(f"JSON validation successful for {data_type}")
            return True
        except jsonschema.ValidationError as e:
            logger.error(f"JSON validation failed for {data_type}: {e.message}")
            return False