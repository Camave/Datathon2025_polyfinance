import json
from datetime import datetime
from collections import defaultdict
from utils.s3_handler import S3Handler
from utils.json_validator import JSONValidator
from utils.logger import setup_logger

logger = setup_logger(__name__)

class SectorClassifier:
    def __init__(self, config):
        self.config = config
        self.s3_handler = S3Handler(config['s3_bucket'], config['s3_base_path'])
        logger.info("Sector Classifier initialized", extra={'extra_data': {'agent': 'sector_classifier'}})
    
    def classify_sectors(self):
        try:
            # Lecture des données du portfolio extractor (Agent 1)
            portfolio_data = self.s3_handler.read_json(f"{self.config['output_destinations']['intermediate_json']}01_portfolio_data.json")
            
            # Classification sectorielle basée sur les descriptions business
            sectors = self._create_sector_taxonomy(portfolio_data)
            
            # Création de l'output JSON
            output_data = {
                "classification_date": datetime.now().strftime("%Y-%m-%d"),
                "total_sectors": len(sectors),
                "methodology": "Business description and risk factor analysis with value chain clustering",
                "sectors": sectors
            }
            
            # Validation
            if not JSONValidator.validate_sector_taxonomy(output_data):
                raise ValueError("Sector taxonomy validation failed")
            
            # Sauvegarde
            output_path = f"{self.config['output_destinations']['intermediate_json']}02_sector_taxonomy.json"
            self.s3_handler.write_json(output_path, output_data)
            
            logger.info(f"Sector classification completed: {len(sectors)} sectors", 
                       extra={'extra_data': {'agent': 'sector_classifier', 'sectors_count': len(sectors)}})
            
            return output_data
            
        except Exception as e:
            logger.error(f"Sector classification failed: {str(e)}", 
                        extra={'extra_data': {'agent': 'sector_classifier', 'error': str(e)}})
            raise
    
    def _create_sector_taxonomy(self, portfolio_data):
        # Classification basée sur les tickers connus
        ticker_sectors = {
            "technology": ["NVDA", "MSFT", "AAPL", "AMZN", "META", "AVGO", "GOOGL", "GOOG", "TSLA", "ORCL", "CRM", "IBM", "INTU", "NOW", "TXN", "ANET", "QCOM", "ACN", "AMAT", "ADBE", "MU", "LRCX", "KLAC", "ADI", "PANW", "SNPS", "CDNS", "DELL", "CRWD", "INTC", "PTC", "VRSN", "TYL", "TTD", "NTAP", "CDW", "ADSK", "WDAY", "NXPI", "FTNT", "AXON", "DDOG", "EPAM", "GDDY", "FFIV", "TRMB", "IT", "SMCI", "HPE", "HPQ", "CTSH", "AKAM", "PAYC", "CPAY", "KEYS", "MCHP", "ON", "SWKS", "TER", "ZBRA", "AMD", "CSCO", "ADP"],
            "healthcare": ["LLY", "UNH", "JNJ", "ABBV", "MRK", "TMO", "ABT", "ISRG", "AMGN", "BSX", "DHR", "GILD", "SYK", "PFE", "VRTX", "BMY", "MDT", "REGN", "HCA", "CVS", "CI", "HUM", "ELV", "BIIB", "ZTS", "DXCM", "IDXX", "IQV", "PODD", "LH", "VTRS", "BAX", "BDX", "RMD", "ALGN", "MRNA", "WAT", "INCY", "RVTY", "WBA", "DVA", "MOH", "HSIC", "CRL"],
            "financial": ["BRK,B", "JPM", "V", "MA", "WFC", "MS", "GS", "BLK", "SCHW", "C", "SPGI", "AXP", "COF", "BX", "KKR", "PNC", "AJG", "BK", "FI", "USB", "TFC", "CME", "ICE", "MCO", "NDAQ", "CBOE", "TRV", "PGR", "AFL", "ALL", "CB", "AON", "MMC", "AIG", "PRU", "MET", "AMP", "CINF", "WRB", "STT", "NTRS", "RF", "FITB", "HBAN", "KEY", "CFG", "MTB", "STZ", "RJF", "BRO", "TROW", "BEN", "IVZ", "BAC"],
            "energy": ["XOM", "CVX", "COP", "EOG", "SLB", "PSX", "MPC", "VLO", "OXY", "KMI", "WMB", "OKE", "TRGP", "BKR", "HAL", "DVN", "FANG", "EQT", "APA", "CTRA"],
            "consumer_discretionary": ["BKNG", "HD", "MCD", "NKE", "LOW", "TJX", "SBUX", "ABNB", "MAR", "RCL", "ORLY", "AZO", "YUM", "CMG", "ROST", "TGT", "DECK", "DPZ", "ULTA", "LULU", "BBY", "DRI", "DLTR", "GM", "F", "TSCO", "LVS", "WYNN", "MGM", "CCL", "NCLH", "HAS", "LUV", "UAL", "DAL", "EXPE", "APTV", "TPR", "RL", "CHRW", "EXPD", "KMX", "LKQ", "MHK", "WHR", "POOL"],
            "industrials": ["CAT", "RTX", "BA", "UNP", "DE", "HON", "APH", "LMT", "GD", "NOC", "MMM", "TDG", "ETN", "ITW", "EMR", "PH", "CARR", "CMI", "FDX", "UPS", "GWW", "FAST", "PWR", "ROK", "DOV", "SWK", "TXT", "ALLE", "JCI", "OTIS", "IR", "ODFL", "JBHT", "CHRW", "EXPD", "LDOS", "LHX", "HII", "TKO", "GNRC", "WSM", "BLDR", "PHM", "LEN", "DHI", "NVR", "TOL"],
            "materials": ["LIN", "APD", "SHW", "ECL", "FCX", "NEM", "VMC", "MLM", "NUE", "STLD", "DOW", "LYB", "CF", "MOS", "ALB", "IFF", "PKG", "AMCR", "WY", "IP", "PPG", "EMN", "BG", "BALL", "AVY"],
            "real_estate": ["PLD", "AMT", "CCI", "EQIX", "PSA", "EXR", "WELL", "DLR", "O", "VICI", "SPG", "AVB", "EQR", "VTR", "ESS", "MAA", "UDR", "CPT", "HST", "ARE", "INVH", "BXP", "KIM", "REG", "FRT"],
            "telecommunications": ["TMUS", "VZ", "T", "CHTR", "CMCSA", "NFLX", "DIS", "WBD", "FOXA", "FOX", "NWSA", "NWS", "SBAC", "TTWO", "EA", "LYV", "MTCH", "RVTY"],
            "consumer_staples": ["PG", "KO", "PEP", "COST", "WMT", "PM", "MO", "MDLZ", "CL", "KMB", "GIS", "K", "HSY", "MKC", "SJM", "CPB", "CAG", "KHC", "KDP", "CHD", "CLX", "TAP", "HRL", "TSN", "ADM", "BF,B", "STZ"],
            "utilities": ["NEE", "SO", "CEG", "DUK", "AEP", "EXC", "XEL", "PEG", "ED", "ETR", "WEC", "PPL", "D", "PCG", "SRE", "AEE", "CMS", "DTE", "EIX", "NI", "LNT", "EVRG", "CNP", "ATO", "NRG", "VST", "FE", "ES", "PNW", "AWK", "AES"]
        }
        
        sectors = []
        sector_companies = defaultdict(list)
        
        # Classification des entreprises par ticker
        for company_data in portfolio_data.get('companies', []):
            ticker = company_data.get('ticker', '')
            assigned_sector = self._assign_sector_by_ticker(ticker, ticker_sectors)
            
            company_info = {
                "ticker": ticker,
                "company_name": company_data.get('company_name', ''),
                "market_cap": company_data.get('market_cap', 0),
                "sector_weight_percent": 0,  # Calculé après
                "business_summary": company_data.get('business_description', '')[:200] + "..." if len(company_data.get('business_description', '')) > 200 else company_data.get('business_description', '')
            }
            
            sector_companies[assigned_sector].append(company_info)
        
        # Création des secteurs avec calculs
        for sector_id, companies in sector_companies.items():
            total_market_cap = sum(c['market_cap'] for c in companies)
            
            # Calcul des poids sectoriels
            for company in companies:
                if total_market_cap > 0:
                    company['sector_weight_percent'] = (company['market_cap'] / total_market_cap) * 100
            
            sector = {
                "sector_id": sector_id,
                "sector_name": sector_id.replace('_', ' ').title(),
                "description": f"Companies operating in {sector_id.replace('_', ' ')} sector",
                "total_market_cap": total_market_cap,
                "companies_count": len(companies),
                "value_chain_description": self._get_value_chain_description(sector_id),
                "companies": companies
            }
            sectors.append(sector)
        
        return sectors
    
    def _assign_sector_by_ticker(self, ticker, ticker_sectors):
        for sector, tickers in ticker_sectors.items():
            if ticker in tickers:
                return sector
        
        # Classification secondaire pour les tickers non répertoriés
        return self._classify_unknown_ticker(ticker)
    
    def _classify_unknown_ticker(self, ticker):
        # Patterns de classification pour les tickers inconnus
        tech_patterns = ['TECH', 'SOFT', 'DATA', 'CYBER', 'AI', 'CLOUD']
        health_patterns = ['BIO', 'PHARM', 'MED', 'HEALTH', 'DRUG']
        finance_patterns = ['BANK', 'FUND', 'INVEST', 'CAPITAL', 'CREDIT']
        energy_patterns = ['OIL', 'GAS', 'ENERGY', 'PETRO', 'SOLAR']
        
        ticker_upper = ticker.upper()
        
        if any(pattern in ticker_upper for pattern in tech_patterns):
            return "technology"
        elif any(pattern in ticker_upper for pattern in health_patterns):
            return "healthcare"
        elif any(pattern in ticker_upper for pattern in finance_patterns):
            return "financial"
        elif any(pattern in ticker_upper for pattern in energy_patterns):
            return "energy"
        
        # Classification par longueur de ticker (heuristique)
        if len(ticker) <= 3:
            return "industrials"  # Tickers courts souvent industriels
        else:
            return "consumer_discretionary"  # Par défaut
    
    def _get_value_chain_description(self, sector_id):
        descriptions = {
            "technology": "R&D → Product Development → Manufacturing → Distribution → Support",
            "healthcare": "Research → Clinical Trials → Manufacturing → Distribution → Patient Care",
            "financial": "Capital Acquisition → Risk Assessment → Product Creation → Distribution → Service",
            "energy": "Exploration → Extraction → Refining → Distribution → Retail",
            "consumer_discretionary": "Design → Manufacturing → Marketing → Retail → After-sales",
            "industrials": "Raw Materials → Manufacturing → Assembly → Distribution → Maintenance",
            "materials": "Extraction → Processing → Manufacturing → Distribution → Application",
            "real_estate": "Development → Construction → Marketing → Sales/Leasing → Management",
            "telecommunications": "Infrastructure → Network → Service Delivery → Customer Support",
            "consumer_staples": "Sourcing → Manufacturing → Distribution → Retail → Consumption",
            "utilities": "Generation → Transmission → Distribution → Customer Service → Maintenance"
        }
        return descriptions.get(sector_id, "Generic value chain")