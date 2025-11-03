from datetime import datetime
from utils.s3_handler import S3Handler
from utils.data_scraper import DataScraper
from utils.logger import setup_logger

logger = setup_logger(__name__)

class MonetaryFlowModeler:
    def __init__(self, config):
        self.config = config
        self.s3_handler = S3Handler(config['s3_bucket'], config['s3_base_path'])
        self.scraper = DataScraper()
        logger.info("Monetary Flow Modeler initialized", extra={'extra_data': {'agent': 'monetary_flow_modeler'}})
    
    def model_monetary_flows(self):
        try:
            # Lecture des données précédentes
            sector_data = self.s3_handler.read_json(f"{self.config['output_destinations']['intermediate_json']}02_sector_taxonomy.json")
            impact_data = self.s3_handler.read_json(f"{self.config['output_destinations']['intermediate_json']}03_sector_impact_coefficients.json")
            
            # Calcul du contexte macro
            macro_context = self._calculate_macro_context()
            
            # Modélisation des flux sectoriels
            sector_flows = self._model_sector_flows(sector_data['sectors'], impact_data['laws_analyzed'])
            
            # Création de l'output JSON
            output_data = {
                "modeling_date": datetime.now().strftime("%Y-%m-%d"),
                "total_market_cap": sum(s['current_market_cap'] for s in sector_flows),
                "macro_context": macro_context,
                "sector_flows": sector_flows
            }
            
            # Sauvegarde
            output_path = f"{self.config['output_destinations']['intermediate_json']}04_monetary_flow_model.json"
            self.s3_handler.write_json(output_path, output_data)
            
            logger.info(f"Monetary flow modeling completed: {len(sector_flows)} sectors modeled", 
                       extra={'extra_data': {'agent': 'monetary_flow_modeler', 'sectors_count': len(sector_flows)}})
            
            return output_data
            
        except Exception as e:
            logger.error(f"Monetary flow modeling failed: {str(e)}", 
                        extra={'extra_data': {'agent': 'monetary_flow_modeler', 'error': str(e)}})
            raise
    
    def _calculate_macro_context(self):
        # Récupération des données FED
        fed_data = self.scraper.get_fed_data()
        
        # Calcul des flux de capitaux
        total_market_cap = 50000000000000  # 50T USD estimation
        equity_flow_rate = 0.02  # 2% flow rate
        
        return {
            "fed_rate_scenario": fed_data,
            "capital_flows": {
                "entering_equity_market": total_market_cap * equity_flow_rate * 0.6,
                "exiting_equity_market": total_market_cap * equity_flow_rate * 0.4,
                "net_flow": total_market_cap * equity_flow_rate * 0.2
            }
        }
    
    def _model_sector_flows(self, sectors, laws_analyzed):
        sector_flows = []
        
        # Création d'un mapping des impacts par secteur
        sector_impacts = {}
        for law in laws_analyzed:
            for impact in law['sector_impacts']:
                sector_id = impact['sector_id']
                if sector_id not in sector_impacts:
                    sector_impacts[sector_id] = []
                sector_impacts[sector_id].append(impact)
        
        for sector in sectors:
            sector_id = sector['sector_id']
            current_market_cap = sector['total_market_cap']
            
            # Calcul de l'impact net avec pondération par loi
            net_impact_coefficient = 0
            if sector_id in sector_impacts:
                coefficients = [imp['impact_coefficient'] for imp in sector_impacts[sector_id]]
                # Moyenne pondérée des coefficients
                net_impact_coefficient = sum(coefficients) / len(coefficients) if coefficients else 0
                logger.info(f"Sector {sector_id}: coefficients {coefficients} → net coefficient {net_impact_coefficient}")
            else:
                logger.warning(f"No impact found for sector {sector_id}")
            
            # Modélisation des flux avec coefficient réel
            impact_direction = self._determine_impact_direction(net_impact_coefficient)
            monetary_impact = self._calculate_monetary_impact(current_market_cap, net_impact_coefficient, sectors)
            
            logger.info(f"Sector {sector_id}: coefficient {net_impact_coefficient} → direction {impact_direction} → net change ${monetary_impact['net_change']/1e9:.1f}B")
            
            sector_flow = {
                "sector_id": sector_id,
                "sector_name": sector['sector_name'],
                "current_market_cap": current_market_cap,
                "value_creation_sources": self._get_value_creation_sources(sector),
                "value_chain": sector.get('value_chain_description', ''),
                "impact_direction": impact_direction,
                "monetary_impact": monetary_impact,
                "interest_rate_sensitivity": self._calculate_rate_sensitivity(sector_id)
            }
            
            sector_flows.append(sector_flow)
        
        return sector_flows
    
    def _determine_impact_direction(self, coefficient):
        if coefficient >= 6:
            return "growth"
        elif coefficient <= 4:
            return "decline"
        else:
            return "stagnation"
    
    def _calculate_monetary_impact(self, market_cap, coefficient, all_sectors):
        # Impact plus réaliste basé sur le coefficient
        if coefficient >= 8:  # Impact très positif
            impact_rate = 0.15  # +15%
        elif coefficient >= 6:  # Impact positif
            impact_rate = 0.08  # +8%
        elif coefficient <= 2:  # Impact très négatif
            impact_rate = -0.20  # -20%
        elif coefficient <= 4:  # Impact négatif
            impact_rate = -0.12  # -12%
        else:  # Impact neutre
            impact_rate = 0.02  # +2% (croissance normale)
        
        capital_change = market_cap * impact_rate
        
        # Sources et destinations plus réalistes
        source_sectors = []
        destination_sectors = []
        
        if capital_change > 0:  # Croissance
            source_sectors = [
                {"sector_id": "external_investment", "amount": capital_change * 0.6, "rationale": "New institutional investment"},
                {"sector_id": "sector_rotation", "amount": capital_change * 0.4, "rationale": "Capital rotation from other sectors"}
            ]
        else:  # Décroissance
            destination_sectors = [
                {"sector_id": "defensive_assets", "amount": abs(capital_change) * 0.5, "rationale": "Flight to defensive assets"},
                {"sector_id": "alternative_sectors", "amount": abs(capital_change) * 0.5, "rationale": "Reallocation to less impacted sectors"}
            ]
        
        return {
            "capital_inflow": max(0, capital_change),
            "capital_outflow": max(0, -capital_change),
            "net_change": capital_change,
            "source_sectors": source_sectors,
            "destination_sectors": destination_sectors
        }
    
    def _get_value_creation_sources(self, sector):
        sources_map = {
            "technology": ["Innovation", "Scalability", "Network effects"],
            "healthcare": ["R&D", "Patents", "Regulatory moats"],
            "financial": ["Interest margins", "Fee income", "Risk management"],
            "energy": ["Resource extraction", "Infrastructure", "Market position"]
        }
        return sources_map.get(sector['sector_id'], ["Operations", "Market share", "Efficiency"])
    
    def _calculate_rate_sensitivity(self, sector_id):
        sensitivity_map = {
            "financial": {"sensitivity_score": 0.8, "impact_description": "Highly sensitive to rate changes"},
            "real_estate": {"sensitivity_score": 0.7, "impact_description": "Very sensitive to rate changes"},
            "technology": {"sensitivity_score": -0.3, "impact_description": "Moderately inverse sensitive"},
            "utilities": {"sensitivity_score": -0.5, "impact_description": "Inverse sensitive to rates"}
        }
        return sensitivity_map.get(sector_id, {"sensitivity_score": 0.0, "impact_description": "Neutral sensitivity"})