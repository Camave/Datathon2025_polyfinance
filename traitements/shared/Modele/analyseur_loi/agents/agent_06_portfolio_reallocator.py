import pandas as pd
from datetime import datetime
from utils.s3_handler import S3Handler
from utils.logger import setup_logger

logger = setup_logger(__name__)

class PortfolioReallocator:
    def __init__(self, config):
        self.config = config
        self.s3_handler = S3Handler(config['s3_bucket'], config['s3_base_path'])
        logger.info("Portfolio Reallocator initialized", extra={'extra_data': {'agent': 'portfolio_reallocator'}})
    
    def reallocate_portfolio(self):
        try:
            # Lecture des données nécessaires
            portfolio_data = self.s3_handler.read_json(f"{self.config['output_destinations']['intermediate_json']}01_portfolio_data.json")
            sector_data = self.s3_handler.read_json(f"{self.config['output_destinations']['intermediate_json']}02_sector_taxonomy.json")
            quantified_impacts = self.s3_handler.read_json(f"{self.config['output_destinations']['intermediate_json']}05_sector_quantified_impacts.json")
            
            # Création du mapping secteur -> entreprises
            sector_company_map = self._create_sector_company_map(sector_data['sectors'])
            
            # Création du mapping secteur -> impacts
            sector_impact_map = {forecast['sector_id']: forecast for forecast in quantified_impacts['sector_forecasts']}
            
            # Réallocation du portefeuille
            reallocated_companies = []
            sector_summary = {}
            
            original_portfolio_value = sum(company['market_cap'] * company['weight_percent'] / 100 
                                         for company in portfolio_data['companies'])
            
            for company in portfolio_data['companies']:
                reallocated_company = self._reallocate_single_company(
                    company, sector_company_map, sector_impact_map
                )
                reallocated_companies.append(reallocated_company)
                
                # Mise à jour du résumé sectoriel
                sector_id = reallocated_company['sector_id']
                if sector_id not in sector_summary:
                    sector_summary[sector_id] = {
                        "sector_id": sector_id,
                        "sector_name": reallocated_company['sector_name'],
                        "companies_count": 0,
                        "original_portfolio_weight": 0,
                        "projected_portfolio_weight": 0,
                        "contribution_to_portfolio_change": 0
                    }
                
                sector_summary[sector_id]['companies_count'] += 1
                sector_summary[sector_id]['original_portfolio_weight'] += company['weight_percent']
                sector_summary[sector_id]['projected_portfolio_weight'] += reallocated_company['projected_weight_percent']
            
            # Calcul de la valeur projetée du portefeuille
            projected_portfolio_value = sum(company['projected_market_cap'] * company['projected_weight_percent'] / 100 
                                          for company in reallocated_companies)
            
            total_impact_percent = ((projected_portfolio_value - original_portfolio_value) / original_portfolio_value * 100) if original_portfolio_value > 0 else 0
            
            # Calcul des contributions sectorielles
            for sector_id, summary in sector_summary.items():
                summary['contribution_to_portfolio_change'] = (
                    summary['projected_portfolio_weight'] - summary['original_portfolio_weight']
                )
            
            # Création de l'output JSON
            output_data = {
                "reallocation_date": datetime.now().strftime("%Y-%m-%d"),
                "original_portfolio_value": original_portfolio_value,
                "projected_portfolio_value": projected_portfolio_value,
                "total_impact_percent": total_impact_percent,
                "projection_timeline_years": max(forecast.get('absorption_timeline_years', 3) 
                                               for forecast in quantified_impacts['sector_forecasts']),
                "companies": reallocated_companies,
                "sector_summary": list(sector_summary.values())
            }
            
            # Sauvegarde JSON
            output_path = f"{self.config['output_destinations']['intermediate_json']}06_portfolio_reallocated.json"
            self.s3_handler.write_json(output_path, output_data)
            
            # Génération et sauvegarde du CSV final
            self._generate_final_csv(reallocated_companies)
            
            logger.info(f"Portfolio reallocation completed: {len(reallocated_companies)} companies reallocated", 
                       extra={'extra_data': {'agent': 'portfolio_reallocator', 'companies_count': len(reallocated_companies), 'total_impact': total_impact_percent}})
            
            return output_data
            
        except Exception as e:
            logger.error(f"Portfolio reallocation failed: {str(e)}", 
                        extra={'extra_data': {'agent': 'portfolio_reallocator', 'error': str(e)}})
            raise
    
    def _create_sector_company_map(self, sectors):
        sector_map = {}
        for sector in sectors:
            for company in sector['companies']:
                sector_map[company['ticker']] = {
                    'sector_id': sector['sector_id'],
                    'sector_name': sector['sector_name'],
                    'sector_market_cap': sector['total_market_cap'],
                    'sector_weight_percent': company['sector_weight_percent']
                }
        
        logger.info(f"Created sector mapping for {len(sector_map)} companies across {len(sectors)} sectors")
        return sector_map
    
    def _calculate_weight_factor(self, sector_weight_percent):
        """
        Calcule le facteur de pondération basé sur le poids de l'entreprise dans son secteur.
        Les entreprises avec un poids plus important subissent un impact plus fort.
        """
        # Normalisation : entreprises de 0-5% = facteur 0.6-0.8, 5-15% = facteur 0.8-1.2, >15% = facteur 1.2-1.5
        if sector_weight_percent <= 5:
            return 0.6 + (sector_weight_percent / 5) * 0.2  # 0.6 à 0.8
        elif sector_weight_percent <= 15:
            return 0.8 + ((sector_weight_percent - 5) / 10) * 0.4  # 0.8 à 1.2
        else:
            return min(1.5, 1.2 + ((sector_weight_percent - 15) / 20) * 0.3)  # 1.2 à 1.5 max
    
    def _reallocate_single_company(self, company, sector_company_map, sector_impact_map):
        ticker = company['ticker']
        
        # Identification du secteur
        if ticker in sector_company_map:
            sector_info = sector_company_map[ticker]
            sector_id = sector_info['sector_id']
        else:
            # Secteur par défaut si non trouvé
            sector_id = "other"
            sector_info = {
                'sector_id': sector_id,
                'sector_name': "Other",
                'sector_market_cap': 1000000000,
                'sector_weight_percent': 1.0
            }
        
        # Récupération des impacts sectoriels avec pondération
        if sector_id in sector_impact_map:
            sector_forecast = sector_impact_map[sector_id]
            base_sector_impact = sector_forecast['total_percent_change']
            
            # Pondération de l'impact selon le poids de l'entreprise dans le secteur
            company_sector_weight = sector_info['sector_weight_percent']
            weight_factor = self._calculate_weight_factor(company_sector_weight)
            
            applied_impact_percent = base_sector_impact * weight_factor
            yearly_projections = sector_forecast['yearly_projections']
            
            logger.info(f"Applying {applied_impact_percent:.2f}% impact to {ticker} (base: {base_sector_impact:.2f}%, weight factor: {weight_factor:.2f})")
        else:
            applied_impact_percent = 0
            yearly_projections = []
            logger.warning(f"No sector impact found for {ticker} in sector {sector_id}")
        
        # Calcul de la nouvelle market cap avec impact réel
        original_market_cap = company['market_cap']
        projected_market_cap = original_market_cap * (1 + applied_impact_percent / 100)
        
        # Calcul des nouveaux poids (proportionnel à l'impact)
        original_weight = company['weight_percent']
        projected_weight = original_weight * (1 + applied_impact_percent / 100)
        
        if applied_impact_percent != 0:
            logger.info(f"{ticker}: ${original_market_cap/1e9:.1f}B → ${projected_market_cap/1e9:.1f}B ({applied_impact_percent:+.2f}%)")
        
        # Génération des projections annuelles pour l'entreprise avec pondération
        company_yearly_projections = []
        if sector_id in sector_impact_map:
            weight_factor = self._calculate_weight_factor(sector_info['sector_weight_percent'])
            for projection in yearly_projections:
                weighted_cumulative_change = projection['cumulative_change_percent'] * weight_factor
                company_projection = {
                    "year": projection['year'],
                    "projected_market_cap": original_market_cap * (1 + weighted_cumulative_change / 100),
                    "projected_weight_percent": original_weight * (1 + weighted_cumulative_change / 100)
                }
                company_yearly_projections.append(company_projection)
        
        return {
            "ticker": ticker,
            "company_name": company['company_name'],
            "sector_id": sector_info['sector_id'],
            "sector_name": sector_info['sector_name'],
            "original_market_cap": original_market_cap,
            "original_weight_percent": original_weight,
            "sector_weight_percent": sector_info['sector_weight_percent'],
            "applied_sector_impact_percent": applied_impact_percent,
            "projected_market_cap": projected_market_cap,
            "projected_weight_percent": projected_weight,
            "absolute_value_change": projected_market_cap - original_market_cap,
            "yearly_projections": company_yearly_projections
        }
    
    def _generate_final_csv(self, reallocated_companies):
        # Création du DataFrame
        csv_data = []
        for company in reallocated_companies:
            row = {
                "Ticker": company['ticker'],
                "Company Name": company['company_name'],
                "Original Market Cap": company['original_market_cap'],
                "Projected Market Cap": company['projected_market_cap'],
                "Original Weight %": company['original_weight_percent'],
                "Projected Weight %": company['projected_weight_percent'],
                "Sector": company['sector_name'],
                "Impact %": company['applied_sector_impact_percent']
            }
            
            # Ajout des projections annuelles
            for projection in company['yearly_projections']:
                row[f"Year {projection['year']} Market Cap"] = projection['projected_market_cap']
                row[f"Year {projection['year']} Weight %"] = projection['projected_weight_percent']
            
            csv_data.append(row)
        
        df = pd.DataFrame(csv_data)
        
        # Sauvegarde du CSV
        csv_filename = "2025-08-15_composition_sp500_updated.csv"
        self.s3_handler.write_csv(csv_filename, df)
        
        logger.info(f"Final CSV generated and uploaded: {csv_filename}")
        
        return df