#!/usr/bin/env python3
"""
SP500 CSV Scorer - Score /100 basé sur fichier CSV réel (sans momentum)
"""

import json
import logging
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import boto3
import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parents[1]
ANALYZER_PATH = BASE_DIR / "analyseur_loi"
if ANALYZER_PATH.exists() and str(ANALYZER_PATH) not in sys.path:
    sys.path.append(str(ANALYZER_PATH))

from utils.s3_handler import S3Handler  # type: ignore


class LegislativeFundamentalConsolidator:
    """Builds a consolidated dataset mixing legislative outputs with fundamental scores."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        config_path = ANALYZER_PATH / "config.json"
        with open(config_path, "r", encoding="utf-8") as handle:
            self.config = json.load(handle)

        self.s3_handler = S3Handler(self.config["s3_bucket"], self.config["s3_base_path"])
        output_prefix = self.config["output_destinations"]["intermediate_json"]
        self.snapshot_key = f"{output_prefix}legislative_fundamental_snapshot.json"
        self.source_keys = {
            "law_coefficients": f"{output_prefix}03_sector_impact_coefficients.json",
            "sector_forecasts": f"{output_prefix}05_sector_quantified_impacts.json",
        }

    def build_snapshot(
        self,
        scoring_results: List[Dict],
        scoring_output_path: Optional[str],
    ) -> Optional[Dict]:
        try:
            law_coefficients = self.s3_handler.read_json(self.source_keys["law_coefficients"])
        except Exception as exc:
            self.logger.warning("Unable to load legislative coefficients from S3: %s", exc)
            return None

        try:
            sector_forecasts = self.s3_handler.read_json(self.source_keys["sector_forecasts"])
        except Exception as exc:
            self.logger.warning("Unable to load sector forecasts from S3: %s", exc)
            return None

        snapshot = self._compose_snapshot(
            scoring_results,
            law_coefficients,
            sector_forecasts,
            scoring_output_path,
        )

        try:
            self.s3_handler.write_json(self.snapshot_key, snapshot)
            snapshot_path = self._to_s3_uri(self.snapshot_key)
            self.logger.info(
                "Consolidated legislative snapshot saved",
                extra={
                    "extra_data": {
                        "snapshot_path": snapshot_path,
                        "agent": "fundamental_consolidator",
                    }
                },
            )
            return {"data": snapshot, "s3_path": snapshot_path}
        except Exception as exc:
            self.logger.error("Failed to write consolidated snapshot: %s", exc)
            return None

    def _compose_snapshot(
        self,
        scoring_results: List[Dict],
        law_coefficients: Dict,
        sector_forecasts: Dict,
        scoring_output_path: Optional[str],
    ) -> Dict:
        sector_impacts = self._aggregate_legislative_impacts(law_coefficients.get("laws_analyzed", []))
        forecast_map = {
            forecast["sector_id"]: forecast
            for forecast in sector_forecasts.get("sector_forecasts", [])
            if isinstance(forecast, dict) and forecast.get("sector_id")
        }
        companies_by_sector = self._group_companies(scoring_results)

        all_sector_ids = set(sector_impacts.keys()) | set(forecast_map.keys()) | set(companies_by_sector.keys())
        sectors = []
        for sector_id in sorted(all_sector_ids):
            legislative_info = sector_impacts.get(sector_id, {})
            forecast_info = forecast_map.get(sector_id, {})
            sector_name = legislative_info.get("sector_name") or forecast_info.get("sector_name")
            if not sector_name:
                sector_name = sector_id.replace("_", " ").title()

            sectors.append(
                {
                    "sector_id": sector_id,
                    "sector_name": sector_name,
                    "legislative": {
                        "average_impact_coefficient": legislative_info.get("average_coefficient"),
                        "max_impact_coefficient": legislative_info.get("max_coefficient"),
                        "min_impact_coefficient": legislative_info.get("min_coefficient"),
                        "laws_impacted": legislative_info.get("laws", []),
                    },
                    "forecast": {
                        "total_percent_change": forecast_info.get("total_percent_change"),
                        "absorption_timeline_years": forecast_info.get("absorption_timeline_years"),
                        "baseline_market_cap": forecast_info.get("baseline_market_cap"),
                    },
                    "top_companies": companies_by_sector.get(sector_id, [])[:5],
                }
            )

        metrics = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "total_laws": len(law_coefficients.get("laws_analyzed", [])),
            "sectors_with_impacts": len(sector_impacts),
            "sectors_with_forecasts": len(forecast_map),
            "companies_scored": len(scoring_results),
        }

        if sector_impacts:
            top_sector_id = max(
                sector_impacts.items(),
                key=lambda item: item[1].get("average_coefficient", 0),
            )[0]
            metrics["top_legislative_sector"] = top_sector_id

        laws_summary = []
        for law in law_coefficients.get("laws_analyzed", []):
            impacts = sorted(
                law.get("sector_impacts", []),
                key=lambda item: item.get("impact_coefficient", 0),
                reverse=True,
            )
            laws_summary.append(
                {
                    "law_id": law.get("law_id"),
                    "law_title": law.get("law_title"),
                    "implementation_date": law.get("implementation_date"),
                    "top_impacted_sectors": [
                        {
                            "sector_id": impact.get("sector_id"),
                            "sector_name": impact.get("sector_name"),
                            "impact_coefficient": impact.get("impact_coefficient"),
                        }
                        for impact in impacts[:3]
                    ],
                }
            )

        return {
            "generated_at": metrics["generated_at"],
            "source_objects": {
                "law_coefficients": self._to_s3_uri(self.source_keys["law_coefficients"]),
                "sector_forecasts": self._to_s3_uri(self.source_keys["sector_forecasts"]),
                "scoring_results": scoring_output_path or "",
            },
            "metrics": metrics,
            "sectors": sectors,
            "laws": laws_summary,
        }

    def _aggregate_legislative_impacts(self, laws: List[Dict]) -> Dict[str, Dict]:
        impacts: Dict[str, Dict] = {}

        for law in laws:
            for impact in law.get("sector_impacts", []):
                sector_id = impact.get("sector_id")
                if not sector_id:
                    continue

                entry = impacts.setdefault(
                    sector_id,
                    {
                        "sector_name": impact.get("sector_name"),
                        "coefficients": [],
                        "laws": [],
                    },
                )

                coefficient = impact.get("impact_coefficient")
                if coefficient is None:
                    continue

                entry["coefficients"].append(coefficient)
                entry["laws"].append(
                    {
                        "law_id": law.get("law_id"),
                        "law_title": law.get("law_title"),
                        "impact_coefficient": coefficient,
                        "impact_type": impact.get("impact_type"),
                        "rationale": impact.get("rationale"),
                    }
                )

        for info in impacts.values():
            coeffs = info["coefficients"]
            if not coeffs:
                continue

            info["average_coefficient"] = round(sum(coeffs) / len(coeffs), 2)
            info["max_coefficient"] = max(coeffs)
            info["min_coefficient"] = min(coeffs)
            info["laws"].sort(key=lambda item: item["impact_coefficient"], reverse=True)

        return impacts

    def _group_companies(self, scoring_results: List[Dict]) -> Dict[str, List[Dict]]:
        companies_by_sector: Dict[str, List[Dict]] = defaultdict(list)

        for company in scoring_results:
            sector_id = self._normalize_sector_id(company.get("sector"))
            if not sector_id:
                continue

            companies_by_sector[sector_id].append(
                {
                    "symbol": company.get("symbol"),
                    "company": company.get("company"),
                    "final_score": company.get("final_score"),
                    "scores_detail": company.get("scores_detail", {}),
                    "financial_metrics": company.get("financial_metrics", {}),
                    "percentile_scores": company.get("percentile_scores", {}),
                    "rank": None,
                }
            )

        for sector_id, entries in companies_by_sector.items():
            entries.sort(key=lambda item: item.get("final_score", 0), reverse=True)
            for idx, entry in enumerate(entries, start=1):
                entry["rank"] = idx

        return companies_by_sector

    def _normalize_sector_id(self, sector_label: Optional[str]) -> Optional[str]:
        if not sector_label:
            return None

        normalized = sector_label.strip().lower().replace(" & ", "_").replace(" ", "_")
        mapping = {
            "information_technology": "technology",
            "technology": "technology",
            "health_care": "healthcare",
            "healthcare": "healthcare",
            "financials": "financial",
            "financial": "financial",
            "energy": "energy",
            "industrials": "industrials",
            "materials": "materials",
            "communication_services": "telecommunications",
            "telecommunications": "telecommunications",
            "consumer_discretionary": "consumer_discretionary",
            "consumer_staples": "consumer_staples",
            "utilities": "utilities",
            "real_estate": "real_estate",
        }
        return mapping.get(normalized, normalized or None)

    def _to_s3_uri(self, key: str) -> str:
        base_prefix = self.config["s3_base_path"].rstrip("/")
        return f"s3://{self.config['s3_bucket']}/{base_prefix}/{key}"

class CSVSP500Scorer:
    def __init__(self):
        self.s3 = boto3.client('s3')
        # Nouvelles pondérations sans momentum
        self.weights = {
            'profitability': 0.3,
            'growth': 0.3,
            'financial_strength': 0.4
        }
        try:
            self.consolidator = LegislativeFundamentalConsolidator(logger)
        except Exception as exc:
            logger.warning("Legislative consolidator unavailable: %s", exc)
            self.consolidator = None
    
    def load_csv_data(self, s3_path: str) -> pd.DataFrame:
        """Charge les données depuis S3"""
        try:
            if s3_path.startswith('s3://'):
                s3_path = s3_path[5:]
            bucket, key = s3_path.split('/', 1)
            response = self.s3.get_object(Bucket=bucket, Key=key)
            df = pd.read_csv(response['Body'])
            logger.info(f"Données chargées depuis S3: {len(df)} entreprises")
            return df
        except Exception as e:
            logger.error(f"Erreur chargement S3: {e}")
            return pd.DataFrame()

    def calculate_financial_ratios(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calcule tous les ratios financiers"""
        logger.info("Calcul des ratios financiers...")
        
        numeric_cols = ['Revenue', 'Net_Income', 'Total_Assets', 'Shareholders_Equity', 
                       'Total_Debt', 'Market_Cap', 'Current_Assets', 'Current_Liabilities',
                       'Book_Value', 'Shares_Outstanding', 'Revenue_Previous_Year']
        
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                df[col] = df[col].clip(lower=0)
        
        df['roe'] = np.where(df['Shareholders_Equity'] > 0, 
                            (df['Net_Income'] / df['Shareholders_Equity']) * 100, 0)
        df['net_margin'] = np.where(df['Revenue'] > 0,
                                   (df['Net_Income'] / df['Revenue']) * 100, 0)
        df['roa'] = np.where(df['Total_Assets'] > 0,
                            (df['Net_Income'] / df['Total_Assets']) * 100, 0)
        df['revenue_growth'] = np.where(df['Revenue_Previous_Year'] > 0,
                                       ((df['Revenue'] - df['Revenue_Previous_Year']) / df['Revenue_Previous_Year']) * 100, 0)
        df['debt_to_equity'] = np.where(df['Shareholders_Equity'] > 0,
                                       df['Total_Debt'] / df['Shareholders_Equity'], 0)
        df['current_ratio'] = np.where(df['Current_Liabilities'] > 0,
                                      df['Current_Assets'] / df['Current_Liabilities'], 1)
        
        df['debt_to_equity'] = df['debt_to_equity'].clip(upper=5)
        df['roe'] = df['roe'].clip(-50, 100)
        df['net_margin'] = df['net_margin'].clip(-50, 100)
        df['revenue_growth'] = df['revenue_growth'].clip(-50, 200)
        
        for col in ['roe', 'net_margin', 'roa', 'revenue_growth', 'debt_to_equity', 'current_ratio']:
            df[col] = df[col].round(4)
        
        logger.info("Ratios calculés")
        return df

    def calculate_percentile_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calcule les scores percentiles par secteur"""
        logger.info("Calcul des scores percentiles...")
        
        def percentile_to_score(percentile, higher_better=True):
            if not higher_better:
                percentile = 1 - percentile
            return max(1, min(10, int(percentile * 9) + 1))
        
        for metric in ['roe', 'net_margin', 'revenue_growth', 'roa', 'current_ratio']:
            df[f'{metric}_percentile'] = df.groupby('Sector')[metric].rank(pct=True).round(4)
            df[f'{metric}_score'] = df[f'{metric}_percentile'].apply(percentile_to_score)
        
        for metric in ['debt_to_equity']:
            df[f'{metric}_percentile'] = df.groupby('Sector')[metric].rank(pct=True).round(4)
            df[f'{metric}_score'] = df[f'{metric}_percentile'].apply(lambda x: percentile_to_score(x, False))
        
        return df

    def calculate_final_scores(self, df: pd.DataFrame) -> list:
        """Calcule les scores finaux /100"""
        logger.info("Calcul des scores finaux...")
        results = []
        
        for _, row in df.iterrows():
            profitability = np.mean([
                row.get('roe_score', 5),
                row.get('net_margin_score', 5),
                row.get('roa_score', 5)
            ])
            growth = row.get('revenue_growth_score', 5)
            financial_strength = np.mean([
                row.get('debt_to_equity_score', 5),
                row.get('current_ratio_score', 5)
            ])
            
            weighted_score = (
                profitability * self.weights['profitability'] +
                growth * self.weights['growth'] +
                financial_strength * self.weights['financial_strength']
            )
            
            final_score = min(100, max(0, weighted_score * 10))
            
            result = {
                'symbol': row['Symbol'],
                'company': row['Company_Name'],
                'sector': row['Sector'],
                'final_score': round(final_score, 4),
                'scores_detail': {
                    'profitability_score': round(profitability, 4),
                    'growth_score': round(growth, 4),
                    'financial_strength_score': round(financial_strength, 4)
                },
                'financial_metrics': {
                    'roe': round(row.get('roe', 0), 4),
                    'net_margin': round(row.get('net_margin', 0), 4),
                    'roa': round(row.get('roa', 0), 4),
                    'revenue_growth': round(row.get('revenue_growth', 0), 4),
                    'debt_to_equity': round(row.get('debt_to_equity', 0), 4),
                    'current_ratio': round(row.get('current_ratio', 0), 4)
                },
                'percentile_scores': {
                    'roe_percentile': round(row.get('roe_percentile', 0), 4),
                    'net_margin_percentile': round(row.get('net_margin_percentile', 0), 4),
                    'roa_percentile': round(row.get('roa_percentile', 0), 4),
                    'revenue_growth_percentile': round(row.get('revenue_growth_percentile', 0), 4),
                    'debt_to_equity_percentile': round(row.get('debt_to_equity_percentile', 0), 4),
                    'current_ratio_percentile': round(row.get('current_ratio_percentile', 0), 4)
                },
                'calculation_detail': {
                    'profitability_calculation': f"({row.get('roe_score', 5)} + {row.get('net_margin_score', 5)} + {row.get('roa_score', 5)}) / 3 = {round(profitability, 4)}",
                    'weighted_score_calculation': f"{round(profitability, 4)} * 0.3 + {round(growth, 4)} * 0.3 + {round(financial_strength, 4)} * 0.4 = {round(weighted_score, 4)}",
                    'final_score_calculation': f"{round(weighted_score, 4)} * 10 = {round(final_score, 4)}"
                }
            }
            
            results.append(result)
        
        return results

    def save_results_to_s3(self, results: list, s3_output_path: str) -> str:
        """Sauvegarde les résultats détaillés sur S3"""
        try:
            output_data = {
                'metadata': {
                    'timestamp': datetime.now().isoformat(),
                    'total_companies': len(results),
                    'methodology': 'Score /100 basé sur vraies données CSV S3 (sans momentum)',
                    'weights': self.weights,
                    'categories': {
                        'profitability': '30% - ROE, Marge nette, ROA',
                        'growth': '30% - Croissance du chiffre d’affaires',
                        'financial_strength': '40% - Debt/Equity, Current Ratio'
                    }
                },
                'summary': {
                    'average_score': round(np.mean([r['final_score'] for r in results]), 4),
                    'median_score': round(np.median([r['final_score'] for r in results]), 4),
                    'max_score': round(max([r['final_score'] for r in results]), 4),
                    'min_score': round(min([r['final_score'] for r in results]), 4),
                    'top_10': results[:10]
                },
                'detailed_scores': results
            }
            
            if s3_output_path.startswith('s3://'):
                s3_output_path = s3_output_path[5:]
            bucket, base_key = s3_output_path.split('/', 1)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            key = f"{base_key}/sp500_detailed_scores_{timestamp}.json"
            
            self.s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(output_data, indent=2, ensure_ascii=False),
                ContentType='application/json'
            )
            
            s3_url = f"s3://{bucket}/{key}"
            print(f"\n✅ Fichier JSON sauvegardé sur S3 : {s3_url}\n")
            return s3_url
            
        except Exception as e:
            logger.error(f"Erreur sauvegarde S3: {e}")
            return ""

    def run_complete_scoring(self, s3_input_path: str, s3_output_path: str) -> dict:
        logger.info("Starting S&P 500 scoring pipeline from S3")
        try:
            df = self.load_csv_data(s3_input_path)
            if df.empty:
                return {'error': 'Impossible de charger les donnees'}
            
            df = self.calculate_financial_ratios(df)
            df = self.calculate_percentile_scores(df)
            results = self.calculate_final_scores(df)
            results.sort(key=lambda x: x['final_score'], reverse=True)
            
            print(f"\n{'='*80}")
            print("S&P 500 SCORING - FINAL RESULTS")
            print(f"{'='*80}")
            print(f"Companies analysed: {len(results)}")
            print("Weights: Profitability 30%, Growth 30%, Financial Strength 40%")
            
            print("\nTOP 20 S&P 500:")
            for i, company in enumerate(results[:20], 1):
                print(f"  {i:2d}. {company['symbol']:5s} | {company['final_score']:8.4f}/100 | {company['company'][:40]}")
            
            output_file = self.save_results_to_s3(results, s3_output_path)

            snapshot_info = None
            if self.consolidator:
                snapshot_info = self.consolidator.build_snapshot(results, output_file)
                if snapshot_info and snapshot_info.get('s3_path'):
                    print(f"\nConsolidated legislative snapshot: {snapshot_info['s3_path']}")

            return {
                'timestamp': datetime.now().isoformat(),
                'total_companies': len(results),
                'weights': self.weights,
                'companies': results,
                'output_file': output_file,
                'legislative_snapshot': snapshot_info.get('s3_path') if snapshot_info else ''
            }
            
        except Exception as e:
            logger.error(f"Erreur lors du scoring: {e}")
            return {'error': str(e)}

def main():
    scorer = CSVSP500Scorer()
    s3_input = "s3://csv-file-store-740fdb60/dzd-43x1yet80db8eo/42xqkj75xfl09c/shared/database/sp500_yfinance.csv"
    s3_output = "s3://csv-file-store-740fdb60/dzd-43x1yet80db8eo/42xqkj75xfl09c/shared/database"
    
    print(f"Input CSV: {s3_input}")
    print(f"Output: {s3_output}")
    
    result = scorer.run_complete_scoring(s3_input, s3_output)
    
    if 'error' in result:
        print(f"Error: {result['error']}")
    else:
        print("\nSCORING TERMINE AVEC SUCCES!")
        print("Ponderations: Profitability 30%, Growth 30%, Financial Strength 40%")
        print(f"{result['total_companies']} entreprises scorees avec les vraies donnees!")
        if result.get('output_file'):
            print(f"Detailed results saved: {result['output_file']}")
        if result.get('legislative_snapshot'):
            print(f"Consolidated snapshot: {result['legislative_snapshot']}")

if __name__ == "__main__":
    main()
