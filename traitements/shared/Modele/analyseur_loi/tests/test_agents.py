import unittest
import json
import sys
import os
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

# Ajout du chemin parent pour les imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents.agent_01_portfolio_extractor import PortfolioExtractor
from agents.agent_02_sector_classifier import SectorClassifier

class TestPortfolioExtractor(unittest.TestCase):
    
    def setUp(self):
        self.test_config = {
            "s3_bucket": "test-bucket",
            "s3_base_path": "test-path/",
            "input_sources": {"portfolio_file": "test.csv"},
            "output_destinations": {"intermediate_json": "outputs/"}
        }
    
    @patch('agents.agent_01_portfolio_extractor.S3Handler')
    @patch('agents.agent_01_portfolio_extractor.JSONValidator')
    def test_portfolio_extraction(self, mock_validator, mock_s3_handler):
        """Test d'extraction de portefeuille"""
        # Configuration des mocks
        mock_s3_instance = Mock()
        mock_s3_handler.return_value = mock_s3_instance
        
        # Données CSV simulées
        test_df = pd.DataFrame({
            'Symbol': ['AAPL', 'MSFT'],
            'Security': ['Apple Inc.', 'Microsoft Corp.'],
            'Weight': [5.0, 4.5],
            'Market Cap': [3000000000000, 2800000000000]
        })
        mock_s3_instance.read_csv.return_value = test_df
        mock_validator.validate_portfolio_data.return_value = True
        
        extractor = PortfolioExtractor(self.test_config)
        result = extractor.extract_portfolio()
        
        self.assertEqual(result['total_companies'], 2)
        self.assertEqual(len(result['companies']), 2)
        self.assertEqual(result['companies'][0]['ticker'], 'AAPL')
        mock_s3_instance.write_json.assert_called_once()

class TestSectorClassifier(unittest.TestCase):
    
    def setUp(self):
        self.test_config = {
            "s3_bucket": "test-bucket",
            "s3_base_path": "test-path/",
            "input_sources": {"10k_reports": "test_10k.json"},
            "output_destinations": {"intermediate_json": "outputs/"}
        }
    
    @patch('agents.agent_02_sector_classifier.S3Handler')
    @patch('agents.agent_02_sector_classifier.JSONValidator')
    def test_sector_classification(self, mock_validator, mock_s3_handler):
        """Test de classification sectorielle"""
        # Configuration des mocks
        mock_s3_instance = Mock()
        mock_s3_handler.return_value = mock_s3_instance
        
        # Données 10K simulées
        test_10k_data = {
            "companies": [
                {
                    "ticker": "AAPL",
                    "company_name": "Apple Inc.",
                    "market_cap": 3000000000000,
                    "business_description": "technology software digital innovation",
                    "risk_factors": "technology disruption competition"
                },
                {
                    "ticker": "JPM",
                    "company_name": "JPMorgan Chase",
                    "market_cap": 500000000000,
                    "business_description": "banking financial services investment",
                    "risk_factors": "financial regulation credit risk"
                }
            ]
        }
        mock_s3_instance.read_json.return_value = test_10k_data
        mock_validator.validate_sector_taxonomy.return_value = True
        
        classifier = SectorClassifier(self.test_config)
        result = classifier.classify_sectors()
        
        self.assertGreater(result['total_sectors'], 0)
        self.assertIn('sectors', result)
        mock_s3_instance.write_json.assert_called_once()
    
    def test_sector_assignment(self):
        """Test d'assignation sectorielle"""
        classifier = SectorClassifier(self.test_config)
        
        ticker_sectors = {
            "technology": ["AAPL", "MSFT"],
            "financial": ["JPM", "GS"]
        }
        
        # Test assignation technologie
        sector = classifier._assign_sector_by_ticker("AAPL", ticker_sectors)
        self.assertEqual(sector, "technology")
        
        # Test assignation financier
        sector = classifier._assign_sector_by_ticker("JPM", ticker_sectors)
        self.assertEqual(sector, "financial")
        
        # Test ticker inconnu
        sector = classifier._assign_sector_by_ticker("UNKNOWN", ticker_sectors)
        self.assertEqual(sector, "other")

if __name__ == '__main__':
    unittest.main()