import unittest
import json
import os
import sys
from unittest.mock import Mock, patch, MagicMock

# Ajout du chemin parent pour les imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from orchestrator import LegislativeImpactOrchestrator

class TestLegislativeImpactOrchestrator(unittest.TestCase):
    
    def setUp(self):
        """Configuration des tests"""
        self.test_config = {
            "s3_bucket": "test-bucket",
            "s3_base_path": "test-path/",
            "input_sources": {
                "10k_reports": "test_10k.json",
                "laws_folder": "test_laws/",
                "portfolio_file": "test_portfolio.csv"
            },
            "output_destinations": {
                "intermediate_json": "test_outputs/",
                "final_csv": ""
            },
            "execution_parameters": {
                "min_impact_coefficient": 3,
                "confidence_threshold": 0.7,
                "max_projection_years": 5
            }
        }
    
    @patch('orchestrator.load_dotenv')
    @patch('builtins.open')
    def test_orchestrator_initialization(self, mock_open, mock_load_dotenv):
        """Test d'initialisation de l'orchestrateur"""
        mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(self.test_config)
        
        orchestrator = LegislativeImpactOrchestrator("test_config.json")
        
        self.assertEqual(orchestrator.config['s3_bucket'], "test-bucket")
        self.assertEqual(len(orchestrator.agents), 6)
        self.assertEqual(len(orchestrator.execution_log), 0)
    
    @patch('orchestrator.load_dotenv')
    @patch('builtins.open')
    def test_single_agent_execution(self, mock_open, mock_load_dotenv):
        """Test d'exécution d'un agent unique"""
        mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(self.test_config)
        
        orchestrator = LegislativeImpactOrchestrator("test_config.json")
        
        # Mock de l'agent 1
        mock_agent = Mock()
        mock_agent.extract_portfolio.return_value = {"test": "result"}
        orchestrator.agents[1] = mock_agent
        
        result = orchestrator.execute_single_agent(1)
        
        self.assertEqual(result, {"test": "result"})
        self.assertEqual(len(orchestrator.execution_log), 1)
        self.assertEqual(orchestrator.execution_log[0]['status'], 'SUCCESS')
    
    @patch('orchestrator.load_dotenv')
    @patch('builtins.open')
    def test_execution_log(self, mock_open, mock_load_dotenv):
        """Test du système de logging d'exécution"""
        mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(self.test_config)
        orchestrator = LegislativeImpactOrchestrator("test_config.json")
        
        orchestrator._log_execution_step(1, "SUCCESS", {"companies": [1, 2, 3]})
        
        self.assertEqual(len(orchestrator.execution_log), 1)
        log_entry = orchestrator.execution_log[0]
        self.assertEqual(log_entry['agent_id'], 1)
        self.assertEqual(log_entry['status'], 'SUCCESS')
    
    @patch('orchestrator.load_dotenv')
    @patch('builtins.open')
    def test_result_summarization(self, mock_open, mock_load_dotenv):
        """Test de la fonction de résumé des résultats"""
        mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(self.test_config)
        orchestrator = LegislativeImpactOrchestrator("test_config.json")
        
        result = {"companies": [1, 2, 3], "sectors": [1, 2]}
        summary = orchestrator._summarize_result(result)
        
        self.assertEqual(summary['type'], 'dict')
        self.assertEqual(summary['companies_count'], 3)
        self.assertIn('companies', summary['keys'])

if __name__ == '__main__':
    unittest.main()