import json
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# Ajout du chemin pour les imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from agents.agent_01_portfolio_extractor import PortfolioExtractor
from agents.agent_02_sector_classifier import SectorClassifier
from agents.agent_03_legislative_impact_analyzer import LegislativeImpactAnalyzer
from agents.agent_04_monetary_flow_modeler import MonetaryFlowModeler
from agents.agent_05_sector_impact_quantifier import SectorImpactQuantifier
from agents.agent_06_portfolio_reallocator import PortfolioReallocator
from utils.logger import setup_logger
from utils.s3_handler import S3Handler

# Chargement des variables d'environnement
load_dotenv()

logger = setup_logger(__name__)

class LegislativeImpactOrchestrator:
    def __init__(self, config_path="config.json"):
        self.config = self._load_config(config_path)
        self.s3_handler = S3Handler(self.config['s3_bucket'], self.config['s3_base_path'])
        self.agents = self._initialize_agents()
        self.execution_log = []
        
        logger.info("Legislative Impact Orchestrator initialized", 
                   extra={'extra_data': {'agent': 'orchestrator', 'config_loaded': True}})
    
    def _load_config(self, config_path):
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {str(e)}")
            raise
    
    def _initialize_agents(self):
        return {
            1: PortfolioExtractor(self.config),
            2: SectorClassifier(self.config),
            3: LegislativeImpactAnalyzer(self.config),
            4: MonetaryFlowModeler(self.config),
            5: SectorImpactQuantifier(self.config),
            6: PortfolioReallocator(self.config)
        }
    
    def execute_full_pipeline(self):
        """Exécute tous les agents dans l'ordre séquentiel"""
        logger.info("Starting full pipeline execution", extra={'extra_data': {'agent': 'orchestrator'}})
        
        try:
            # Création du dossier intermediate_outputs s'il n'existe pas
            self._ensure_intermediate_outputs_folder()
            
            results = {}
            
            # Exécution séquentielle des agents
            for agent_id in range(1, 7):
                logger.info(f"Executing Agent {agent_id}", 
                           extra={'extra_data': {'agent': 'orchestrator', 'current_agent': agent_id}})
                
                result = self._execute_single_agent(agent_id)
                results[f"agent_{agent_id}"] = result
                
                self._log_execution_step(agent_id, "SUCCESS", result)
                logger.info(f"Agent {agent_id} completed successfully")
            
            # Génération du rapport final
            final_report = self._generate_final_report(results)
            
            logger.info("Full pipeline execution completed successfully", 
                       extra={'extra_data': {'agent': 'orchestrator', 'total_agents': 6}})
            
            return final_report
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}", 
                        extra={'extra_data': {'agent': 'orchestrator', 'error': str(e)}})
            raise
    
    def execute_single_agent(self, agent_id):
        """Exécute un agent spécifique"""
        logger.info(f"Executing single agent: {agent_id}", 
                   extra={'extra_data': {'agent': 'orchestrator', 'target_agent': agent_id}})
        
        try:
            result = self._execute_single_agent(agent_id)
            self._log_execution_step(agent_id, "SUCCESS", result)
            return result
        except Exception as e:
            self._log_execution_step(agent_id, "FAILED", {"error": str(e)})
            raise
    
    def _execute_single_agent(self, agent_id):
        """Exécution interne d'un agent"""
        if agent_id not in self.agents:
            raise ValueError(f"Agent {agent_id} not found")
        
        agent = self.agents[agent_id]
        
        # Exécution selon l'agent
        if agent_id == 1:
            return agent.extract_portfolio()
        elif agent_id == 2:
            return agent.classify_sectors()
        elif agent_id == 3:
            return agent.analyze_legislative_impact()
        elif agent_id == 4:
            return agent.model_monetary_flows()
        elif agent_id == 5:
            return agent.quantify_sector_impacts()
        elif agent_id == 6:
            return agent.reallocate_portfolio()
        else:
            raise ValueError(f"Unknown agent ID: {agent_id}")
    
    def _ensure_intermediate_outputs_folder(self):
        """Assure que le dossier intermediate_outputs existe dans S3"""
        try:
            # Création d'un fichier marker pour créer le dossier
            marker_path = f"{self.config['output_destinations']['intermediate_json']}.marker"
            self.s3_handler.write_json(marker_path, {"created": datetime.now().isoformat()})
            logger.info("Intermediate outputs folder ensured")
        except Exception as e:
            logger.warning(f"Could not ensure intermediate outputs folder: {str(e)}")
    
    def _log_execution_step(self, agent_id, status, result):
        """Log une étape d'exécution"""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "agent_id": agent_id,
            "status": status,
            "result_summary": self._summarize_result(result) if status == "SUCCESS" else result
        }
        self.execution_log.append(log_entry)
    
    def _summarize_result(self, result):
        """Crée un résumé du résultat pour le log"""
        if isinstance(result, dict):
            summary = {"type": "dict", "keys": list(result.keys())}
            if "companies" in result:
                summary["companies_count"] = len(result["companies"])
            if "sectors" in result:
                summary["sectors_count"] = len(result["sectors"])
            return summary
        return {"type": type(result).__name__}
    
    def _generate_final_report(self, results):
        """Génère un rapport final d'exécution"""
        report = {
            "execution_date": datetime.now().isoformat(),
            "pipeline_status": "COMPLETED",
            "agents_executed": len(results),
            "execution_log": self.execution_log,
            "final_outputs": {
                "portfolio_updated": "2025-08-15_composition_sp500_updated.csv",
                "intermediate_files": [
                    "01_portfolio_data.json",
                    "02_sector_taxonomy.json",
                    "03_sector_impact_coefficients.json",
                    "04_monetary_flow_model.json",
                    "05_sector_quantified_impacts.json",
                    "06_portfolio_reallocated.json"
                ]
            },
            "summary": self._generate_execution_summary(results)
        }
        
        # Sauvegarde du rapport
        report_path = f"{self.config['output_destinations']['intermediate_json']}execution_report.json"
        self.s3_handler.write_json(report_path, report)
        
        return report
    
    def _generate_execution_summary(self, results):
        """Génère un résumé de l'exécution"""
        summary = {
            "total_companies_analyzed": 0,
            "total_sectors_identified": 0,
            "laws_analyzed": 0,
            "portfolio_impact_percent": 0
        }
        
        try:
            if "agent_1" in results and "companies" in results["agent_1"]:
                summary["total_companies_analyzed"] = len(results["agent_1"]["companies"])
            
            if "agent_2" in results and "sectors" in results["agent_2"]:
                summary["total_sectors_identified"] = len(results["agent_2"]["sectors"])
            
            if "agent_3" in results and "laws_analyzed" in results["agent_3"]:
                summary["laws_analyzed"] = len(results["agent_3"]["laws_analyzed"])
            
            if "agent_6" in results and "total_impact_percent" in results["agent_6"]:
                summary["portfolio_impact_percent"] = results["agent_6"]["total_impact_percent"]
        
        except Exception as e:
            logger.warning(f"Could not generate complete summary: {str(e)}")
        
        return summary
    
    def get_execution_status(self):
        """Retourne le statut d'exécution actuel"""
        return {
            "execution_log": self.execution_log,
            "last_execution": self.execution_log[-1] if self.execution_log else None,
            "total_steps": len(self.execution_log)
        }

def main():
    """Point d'entrée principal"""
    try:
        orchestrator = LegislativeImpactOrchestrator()
        
        # Vérification des arguments de ligne de commande
        if len(sys.argv) > 1:
            if sys.argv[1] == "single" and len(sys.argv) > 2:
                agent_id = int(sys.argv[2])
                result = orchestrator.execute_single_agent(agent_id)
                print(f"Agent {agent_id} executed successfully")
            elif sys.argv[1] == "status":
                status = orchestrator.get_execution_status()
                print(json.dumps(status, indent=2))
            else:
                print("Usage: python orchestrator.py [single <agent_id>|status]")
        else:
            # Exécution complète
            report = orchestrator.execute_full_pipeline()
            print("Pipeline executed successfully!")
            print(f"Final report saved. Portfolio impact: {report['summary']['portfolio_impact_percent']:.2f}%")
    
    except Exception as e:
        logger.error(f"Orchestrator execution failed: {str(e)}")
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()