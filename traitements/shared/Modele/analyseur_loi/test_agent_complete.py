#!/usr/bin/env python3
"""
Script de test complet pour le système Legislative Impact Analyzer
Teste tous les agents individuellement et vérifie leur fonctionnement
"""

import sys
import os
import json
from datetime import datetime

# Ajout du chemin pour les imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from orchestrator import LegislativeImpactOrchestrator
from utils.logger import setup_logger

logger = setup_logger(__name__)

def test_individual_agents():
    """Test de chaque agent individuellement"""
    print("🚀 Démarrage des tests individuels des agents...")
    
    try:
        orchestrator = LegislativeImpactOrchestrator()
        results = {}
        
        # Test de chaque agent
        for agent_id in range(1, 7):
            print(f"\n📊 Test de l'Agent {agent_id}...")
            
            try:
                result = orchestrator.execute_single_agent(agent_id)
                results[f"agent_{agent_id}"] = {
                    "status": "SUCCESS",
                    "result": result,
                    "timestamp": datetime.now().isoformat()
                }
                print(f"✅ Agent {agent_id} : SUCCÈS")
                
                # Affichage des informations clés
                if agent_id == 1 and "total_companies" in result:
                    print(f"   📈 Entreprises analysées: {result['total_companies']}")
                elif agent_id == 2 and "total_sectors" in result:
                    print(f"   🏢 Secteurs identifiés: {result['total_sectors']}")
                elif agent_id == 3 and "laws_analyzed" in result:
                    print(f"   📜 Lois analysées: {len(result.get('laws_analyzed', []))}")
                
            except Exception as e:
                results[f"agent_{agent_id}"] = {
                    "status": "FAILED",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }
                print(f"❌ Agent {agent_id} : ÉCHEC - {str(e)}")
        
        return results
        
    except Exception as e:
        print(f"❌ Erreur lors de l'initialisation: {str(e)}")
        return None

def test_orchestrator_status():
    """Test du statut de l'orchestrateur"""
    print("\n🔍 Test du statut de l'orchestrateur...")
    
    try:
        orchestrator = LegislativeImpactOrchestrator()
        status = orchestrator.get_execution_status()
        
        print(f"✅ Statut récupéré avec succès")
        print(f"   📊 Étapes d'exécution: {status['total_steps']}")
        
        return status
        
    except Exception as e:
        print(f"❌ Erreur lors de la récupération du statut: {str(e)}")
        return None

def generate_test_report(agent_results, status_result):
    """Génère un rapport de test"""
    report = {
        "test_date": datetime.now().isoformat(),
        "test_summary": {
            "total_agents_tested": 6,
            "successful_agents": 0,
            "failed_agents": 0
        },
        "agent_results": agent_results,
        "orchestrator_status": status_result
    }
    
    if agent_results:
        for agent_id, result in agent_results.items():
            if result["status"] == "SUCCESS":
                report["test_summary"]["successful_agents"] += 1
            else:
                report["test_summary"]["failed_agents"] += 1
    
    # Sauvegarde du rapport
    report_path = "test_report.json"
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"\n📄 Rapport de test sauvegardé: {report_path}")
    return report

def main():
    """Fonction principale de test"""
    print("=" * 60)
    print("🧪 TESTS DU SYSTÈME LEGISLATIVE IMPACT ANALYZER")
    print("=" * 60)
    
    # Test des agents individuels
    agent_results = test_individual_agents()
    
    # Test du statut de l'orchestrateur
    status_result = test_orchestrator_status()
    
    # Génération du rapport
    report = generate_test_report(agent_results, status_result)
    
    # Résumé final
    print("\n" + "=" * 60)
    print("📋 RÉSUMÉ DES TESTS")
    print("=" * 60)
    
    if agent_results:
        successful = report["test_summary"]["successful_agents"]
        failed = report["test_summary"]["failed_agents"]
        
        print(f"✅ Agents réussis: {successful}/6")
        print(f"❌ Agents échoués: {failed}/6")
        
        if failed == 0:
            print("\n🎉 TOUS LES TESTS SONT PASSÉS AVEC SUCCÈS!")
        else:
            print(f"\n⚠️  {failed} agent(s) ont échoué. Vérifiez les logs pour plus de détails.")
    else:
        print("❌ Impossible d'exécuter les tests")
    
    print("=" * 60)

if __name__ == "__main__":
    main()