# Legislative Impact Analyzer

Système d'agents IA orchestrés pour analyser automatiquement l'impact de nouveaux textes de loi sur un portefeuille d'investissement.

## Architecture

Le système est composé de 6 agents séquentiels coordonnés par un orchestrateur principal :

1. **Portfolio Extractor** - Extraction des données de portefeuille depuis S3
2. **Sector Classifier** - Classification sectorielle automatique basée sur les 10K reports
3. **Legislative Impact Analyzer** - Analyse d'impact des textes de loi par secteur
4. **Monetary Flow Modeler** - Modélisation des flux monétaires intersectoriels
5. **Sector Impact Quantifier** - Quantification précise des impacts sectoriels
6. **Portfolio Reallocator** - Réallocation du portefeuille avec nouvelles projections

## Installation

1. Cloner le projet :
```bash
cd legislative-impact-analyzer
```

2. Installer les dépendances :
```bash
pip install -r requirements.txt
```

3. Configurer les credentials AWS dans `.env` :
```bash
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_DEFAULT_REGION=us-east-1
```

## Configuration

Modifier `config.json` selon vos besoins :
- Bucket S3 et chemins des données
- Paramètres d'exécution (seuils, timeline, etc.)
- Sources de données externes

## Utilisation

### Exécution complète du pipeline
```bash
python orchestrator.py
```

### Exécution d'un agent spécifique
```bash
python orchestrator.py single 1  # Exécute seulement l'agent 1
```

### Vérification du statut
```bash
python orchestrator.py status
```

### Consolidation législation / fondamentaux
- Le scorer `Analyse/fondamental.py` génère un snapshot consolidé `intermediate_outputs/legislative_fundamental_snapshot.json` en combinant les sorties des agents 03 et 05 avec le scoring fondamental.
- Le serveur web (`web-integration/server.js`) consomme ce snapshot unique pour alimenter les dashboards (secteurs impactés, lois majeures, top entreprises).
- La mise à jour est déclenchée automatiquement à la fin de `run_complete_scoring`, garantissant une cohérence front/back.

### Nouveau dashboard Streamlit
- Le répertoire `web_streamlit/` contient l'application Streamlit en français répondant au cahier des charges “Analyse des impacts réglementaires”.
- L'application s'appuie sur le nouvel orchestrateur `MarketTechOrchestrator` qui pilote quatre agents spécialisés (`LawAgent`, `MarketAgent`, `PortfolioAgent`, `SynthesisAgent`).
- Les utilisateurs peuvent sélectionner un portefeuille (S3 ou local), choisir des lois à analyser et obtenir des recommandations (Acheter / Vendre / Conserver) ainsi que des exports automatiques sur S3.

## Structure des données

### Inputs (S3)
- `10k_clean.json` - Données historiques d'entreprises
- `lois/` - Dossier contenant les textes de loi (HTML, TXT)
- `2025-08-15_composition_sp500.csv` - Composition du portefeuille

### Outputs (S3)
- `intermediate_outputs/` - Fichiers JSON intermédiaires de chaque agent
  - `legislative_fundamental_snapshot.json` - Vue consolidée législation + fondamentaux pour le front web
- `2025-08-15_composition_sp500_updated.csv` - Portefeuille actualisé
- `execution_report.json` - Rapport d'exécution complet

## Agents détaillés

### Agent 1 : Portfolio Extractor
- Lit le CSV du portefeuille depuis S3
- Extrait ticker, nom, pondération, market cap
- Output : `01_portfolio_data.json`

### Agent 2 : Sector Classifier
- Analyse les 10K reports pour classification sectorielle
- Crée une taxonomie dynamique basée sur les chaînes de valeur
- Output : `02_sector_taxonomy.json`

### Agent 3 : Legislative Impact Analyzer
- Analyse les textes de loi et leur impact par secteur
- Consulte sources externes (Bloomberg, Yahoo Finance)
- Attribue coefficients d'impact (1-10)
- Output : `03_sector_impact_coefficients.json`

### Agent 4 : Monetary Flow Modeler
- Modélise les flux monétaires intersectoriels
- Intègre données FED et sensibilité aux taux
- Principe de conservation de la masse monétaire
- Output : `04_monetary_flow_model.json`

### Agent 5 : Sector Impact Quantifier
- Quantifie précisément les fluctuations sectorielles
- Génère projections annuelles avec courbes d'adoption
- Valide avec sources consulting (McKinsey, BCG, Bain)
- Output : `05_sector_quantified_impacts.json`

### Agent 6 : Portfolio Reallocator
- Applique impacts sectoriels au portefeuille
- Recalcule market caps et pondérations
- Génère CSV final actualisé
- Output : `06_portfolio_reallocated.json` + CSV final

## Logging et monitoring

- Logs structurés en format JSON
- Niveaux : DEBUG, INFO, WARNING, ERROR, CRITICAL
- Retry logic avec exponential backoff pour S3
- Validation JSON stricte à chaque étape

## Gestion d'erreurs

- Try-catch sur toutes opérations S3 et API externes
- Fallback strategies si sources indisponibles
- Validation des données à chaque étape
- Logs détaillés pour debugging

## Tests

```bash
# Tests unitaires (à implémenter)
python -m pytest tests/

# Test d'intégration end-to-end
python orchestrator.py
```

## Limitations et assumptions

- Classification sectorielle basée sur mots-clés (peut être améliorée avec NLP avancé)
- Modèle de flux monétaires simplifié
- Sources externes simulées (à connecter aux vraies APIs)
- Timeline d'absorption basée sur heuristiques

## Développement futur

- Interface utilisateur (dashboard)
- Support multi-portefeuilles
- Cache pour données scraped
- Projections trimestrielles
- Analyses de sensibilité avancées
