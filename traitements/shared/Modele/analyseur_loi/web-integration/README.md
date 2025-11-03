# 🏛️ Legislative Impact Analyzer - Intégration Web

## Architecture
```
Site Web (HTML/CSS/JS)
    ↓ (requête HTTP)
Serveur Node.js (API)
    ↓ (exécution Python)
Orchestrateur Legislative Impact
    ↓↑ (lecture/écriture)
AWS S3
```

## Installation

```bash
cd web-integration
npm install
```

## Configuration

Vérifiez le fichier `.env` :
```
AWS_REGION=us-east-1
PORT=3000
S3_BUCKET_NAME=csv-file-store-740fdb60
S3_BASE_PATH=dzd-43x1yet80db8eo/42xqkj75xfl09c/shared/database/
```

## Démarrage

```bash
npm start
```

Puis ouvrez http://localhost:3000

## Endpoints API

### POST /api/agent/invoke
Exécute un agent spécifique
```json
{
  "agentId": 1,
  "userInput": "Analyser le portfolio"
}
```

### POST /api/pipeline/execute
Exécute le pipeline complet
```json
{
  "userInput": "Analyse complète"
}
```

### GET /api/status
Obtient le statut d'exécution

### GET /api/history
Récupère l'historique des exécutions depuis S3

## Structure S3

```
csv-file-store-740fdb60/
└── dzd-43x1yet80db8eo/42xqkj75xfl09c/shared/database/
    ├── inputs/
    │   ├── timestamp_agent1.json
    │   └── timestamp_full_pipeline.json
    ├── outputs/
    │   ├── timestamp_agent1.json
    │   └── timestamp_full_pipeline.json
    └── intermediate_outputs/
        └── execution_report.json
```

## Agents Disponibles

1. **Portfolio Extractor** - Extraction des données de portfolio
2. **Sector Classifier** - Classification par secteurs
3. **Legislative Impact Analyzer** - Analyse d'impact législatif
4. **Monetary Flow Modeler** - Modélisation des flux monétaires
5. **Sector Impact Quantifier** - Quantification des impacts sectoriels
6. **Portfolio Reallocator** - Réallocation de portfolio

## Fonctionnalités

✅ Interface web intuitive
✅ Exécution d'agents individuels ou pipeline complet
✅ Sauvegarde automatique dans S3
✅ Historique des exécutions
✅ Statut en temps réel
✅ Gestion d'erreurs