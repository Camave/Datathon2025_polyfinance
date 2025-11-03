## Tableau de bord IA réglementaire

Application Streamlit entièrement en français permettant :

- la sélection d'un portefeuille (local ou S3) ;
- la sélection de lois à analyser depuis `s3://<bucket>/<base>/lois/` ;
- l'exécution d'un pipeline d'agents IA (LawAgent, MarketAgent, PortfolioAgent, SynthesisAgent) via `MarketTechOrchestrator` ;
- l'affichage de visualisations interactives (Plotly) et de recommandations d'ajustement ;
- l'export automatique des résultats sur S3 (`webapp_outputs/`) ainsi que le téléchargement local (CSV / JSON).

### Lancement

```bash
cd traitements/shared/Modele/analyseur_loi/web_streamlit
streamlit run app.py
```

Assurez-vous que les variables d'environnement AWS (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`) sont définies. Le fichier `config.json` à la racine du projet est utilisé pour récupérer le bucket et le préfixe S3.

### Exigences

- `streamlit`
- `plotly`
- `pandas`
- `numpy`
- `boto3`

Ces dépendances peuvent être installées en exécutant : 

```bash
pip install streamlit plotly pandas numpy boto3
```

### Structure de sortie

Les résultats sont enregistrés sous `webapp_outputs/<horodatage>_<label>/` en trois fichiers :

- `wallet_initial.csv`
- `wallet_projection.csv`
- `analyse_complete.json`

Ces artefacts peuvent ensuite être consommés par d'autres systèmes (reporting PDF, automatisation de rééquilibrage, etc.).
