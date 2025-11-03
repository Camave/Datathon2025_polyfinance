#!/usr/bin/env python3
"""
Application Streamlit principale pour l'analyse réglementaire et financière.
Tout le contenu est présenté en français conformément au cahier des charges.
"""

from __future__ import annotations

import json
import textwrap
from pathlib import Path
from typing import Dict, List

import pandas as pd
import plotly.express as px
import streamlit as st

from markettech_orchestrator import MarketTechOrchestrator


# --------------------------- Configuration générale ---------------------------

st.set_page_config(
    page_title="Analyse réglementaire IA",
    page_icon="📊",
    layout="wide",
)

st.title("Analyse des impacts réglementaires sur votre portefeuille")
st.write(
    "Ce tableau de bord orchestre des agents IA afin d'évaluer l'impact de nouvelles lois sur votre portefeuille "
    "d'investissement. Toutes les informations sont traitées et affichées en français."
)

STATE_KEYS = {
    "wallet_df": "wallet_df",
    "analysis_results": "analysis_results",
    "selected_laws": "selected_laws",
    "selected_wallet": "selected_wallet",
    "user_label": "user_label",
}

for key in STATE_KEYS.values():
    if key not in st.session_state:
        st.session_state[key] = None


# --------------------------- Initialisation orchestrateur ---------------------

@st.cache_resource
def get_orchestrator() -> MarketTechOrchestrator:
    return MarketTechOrchestrator()


orchestrator = get_orchestrator()


# --------------------------- Panneau latéral ----------------------------------

st.sidebar.header("Paramètres d'analyse")
user_label = st.sidebar.text_input(
    "Nommer cette analyse",
    value=st.session_state.get(STATE_KEYS["user_label"]) or "analyse_user",
    help="Identifiant utilisé pour sauvegarder les résultats sur S3.",
)

available_laws = orchestrator.list_available_laws()
law_options = {
    f"{Path(item['relative_key']).name} ({item['size']/1024:.0f} Ko)": item["relative_key"] for item in available_laws
}

selected_laws_labels = st.sidebar.multiselect(
    "Sélection des lois (S3)",
    options=list(law_options.keys()),
    default=list(law_options.keys())[:2],
)
selected_law_keys = [law_options[label] for label in selected_laws_labels]

available_wallets = orchestrator.list_available_wallets()
wallet_options = {
    f"{Path(item['relative_key']).name}": item["relative_key"] for item in available_wallets
}

selected_wallet_label = st.sidebar.selectbox(
    "Portefeuille S3 (optionnel)",
    options=["Aucun"] + list(wallet_options.keys()),
)

uploaded_wallet = st.sidebar.file_uploader(
    "Ou importer un portefeuille CSV local",
    type=["csv"],
    help="Le fichier doit contenir les colonnes Symbol/Ticker, Company/Entreprise, Sector/Secteur, Weight/Poids.",
)

st.sidebar.info(
    "Cliquez sur **Lancer l'analyse** pour déclencher les agents IA (LawAgent, MarketAgent, PortfolioAgent, SynthesisAgent)."
)

if st.sidebar.button("Lancer l'analyse 🚀", use_container_width=True):
    if not selected_law_keys:
        st.error("Veuillez sélectionner au moins une loi dans S3.")
    else:
        try:
            if uploaded_wallet is not None:
                wallet_df = orchestrator.load_wallet("upload", uploaded_bytes=uploaded_wallet.getvalue())
                st.session_state[STATE_KEYS["selected_wallet"]] = "upload"
            elif selected_wallet_label != "Aucun":
                wallet_df = orchestrator.load_wallet(wallet_options[selected_wallet_label])
                st.session_state[STATE_KEYS["selected_wallet"]] = wallet_options[selected_wallet_label]
            else:
                st.error("Merci de fournir un portefeuille (S3 ou local).")
                wallet_df = None

            if wallet_df is not None:
                results = orchestrator.run_analysis(
                    wallet_df,
                    selected_law_keys,
                    user_label=user_label or "analyse",
                )
                st.session_state[STATE_KEYS["wallet_df"]] = wallet_df
                st.session_state[STATE_KEYS["analysis_results"]] = results
                st.session_state[STATE_KEYS["selected_laws"]] = selected_law_keys
                st.session_state[STATE_KEYS["user_label"]] = user_label
                st.success("Analyse terminée avec succès ! Consultez les onglets ci-dessous.")
        except Exception as exc:
            st.exception(exc)


# --------------------------- Contenu conditionnel -----------------------------

results = st.session_state.get(STATE_KEYS["analysis_results"])
wallet_df = st.session_state.get(STATE_KEYS["wallet_df"])

tabs = st.tabs(
    [
        "Portefeuille actuel",
        "Analyse réglementaire",
        "Visualisations",
        "Recommandations",
        "Exports S3",
    ]
)


# --- Onglet 1 : portefeuille --------------------------------------------------
with tabs[0]:
    st.subheader("Portefeuille utilisateur - données initiales")
    if wallet_df is None:
        st.info("Aucun portefeuille chargé pour le moment.")
    else:
        col1, col2 = st.columns([2, 1])
        with col1:
            st.dataframe(wallet_df, use_container_width=True, hide_index=True)
        with col2:
            st.metric("Nombre de lignes", len(wallet_df))
            st.metric("Poids total (%)", round(wallet_df["Poids"].sum(), 2))


# --- Onglet 2 : analyse réglementaire ----------------------------------------
with tabs[1]:
    st.subheader("Analyse des lois sélectionnées")
    if results is None:
        st.info("Lancez une analyse pour visualiser les détails.")
    else:
        law_impacts = results["law_impacts"]
        for impact in law_impacts:
            with st.expander(f"{impact.law_title}", expanded=False):
                st.markdown(f"**Résumé** : {impact.summary}")
                st.write("**Mesures clés** :", ", ".join(impact.key_measures) or "Non détectées")
                st.write("**Thèmes** :", ", ".join(impact.key_themes) or "Non détectés")
                if impact.sector_impacts:
                    law_df = pd.DataFrame.from_dict(impact.sector_impacts, orient="index")
                    law_df.index.name = "Secteur"
                    st.dataframe(law_df, use_container_width=True)
                else:
                    st.info("Aucun impact sectoriel significatif détecté.")


# --- Onglet 3 : visualisations ------------------------------------------------
with tabs[2]:
    st.subheader("Comparaison du portefeuille et projections")
    if results is None:
        st.info("Lancez une analyse pour générer des visualisations.")
    else:
        projected_df = results["portfolio_projection"]
        market_view = results["market_view"]

        col1, col2 = st.columns(2)
        with col1:
            fig_current = px.pie(
                wallet_df,
                values="Poids",
                names="Secteur",
                title="Répartition actuelle par secteur",
            )
            st.plotly_chart(fig_current, use_container_width=True)

        with col2:
            fig_projected = px.pie(
                projected_df,
                values="Poids_projete_normalise",
                names="Secteur",
                title="Répartition projetée par secteur",
            )
            st.plotly_chart(fig_projected, use_container_width=True)

        st.markdown("### Variations par entreprise")
        fig_variation = px.bar(
            projected_df,
            x="Entreprise",
            y="Variation_pct",
            color="Variation_pct",
            color_continuous_scale="RdYlGn",
            title="Variation en pourcentage des poids",
        )
        st.plotly_chart(fig_variation, use_container_width=True)

        st.markdown("### Impact de marché par secteur")
        market_df = pd.DataFrame.from_dict(market_view, orient="index").reset_index().rename(columns={"index": "Secteur"})
        fig_market = px.bar(
            market_df,
            x="Secteur",
            y="average_impact",
            color="average_impact",
            color_continuous_scale="RdYlGn",
            title="Coefficient moyen d'impact réglementaire",
        )
        st.plotly_chart(fig_market, use_container_width=True)


# --- Onglet 4 : recommandations -----------------------------------------------
with tabs[3]:
    st.subheader("Recommandations d'ajustement")
    if results is None:
        st.info("Lancez une analyse pour générer des recommandations.")
    else:
        synthesis = results["synthesis"]
        recs_df = pd.DataFrame(synthesis["recommendations"])
        st.dataframe(recs_df, use_container_width=True, hide_index=True)

        st.markdown("### Synthèse automatique")
        summary = synthesis["summary"]

        def _format_sector(sector_entry: Dict) -> str:
            return (
                f"- **{sector_entry['secteur'].replace('_', ' ').title()}** : "
                f"impact moyen {sector_entry['average_impact']} /10, "
                f"flux anticipé {sector_entry['expected_flux_musd']} M$"
            )

        if summary["top_secteurs_positifs"]:
            st.markdown("**Secteurs favorisés :**")
            st.write("\n".join(_format_sector(entry) for entry in summary["top_secteurs_positifs"]))
        if summary["top_secteurs_negatifs"]:
            st.markdown("**Secteurs pénalisés :**")
            st.write("\n".join(_format_sector(entry) for entry in summary["top_secteurs_negatifs"]))

        st.markdown("**Répartition des actions suggérées :**")
        st.json(summary["répartition_actions"])

        st.markdown("**Résumé des lois analysées :**")
        for law in summary["resume_lois"]:
            st.write(
                textwrap.dedent(
                    f"""
                    • {law['titre']}
                        - Thèmes : {", ".join(law['themes']) or "N/A"}
                        - Mesures : {", ".join(law['mesures']) or "N/A"}
                        - Secteurs ciblés : {", ".join(law['secteurs_cibles']) or "N/A"}
                    """
                )
            )


# --- Onglet 5 : exports -------------------------------------------------------
with tabs[4]:
    st.subheader("Exports et sauvegardes S3")
    if results is None:
        st.info("Une fois l'analyse terminée, les exports seront listés ici.")
    else:
        exports = results["exports"]
        st.success("Résultats sauvegardés sur S3 :")
        for label, uri in exports.items():
            st.write(f"- **{label.replace('_', ' ').title()}** : {uri}")

        st.download_button(
            "Télécharger le portefeuille projeté (CSV)",
            data=results["portfolio_projection"].to_csv(index=False).encode("utf-8"),
            file_name="wallet_projection.csv",
            mime="text/csv",
        )

        st.download_button(
            "Télécharger les recommandations (JSON)",
            data=json.dumps(results["synthesis"], ensure_ascii=False, indent=2).encode("utf-8"),
            file_name="recommandations.json",
            mime="application/json",
        )


st.caption("© 2025 - Plateforme IA MarketTech. Toutes les données restent hébergées de manière sécurisée sur S3.")
