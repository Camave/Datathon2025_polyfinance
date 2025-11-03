#!/usr/bin/env python3
"""
MarketTechOrchestrator - orchestrateur d'agents IA pour l'analyse règlementaire et financière.

Ce module fournit :
    - Un orchestrateur principal facile à intégrer côté Streamlit.
    - Des agents spécialisés (lois, marché, portefeuille, synthèse).
    - Une couche utilitaire autour de S3 pour charger et sauver les données utilisateurs.
"""

from __future__ import annotations

import asyncio
import io
import json
import math
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import boto3
import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]


def _load_config(config_path: Optional[Path] = None) -> Dict:
    """Charge la configuration JSON principale du projet."""
    config_file = config_path or (BASE_DIR / "config.json")
    with open(config_file, "r", encoding="utf-8") as handle:
        return json.load(handle)


class S3Portal:
    """Couche d'abstraction légère pour interagir avec S3 côté web-app."""

    def __init__(self, bucket: str, base_path: str = ""):
        self.bucket = bucket
        self.base_path = base_path.rstrip("/")
        self.client = boto3.client("s3")

    def _full_key(self, key: str) -> str:
        if key.startswith("s3://"):
            prefix = f"s3://{self.bucket}/"
            key = key[len(prefix) :] if key.startswith(prefix) else key
        return f"{self.base_path}/{key.lstrip('/')}" if self.base_path else key.lstrip("/")

    def list_objects(self, prefix: str) -> List[Dict]:
        """Retourne la liste d'objets avec métadonnées minimales."""
        full_prefix = self._full_key(prefix)
        response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=full_prefix)
        contents = response.get("Contents", [])
        items = []
        for obj in contents:
            key = obj["Key"]
            size = obj.get("Size", 0)
            if key.endswith("/"):
                continue
            items.append(
                {
                    "key": key,
                    "relative_key": key[len(self.base_path) + 1 :] if self.base_path else key,
                    "size": size,
                    "last_modified": obj.get("LastModified"),
                }
            )
        return items

    def read_text(self, key: str, encoding: str = "utf-8") -> str:
        full_key = self._full_key(key)
        response = self.client.get_object(Bucket=self.bucket, Key=full_key)
        return response["Body"].read().decode(encoding, errors="ignore")

    def read_csv(self, key: str) -> pd.DataFrame:
        full_key = self._full_key(key)
        response = self.client.get_object(Bucket=self.bucket, Key=full_key)
        return pd.read_csv(response["Body"])

    def write_json(self, key: str, data: Dict) -> str:
        full_key = self._full_key(key)
        buffer = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
        self.client.put_object(
            Bucket=self.bucket,
            Key=full_key,
            Body=buffer,
            ContentType="application/json",
        )
        return f"s3://{self.bucket}/{full_key}"

    def write_csv(self, key: str, df: pd.DataFrame) -> str:
        full_key = self._full_key(key)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        self.client.put_object(
            Bucket=self.bucket,
            Key=full_key,
            Body=csv_buffer.getvalue().encode("utf-8"),
            ContentType="text/csv",
        )
        return f"s3://{self.bucket}/{full_key}"


# ---------- Agents spécialisés -------------------------------------------------


@dataclass
class LawImpact:
    law_id: str
    law_title: str
    summary: str
    sector_impacts: Dict[str, Dict]
    key_measures: List[str] = field(default_factory=list)
    key_themes: List[str] = field(default_factory=list)


class LawAgent:
    """Analyse textuelle des lois pour identifier les impacts sectoriels."""

    THEME_KEYWORDS = {
        "utilities": ["grid", "utility", "water", "electricity", "hydro"],
        "energy": ["renewable", "solar", "wind", "fuel", "oil", "gas", "emission"],
        "technology": ["data", "cloud", "ai", "artificial intelligence", "cyber", "digital"],
        "healthcare": ["health", "pharma", "drug", "medical", "patient"],
        "financial": ["bank", "credit", "tax", "finance", "loan", "capital"],
        "industrials": ["manufactur", "factory", "industrial", "supply chain"],
        "materials": ["mining", "chemical", "aluminium", "steel"],
        "consumer_discretionary": ["retail", "consumer", "luxury", "hospitality"],
        "consumer_staples": ["food", "beverage", "staple", "grocery"],
        "telecommunications": ["spectrum", "telecom", "5g", "connectivity"],
    }

    MEASURE_KEYWORDS = {
        "subsidy": ["subsid", "grant", "funding", "credit d'impôt", "tax credit"],
        "tax_increase": ["tax increase", "higher tax", "levy", "surtaxe"],
        "restriction": ["ban", "limit", "restriction", "moratorium"],
        "reporting": ["reporting", "disclosure", "audit"],
        "compliance": ["compliance", "obligation", "standard"],
        "penalty": ["penalty", "fine", "sanction"],
    }

    SECTOR_BASELINE = {
        "utilities": 0,
        "energy": 0,
        "technology": 0,
        "healthcare": 0,
        "financial": 0,
        "industrials": 0,
        "materials": 0,
        "consumer_discretionary": 0,
        "consumer_staples": 0,
        "telecommunications": 0,
    }

    def analyze_laws(self, law_payloads: Sequence[Tuple[str, str, str]]) -> List[LawImpact]:
        impacts: List[LawImpact] = []
        for key, title, text in law_payloads:
            lower = text.lower()
            sector_scores = self.SECTOR_BASELINE.copy()

            for sector, keywords in self.THEME_KEYWORDS.items():
                score = sum(lower.count(word) for word in keywords)
                if score:
                    sector_scores[sector] += score

            measures = [
                measure
                for measure, keywords in self.MEASURE_KEYWORDS.items()
                if any(lower.count(word) for word in keywords)
            ]

            normalized_impacts = {}
            for sector, score in sector_scores.items():
                if score == 0:
                    continue
                coefficient = min(10.0, 3 + math.log(score + 1, 2) * 2)
                impact_type = "positive" if any(
                    m in measures for m in ["subsidy"]
                ) or "green" in title.lower() else "negative" if "tax_increase" in measures else "neutral"
                normalized_impacts[sector] = {
                    "impact_coefficient": round(coefficient, 2),
                    "impact_type": impact_type,
                }

            summary = self._build_summary(title, normalized_impacts, measures)
            impacts.append(
                LawImpact(
                    law_id=Path(key).stem,
                    law_title=title,
                    summary=summary,
                    sector_impacts=normalized_impacts,
                    key_measures=measures,
                    key_themes=[sector for sector, score in sector_scores.items() if score > 0],
                )
            )
        return impacts

    def _build_summary(self, title: str, impacts: Dict[str, Dict], measures: List[str]) -> str:
        if not impacts:
            return f"La loi {title} n'a pas d'impact sectoriel significatif identifié."

        top_sectors = sorted(
            impacts.items(), key=lambda item: item[1]["impact_coefficient"], reverse=True
        )[:3]
        sector_sentence = ", ".join(
            f"{sector.replace('_', ' ').title()} ({data['impact_coefficient']}/10)"
            for sector, data in top_sectors
        )
        measure_sentence = ", ".join(m.replace("_", " ") for m in measures) or "aucune mesure spécifique détectée"
        return (
            f"La loi {title} cible principalement : {sector_sentence}. "
            f"Mesures clés détectées : {measure_sentence}."
        )


class MarketAgent:
    """Projection des impacts au niveau des secteurs et indices."""

    def evaluate_market(self, law_impacts: List[LawImpact]) -> Dict[str, Dict]:
        aggregated: Dict[str, List[float]] = {}
        for impact in law_impacts:
            for sector, data in impact.sector_impacts.items():
                aggregated.setdefault(sector, []).append(data["impact_coefficient"])

        market_view = {}
        for sector, coefficients in aggregated.items():
            avg_coeff = np.mean(coefficients)
            market_view[sector] = {
                "average_impact": round(float(avg_coeff), 2),
                "expected_flux_musd": round(float(avg_coeff * 7.5), 1),
                "volatility_delta": round(float(np.std(coefficients) * 0.8), 2),
            }

        return market_view


class PortfolioAgent:
    """Mesure des impacts spécifiques à un portefeuille."""

    def __init__(self, base_return: float = 0.06):
        self.base_return = base_return

    def evaluate_portfolio(
        self,
        wallet_df: pd.DataFrame,
        market_view: Dict[str, Dict],
    ) -> Tuple[pd.DataFrame, Dict]:
        df = wallet_df.copy()
        df["Secteur"] = df["Secteur"].fillna("autre").apply(lambda x: x.lower().replace(" ", "_"))

        portfolio_value = df["Poids"].sum() or 100.0
        projections = []
        for _, row in df.iterrows():
            sector = row["Secteur"]
            sector_data = market_view.get(sector, {"average_impact": 0.0})
            coeff = sector_data["average_impact"]
            projected_weight = row["Poids"] * (1 + coeff / 20)
            expected_return = self.base_return + (coeff / 100)

            projections.append(
                {
                    "Ticker": row["Ticker"],
                    "Entreprise": row["Entreprise"],
                    "Secteur": sector,
                    "Poids_actuel": round(row["Poids"], 3),
                    "Poids_projete": round(projected_weight, 3),
                    "Variation_pct": round(((projected_weight - row["Poids"]) / max(row["Poids"], 1e-6)) * 100, 2),
                    "Rendement_attendu": round(expected_return * 100, 2),
                }
            )

        projected_df = pd.DataFrame(projections)
        projected_df["Poids_projete_normalise"] = (
            projected_df["Poids_projete"] / projected_df["Poids_projete"].sum()
        ) * portfolio_value

        metrics = {
            "poids_total_initial": round(df["Poids"].sum(), 2),
            "poids_total_projete": round(projected_df["Poids_projete_normalise"].sum(), 2),
            "rendement_moyen_projete": round(float(projected_df["Rendement_attendu"].mean()), 2),
            "volatilite_estimee": round(float(projected_df["Variation_pct"].std()), 2),
        }

        return projected_df, metrics


class SynthesisAgent:
    """Génération des recommandations et synthèse utilisateur."""

    def build_recommendations(
        self,
        projected_df: pd.DataFrame,
        law_impacts: List[LawImpact],
        market_view: Dict[str, Dict],
    ) -> Dict:
        recs = []
        for _, row in projected_df.iterrows():
            variation = row["Variation_pct"]
            if variation > 5:
                action = "Acheter"
            elif variation < -4:
                action = "Vendre"
            else:
                action = "Conserver"

            reason = self._build_reason(row, law_impacts)
            recs.append(
                {
                    "ticker": row["Ticker"],
                    "entreprise": row["Entreprise"],
                    "action": action,
                    "variation_pct": variation,
                    "raison": reason,
                    "rendement_attendu_pct": row["Rendement_attendu"],
                }
            )

        key_findings = self._build_global_summary(law_impacts, market_view, recs)
        return {"recommendations": recs, "summary": key_findings}

    def _build_reason(self, row: pd.Series, law_impacts: List[LawImpact]) -> str:
        sector = row["Secteur"]
        supporting_laws = []
        for impact in law_impacts:
            data = impact.sector_impacts.get(sector)
            if not data:
                continue
            direction = "positif" if data["impact_type"] == "positive" else "négatif"
            supporting_laws.append(f"{impact.law_title} ({direction}, coeff {data['impact_coefficient']}/10)")
        return " ; ".join(supporting_laws) or "Pas d'effet notable identifié."

    def _build_global_summary(
        self,
        law_impacts: List[LawImpact],
        market_view: Dict[str, Dict],
        recs: List[Dict],
    ) -> Dict:
        sectors_sorted = sorted(
            market_view.items(), key=lambda item: item[1]["average_impact"], reverse=True
        )
        positives = [
            {"secteur": sector, **data}
            for sector, data in sectors_sorted
            if data["average_impact"] > 0.5
        ]
        negatives = [
            {"secteur": sector, **data}
            for sector, data in sectors_sorted
            if data["average_impact"] < -0.5
        ]

        actions = {
            "Acheter": sum(1 for rec in recs if rec["action"] == "Acheter"),
            "Vendre": sum(1 for rec in recs if rec["action"] == "Vendre"),
            "Conserver": sum(1 for rec in recs if rec["action"] == "Conserver"),
        }

        return {
            "top_secteurs_positifs": positives[:3],
            "top_secteurs_negatifs": negatives[:3],
            "resume_lois": [
                {
                    "titre": impact.law_title,
                    "themes": impact.key_themes,
                    "mesures": impact.key_measures,
                    "secteurs_cibles": list(impact.sector_impacts.keys()),
                }
                for impact in law_impacts
            ],
            "répartition_actions": actions,
        }


# ---------- Orchestrateur principal -------------------------------------------


class MarketTechOrchestrator:
    """Coordonne les agents et assure la persistance des résultats."""

    def __init__(
        self,
        config_path: Optional[Path] = None,
        output_prefix: str = "webapp_outputs/",
    ):
        self.config = _load_config(config_path)
        self.s3_portal = S3Portal(self.config["s3_bucket"], self.config["s3_base_path"])
        self.output_prefix = output_prefix.rstrip("/") + "/"
        self.law_agent = LawAgent()
        self.market_agent = MarketAgent()
        self.portfolio_agent = PortfolioAgent()
        self.synthesis_agent = SynthesisAgent()

    # --- S3 helpers -----------------------------------------------------------

    def list_available_laws(self) -> List[Dict]:
        return [
            item
            for item in self.s3_portal.list_objects("lois/")
            if item["relative_key"].lower().endswith((".txt", ".html", ".md"))
        ]

    def list_available_wallets(self) -> List[Dict]:
        return [
            item
            for item in self.s3_portal.list_objects("wallet/")
            if item["relative_key"].lower().endswith(".csv")
        ]

    def load_wallet(self, source: str, uploaded_bytes: Optional[bytes] = None) -> pd.DataFrame:
        if source == "upload":
            if uploaded_bytes is None:
                raise ValueError("Aucun fichier local fourni.")
            buffer = io.BytesIO(uploaded_bytes)
            df = pd.read_csv(buffer)
        else:
            df = self.s3_portal.read_csv(source)

        column_mapping = {
            "Symbol": "Ticker",
            "Ticker": "Ticker",
            "Company": "Entreprise",
            "Company_Name": "Entreprise",
            "Poids": "Poids",
            "Weight": "Poids",
            "Weight_percent": "Poids",
            "Sector": "Secteur",
        }
        df = df.rename(columns=column_mapping)
        if "Poids" not in df.columns:
            df["Poids"] = 100 / len(df)
        if "Secteur" not in df.columns:
            df["Secteur"] = "autre"
        if "Entreprise" not in df.columns:
            df["Entreprise"] = df["Ticker"]
        return df[["Ticker", "Entreprise", "Secteur", "Poids"]]

    def _fetch_law_payloads(self, law_keys: Sequence[str]) -> List[Tuple[str, str, str]]:
        payloads = []
        for key in law_keys:
            content = self.s3_portal.read_text(key)
            title = Path(key).name
            payloads.append((key, title, content))
        return payloads

    # --- Analyse principale ---------------------------------------------------

    def run_analysis(
        self,
        wallet_df: pd.DataFrame,
        law_keys: Sequence[str],
        user_label: str,
    ) -> Dict:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        payloads = self._fetch_law_payloads(law_keys)
        law_impacts = self.law_agent.analyze_laws(payloads)
        market_view = self.market_agent.evaluate_market(law_impacts)
        projected_df, portfolio_metrics = self.portfolio_agent.evaluate_portfolio(wallet_df, market_view)
        synthesis = self.synthesis_agent.build_recommendations(projected_df, law_impacts, market_view)

        exports = self._persist_outputs(
            timestamp,
            user_label,
            law_keys,
            law_impacts,
            market_view,
            wallet_df,
            projected_df,
            portfolio_metrics,
            synthesis,
        )

        return {
            "timestamp": timestamp,
            "law_impacts": law_impacts,
            "market_view": market_view,
            "portfolio_projection": projected_df,
            "portfolio_metrics": portfolio_metrics,
            "synthesis": synthesis,
            "exports": exports,
        }

    def _persist_outputs(
        self,
        timestamp: str,
        user_label: str,
        law_keys: Sequence[str],
        law_impacts: List[LawImpact],
        market_view: Dict[str, Dict],
        wallet_df: pd.DataFrame,
        projected_df: pd.DataFrame,
        portfolio_metrics: Dict,
        synthesis: Dict,
    ) -> Dict[str, str]:
        base_key = f"{self.output_prefix}{timestamp}_{user_label}"
        wallet_key = f"{base_key}/wallet_initial.csv"
        projected_key = f"{base_key}/wallet_projection.csv"
        analysis_key = f"{base_key}/analyse_complete.json"

        wallet_uri = self.s3_portal.write_csv(wallet_key, wallet_df)
        projected_uri = self.s3_portal.write_csv(projected_key, projected_df)

        analysis_payload = {
            "timestamp": timestamp,
            "user_label": user_label,
            "laws_analyzed": list(law_keys),
            "law_impacts": [
                {
                    "law_id": impact.law_id,
                    "law_title": impact.law_title,
                    "summary": impact.summary,
                    "sector_impacts": impact.sector_impacts,
                    "key_measures": impact.key_measures,
                    "key_themes": impact.key_themes,
                }
                for impact in law_impacts
            ],
            "market_view": market_view,
            "portfolio_metrics": portfolio_metrics,
            "recommendations": synthesis,
        }
        analysis_uri = self.s3_portal.write_json(analysis_key, analysis_payload)

        return {
            "wallet_initial": wallet_uri,
            "wallet_projection": projected_uri,
            "analyse_complete": analysis_uri,
        }


# ---------- Async helper (non obligatoire, mais utile si réutilisé ailleurs) ---


def run_async_task(task):
    """Exécute une coroutine compatible avec Streamlit (qui n'aime pas asyncio.run)."""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    if loop.is_running():
        return asyncio.ensure_future(task)
    return loop.run_until_complete(task)


__all__ = [
    "MarketTechOrchestrator",
    "LawAgent",
    "MarketAgent",
    "PortfolioAgent",
    "SynthesisAgent",
    "LawImpact",
]
