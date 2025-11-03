#!/usr/bin/env python3
"""
Agentic law analyzer that fetches legislative text from S3 and produces sector insights.
"""

import asyncio
import json
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import boto3

BASE_DIR = Path(__file__).resolve().parent
ANALYZER_DIR = BASE_DIR / "analyseur_loi"
if ANALYZER_DIR.exists() and str(ANALYZER_DIR) not in sys.path:
    sys.path.append(str(ANALYZER_DIR))

from utils.logger import setup_logger  # type: ignore
from utils.s3_handler import S3Handler  # type: ignore

logger = setup_logger(__name__)


@dataclass
class LawStrand:
    strand_id: str
    task: str
    status: str
    result: Dict = field(default_factory=dict)


class AgenticLawS3Analyzer:
    """Agent orchestrating S3 retrieval, sector analysis, and publishing of legislative insights."""

    def __init__(self, config_path: Optional[Path] = None):
        self.config = self._load_config(config_path)
        self.bucket = self.config["s3_bucket"]
        self.base_path = self.config["s3_base_path"].rstrip("/")
        self.laws_prefix = self.config["input_sources"]["laws_folder"].rstrip("/") + "/"
        self.output_prefix = self.config["output_destinations"]["intermediate_json"]
        self.s3_handler = S3Handler(self.bucket, self.config["s3_base_path"])
        self.s3_client = self.s3_handler.s3_client
        self.bedrock_client = self._build_bedrock_client()
        self.active_strands: List[LawStrand] = []

    def _load_config(self, config_path: Optional[Path]) -> Dict:
        config_file = config_path or (ANALYZER_DIR / "config.json")
        with open(config_file, "r", encoding="utf-8") as handle:
            return json.load(handle)

    def _build_bedrock_client(self):
        region = (
            os.getenv("AWS_BEDROCK_REGION")
            or os.getenv("AWS_REGION")
            or os.getenv("AWS_DEFAULT_REGION")
            or "us-east-1"
        )
        return boto3.client("bedrock-runtime", region_name=region)

    async def analyze_laws_s3(self) -> Dict:
        logger.info("Starting law analyzer against S3", extra={"extra_data": {"agent": "law_s3"}})
        self.active_strands.clear()

        fetch_strand = LawStrand("fetch_laws", "Fetch laws from S3", "running")
        self.active_strands.append(fetch_strand)
        laws_data = await self._fetch_laws_from_s3()
        fetch_strand.status = "completed"
        fetch_strand.result = {"laws_count": len(laws_data)}

        analysis_strand = LawStrand("analyze_sectors", "Analyze sectors per law", "running")
        self.active_strands.append(analysis_strand)
        sector_impacts = await self._analyze_sectors_from_laws(laws_data)
        analysis_strand.status = "completed"
        analysis_strand.result = {"sectors_found": len(sector_impacts)}

        weights_strand = LawStrand("calculate_weights", "Derive sector weights", "running")
        self.active_strands.append(weights_strand)
        final_weights = await self._calculate_sector_weights(sector_impacts)
        weights_strand.status = "completed"
        weights_strand.result = {"weights_calculated": bool(final_weights)}

        save_strand = LawStrand("save_s3", "Persist snapshot to S3", "running")
        self.active_strands.append(save_strand)
        s3_path = await self._save_results_to_s3(final_weights, sector_impacts, laws_data)
        save_strand.status = "completed"
        save_strand.result = {"s3_path": s3_path}

        logger.info(
            "Law analysis completed",
            extra={
                "extra_data": {
                    "agent": "law_s3",
                    "laws_analyzed": len(laws_data),
                    "snapshot_path": s3_path,
                }
            },
        )

        return {
            "sector_weights": final_weights,
            "sector_impacts": sector_impacts,
            "laws_analyzed": len(laws_data),
            "strands_executed": len(self.active_strands),
            "s3_output_path": s3_path,
        }

    async def _fetch_laws_from_s3(self) -> List[Dict]:
        laws: List[Dict] = []
        try:
            law_keys = self.s3_handler.list_objects(self.laws_prefix)
        except Exception as exc:
            logger.error("Failed to list laws on S3: %s", exc)
            return laws

        for key in law_keys:
            if key.endswith("/"):
                continue
            try:
                response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
                text = response["Body"].read().decode("utf-8", errors="ignore")
                size = response.get("ContentLength", len(text))
            except Exception as exc:
                logger.warning("Could not read law %s: %s", key, exc)
                continue

            laws.append(
                {
                    "s3_key": key,
                    "filename": Path(key).name,
                    "content": text[:5000],
                    "size": size,
                }
            )

        logger.info(
            "Fetched %d law files from S3",
            len(laws),
            extra={"extra_data": {"agent": "law_s3", "prefix": self.laws_prefix}},
        )
        return laws

    async def _analyze_sectors_from_laws(self, laws_data: List[Dict]) -> Dict:
        sector_impacts: Dict[str, Dict] = {}

        for law in laws_data:
            impacts = await self._analyze_single_law(law)

            for sector, impact_data in impacts.items():
                sector_entry = sector_impacts.setdefault(
                    sector,
                    {
                        "total_impact": 0.0,
                        "law_count": 0,
                        "details": [],
                    },
                )
                sector_entry["total_impact"] += impact_data["score"]
                sector_entry["law_count"] += 1
                sector_entry["details"].append(
                    {
                        "law": law["filename"],
                        "impact": impact_data["score"],
                        "type": impact_data["type"],
                    }
                )

        for sector, data in sector_impacts.items():
            count = max(data["law_count"], 1)
            data["average_impact"] = round(data["total_impact"] / count, 3)

        return sector_impacts

    async def _analyze_single_law(self, law: Dict) -> Dict:
        prompt = (
            "Analyse this law and list impacted economic sectors.\n\n"
            f"Law: {law['filename']}\n"
            f"Content: {law['content']}\n\n"
            "Return only JSON with the sectors and their impacts. Example:\n"
            '{\n'
            '  "Technology": {"score": 5.2, "type": "positive"},\n'
            '  "Energy": {"score": -3.1, "type": "negative"}\n'
            "}\n\n"
            "Allowed sectors: Technology, Financial, Healthcare, Energy, "
            "Consumer_Staples, Consumer_Discretionary, Industrials, Materials, "
            "Telecommunications, Utilities"
        )

        try:
            response = self.bedrock_client.invoke_model(
                modelId="anthropic.claude-3-haiku-20240307-v1:0",
                body=json.dumps(
                    {
                        "anthropic_version": "bedrock-2023-05-31",
                        "max_tokens": 400,
                        "messages": [{"role": "user", "content": prompt}],
                    }
                ),
            )

            payload = json.loads(response["body"].read())
            text = payload["content"][0]["text"]

            start = text.find("{")
            end = text.rfind("}") + 1
            if start != -1 and end != -1:
                return json.loads(text[start:end])

        except Exception as exc:
            logger.warning("Sector analysis failed for %s: %s", law["filename"], exc)

        return {}

    async def _calculate_sector_weights(self, sector_impacts: Dict) -> Dict:
        base_weights = {
            "Technology": 0.25,
            "Financial": 0.15,
            "Healthcare": 0.12,
            "Consumer_Discretionary": 0.10,
            "Industrials": 0.08,
            "Energy": 0.08,
            "Consumer_Staples": 0.06,
            "Utilities": 0.05,
            "Materials": 0.05,
            "Telecommunications": 0.04,
            "Other": 0.02,
        }

        final_weights = base_weights.copy()

        for sector, impact_data in sector_impacts.items():
            if sector in final_weights:
                avg_impact = impact_data["average_impact"]
                adjustment = avg_impact * 0.01
                final_weights[sector] = max(0.01, min(0.4, final_weights[sector] + adjustment))

        total = sum(final_weights.values()) or 1.0
        return {name: round(weight / total, 4) for name, weight in final_weights.items()}

    async def _save_results_to_s3(
        self,
        final_weights: Dict,
        sector_impacts: Dict,
        laws_data: List[Dict],
    ) -> str:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        relative_key = f"{self.output_prefix}law_s3_snapshot_{timestamp}.json"

        law_summaries = [
            {
                "filename": law["filename"],
                "s3_key": law["s3_key"],
                "size": law["size"],
            }
            for law in laws_data
        ]

        output_data = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "laws": law_summaries,
            "sector_impacts": sector_impacts,
            "sector_weights": final_weights,
        }

        try:
            self.s3_handler.write_json(relative_key, output_data)
            return f"s3://{self.bucket}/{self.base_path}/{relative_key}"
        except Exception as exc:
            logger.error("Could not write snapshot to S3: %s", exc)
            return ""

    def display_results(self, results: Dict):
        separator = "=" * 60
        print(f"\n{separator}")
        print("LAW S3 ANALYZER RESULTS")
        print(separator)
        print("\nAnalysis summary:")
        print(f"  Laws analyzed: {results['laws_analyzed']}")
        print(f"  Strands executed: {results['strands_executed']}\n")

        print("Sector impacts:")
        for sector, data in results["sector_impacts"].items():
            avg_impact = data.get("average_impact", 0)
            impact_label = "positive" if avg_impact > 0 else "negative" if avg_impact < 0 else "neutral"
            print(f"  - {sector}: {avg_impact:+.2f} ({impact_label}, {data['law_count']} laws)")

        print("\nFinal weights:")
        for sector, weight in sorted(results["sector_weights"].items(), key=lambda item: item[1], reverse=True):
            if weight > 0.01:
                print(f"  - {sector:<22} {weight:6.2%}")

        if results.get("s3_output_path"):
            print(f"\nOutput snapshot: {results['s3_output_path']}")
        print(separator)


async def main():
    analyzer = AgenticLawS3Analyzer()
    results = await analyzer.analyze_laws_s3()
    analyzer.display_results(results)
    return results


if __name__ == "__main__":
    asyncio.run(main())
