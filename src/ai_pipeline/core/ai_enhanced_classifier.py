"""
AI-Enhanced Data Classifier  –  Pattern + Google Gemini
------------------------------------------------------

Uses your existing pattern engine for speed and determinism, then lets
Gemini validate / enrich the result when a GEMINI_API_KEY is present.
"""

import os, json
from dataclasses import dataclass
from typing import Tuple, List

import pandas as pd
import google.generativeai as genai
from dotenv import load_dotenv

from .ai_data_classifier import AIDataClassifier, ColumnProfile, DataType

# ------------------------------------------------------------------ #
#  Initialise                                                         #
# ------------------------------------------------------------------ #

load_dotenv()                                    # reads .env if present


@dataclass
class AIInsight:
    confidence_score: float
    business_meaning: str
    data_quality_notes: str
    suggested_improvements: str
    ai_classification: DataType
    reasoning: str


class AIEnhancedClassifier:
    """
    Hybrid classifier – runs pattern logic first, then (optionally) calls
    Google Gemini to validate / correct the result.
    """

    def __init__(self, api_key: str | None = None, sample_size: int = 1_000):
        self.pattern = AIDataClassifier(sample_size)
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        if self.api_key:
            genai.configure(api_key=self.api_key)
            self.model = genai.GenerativeModel("gemini-2.5-pro")
            self.ai_enabled = True
            print("✅ Gemini AI enabled")
        else:
            self.ai_enabled = False
            print("⚠️  GEMINI_API_KEY not set – pattern-only mode")

    # ------------------------------------------------------------------ #
    #  Public helpers                                                    #
    # ------------------------------------------------------------------ #

    def analyze_column_with_ai(
        self, df: pd.DataFrame, col: str
    ) -> Tuple[ColumnProfile, AIInsight]:

        # 1️⃣  pattern pass
        pattern_profile = self.pattern.analyze_column(df, col)

        # 2️⃣  AI pass (optional)
        if not self.ai_enabled:
            return pattern_profile, self._fallback_insight(pattern_profile, "AI disabled")

        ai_insight = self._gemini_insight(df, col, pattern_profile)
        final_profile = self._merge(pattern_profile, ai_insight)
        return final_profile, ai_insight

    # ------------------------------------------------------------------ #
    #  Internal: Gemini call                                             #
    # ------------------------------------------------------------------ #

    def _gemini_insight(
        self, df: pd.DataFrame, col: str, pattern_profile: ColumnProfile
    ) -> AIInsight:

        sample_vals = df[col].dropna().astype(str).head(5).tolist()
        prompt = (
            "You are a data-engineering assistant. Classify the database "
            f"column '{col}'.  Allowed classes: identifier, business_key, "
            "date, numeric, text, boolean.\n\n"
            f"Sample values: {sample_vals}\n"
            f"Total rows: {len(df)}, nulls: {df[col].isnull().sum()}\n"
            f"Pattern suggestion: {pattern_profile.data_type.value}\n\n"
            "Return ONLY valid JSON with exactly these keys:\n"
            '{'
            '"confidence_score":float,'
            '"business_meaning":str,'
            '"data_quality_notes":str,'
            '"suggested_classification":str,'
            '"reasoning":str,'
            '"suggested_improvements":str'
            '}'
        )

        try:
            resp = self.model.generate_content(prompt, safety_settings={})
            text = resp.text.strip()

            # Strip markdown ```
            if text.startswith("```"):
                text = text.split("```json")[1].split("```")[0] if "```json" in text else text.split("```")[1]
            payload = json.loads(text)

            cls_map = {
                "identifier": DataType.IDENTIFIER,
                "business_key": DataType.BUSINESS_KEY,
                "date": DataType.DATE,
                "numeric": DataType.NUMERIC,
                "text": DataType.TEXT,
                "boolean": DataType.BOOLEAN,
            }
            return AIInsight(
                confidence_score=float(payload.get("confidence_score", 0.5)),
                business_meaning=payload.get("business_meaning", ""),
                data_quality_notes=payload.get("data_quality_notes", ""),
                suggested_improvements=payload.get("suggested_improvements", ""),
                ai_classification=cls_map.get(
                    payload.get("suggested_classification", "text"), DataType.TEXT
                ),
                reasoning=payload.get("reasoning", ""),
            )

        except Exception as exc:
            print(f"⚠️  Gemini fail for {col}: {exc}")
            return self._fallback_insight(pattern_profile, str(exc))

    # ------------------------------------------------------------------ #
    #  Internal: merge logic & fallback                                  #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _merge(pattern: ColumnProfile, insight: AIInsight) -> ColumnProfile:
        if (
            insight.confidence_score > 0.8
            and insight.ai_classification != pattern.data_type
        ):
            pattern.data_type = insight.ai_classification
        return pattern

    @staticmethod
    def _fallback_insight(profile: ColumnProfile, msg: str) -> AIInsight:
        return AIInsight(
            confidence_score=0.6,
            business_meaning=f"Pattern: {profile.data_type.value}",
            data_quality_notes=msg,
            suggested_improvements="",
            ai_classification=profile.data_type,
            reasoning="Pattern-only",
        )
    def analyze_dataframe_hybrid(self, df: pd.DataFrame) -> List[Tuple[ColumnProfile, AIInsight]]:
        """Analyze entire DataFrame using hybrid approach"""
        results = []
        print(f"🔍 Analyzing {len(df.columns)} columns with hybrid approach...")
        for i, col in enumerate(df.columns, 1):
            print(f"  Column {i}/{len(df.columns)}: {col}")
            profile, ai_insight = self.analyze_column_with_ai(df, col)
            results.append((profile, ai_insight))
        return results
     # >>> THE NEW METHODS <<<
    def get_analysis_summary(self, results: List[Tuple[ColumnProfile, AIInsight]]) -> dict:
        """Generate summary statistics of the hybrid analysis"""
        total_columns   = len(results)
        agreement_count = sum(1 for profile, ai in results if profile.data_type == ai.ai_classification)

        classification_counts = {}
        for profile, _ in results:
            cls = profile.data_type.value
            classification_counts[cls] = classification_counts.get(cls, 0) + 1

        return {
            "total_columns": total_columns,
            "pattern_ai_agreement": agreement_count,
            "agreement_percentage": round(agreement_count / total_columns * 100, 1) if total_columns else 0,
            "classification_breakdown": classification_counts,
            "ai_enabled": self.ai_enabled
        }
    
