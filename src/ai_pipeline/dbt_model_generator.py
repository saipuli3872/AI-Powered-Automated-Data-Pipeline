"""
AI-Powered dbt Model Generator using AutomateDV
==============================================

Generates production-ready dbt models from AI-validated column profiles.
"""

import os, json
from typing import List, Dict
from pathlib import Path

# NEW ► import the hybrid classifier
from ai_pipeline.core.ai_enhanced_classifier import AIEnhancedClassifier

# still need the dataclasses for type hints
from ai_pipeline.core.ai_data_classifier import ColumnProfile, DataType, PIILevel


class AutomateDVModelGenerator:
    """Generate dbt models using AutomateDV macros from AI classification"""

    def __init__(self, dbt_project_path: str):
        self.dbt_project_path = Path(dbt_project_path)
        self.models_path      = self.dbt_project_path / "models"

    # ------------------------------------------------------------------ #
    #  PUBLIC ENTRY-POINT                                                #
    # ------------------------------------------------------------------ #
    def generate_complete_data_vault(self, source_table: str, df):
        """
        Full pipeline for one raw table:
        1. Classify with hybrid Pattern + Gemini
        2. Generate staging, hubs, satellites, links
        3. Write .sql files under models/
        """
        print(f"🚀 Generating Data Vault for: {source_table}")

        # ── 1. Hybrid classification ──────────────────────────────────
        clf      = AIEnhancedClassifier()               # Pattern + Gemini
        results  = clf.analyze_dataframe_hybrid(df)     # list[(profile, insight)]
        profiles = [p for p, _ in results]              # strip AIInsight

        # (optional) show summary
        summary = clf.get_analysis_summary(results)
        print("📊 Classification summary:", json.dumps(summary, indent=2))

        # ── 2. Generate models exactly as before ──────────────────────
        staging_sql    = self.generate_staging_model(source_table, profiles)
        hub_models     = self.generate_hub_models(profiles)
        satellite_sqls = self.generate_satellite_models(profiles)
        link_sqls      = self.generate_link_models(profiles)

        # write files
        self.write_models_to_files({f"stg_{source_table}": staging_sql}, "staging")
        if hub_models:
            self.write_models_to_files(hub_models, "hubs")
        if satellite_sqls:
            self.write_models_to_files(satellite_sqls, "satellites")
        if link_sqls:
            self.write_models_to_files(link_sqls, "links")

        print("✅ Data Vault generation complete!")
        print(f"   Hubs      : {len(hub_models)}")
        print(f"   Satellites: {len(satellite_sqls)}")
        print(f"   Links     : {len(link_sqls)}")

    # ------------------------------------------------------------------ #
    #  EVERYTHING BELOW THIS LINE IS YOUR ORIGINAL CODE                  #
    # ------------------------------------------------------------------ #

    def generate_staging_model(self, source_table: str, profiles: List[ColumnProfile]) -> str:
        columns = {p.suggested_name.upper(): p.suggested_name for p in profiles}
        return f"""
{{{{ config(materialized='view') }}}}

{{{{ automate_dv.stage(
    include_source_columns=true,
    source_model=source('{source_table.lower()}'),
    derived_columns={{}}
) }}}}
"""

    def generate_hub_models(self, profiles: List[ColumnProfile]) -> Dict[str, str]:
        hub_models = {}
        for p in profiles:
            if p.is_primary_key or p.data_type == DataType.BUSINESS_KEY:
                hub_name = f"hub_{p.suggested_name}"
                hub_sql  = f"""
{{{{ config(materialized='incremental') }}}}

{{{{ automate_dv.hub(
    src_pk="{p.suggested_name.upper()}_HK",
    src_nk="{p.suggested_name.upper()}",
    src_ldts="LOAD_DATETIME",
    src_source="RECORD_SOURCE",
    source_model=ref('stg_{p.suggested_name}')
) }}}}
"""
                hub_models[hub_name] = hub_sql
        return hub_models

    def generate_satellite_models(self, profiles: List[ColumnProfile]) -> Dict[str, str]:
        satellites, hub_sat = {}, {}
        for p in profiles:
            if not (p.is_primary_key or p.is_business_key or p.references):
                hub_key = "customer"
                hub_sat.setdefault(hub_key, []).append(p.suggested_name)

        for hub, cols in hub_sat.items():
            if cols:
                sat_name    = f"sat_{hub}_details"
                payload     = ", ".join(f'"{c.upper()}"' for c in cols)
                satellites[sat_name] = f"""
{{{{ config(materialized='incremental') }}}}

{{{{ automate_dv.sat(
    src_pk="{hub.upper()}_HK",
    src_hashdiff="{hub.upper()}_DETAILS_HD",
    src_payload=[{payload}],
    src_eff="EFFECTIVE_FROM",
    src_ldts="LOAD_DATETIME",
    src_source="RECORD_SOURCE",
    source_model=ref('stg_{hub}')
) }}}}
"""
        return satellites

    def generate_link_models(self, profiles: List[ColumnProfile]) -> Dict[str, str]:
        links = {}
        for p in profiles:
            if p.references:
                for ref in p.references:
                    link_name = f"link_{p.suggested_name}_{ref}"
                    links[link_name] = f"""
{{{{ config(materialized='incremental') }}}}

{{{{ automate_dv.link(
    src_pk="{link_name.upper()}_HK",
    src_fk=["{p.suggested_name.upper()}_HK", "{ref.upper()}_HK"],
    src_ldts="LOAD_DATETIME",
    src_source="RECORD_SOURCE",
    source_model=ref('stg_combined')
) }}}}
"""
        return links

    def write_models_to_files(self, models: Dict[str, str], subfolder: str = ""):
        target_dir = self.models_path / subfolder if subfolder else self.models_path
        target_dir.mkdir(parents=True, exist_ok=True)
        for name, sql in models.items():
            path = target_dir / f"{name}.sql"
            with open(path, "w") as f:
                f.write(sql.strip())
            print(f"✅ Generated: {path}")
