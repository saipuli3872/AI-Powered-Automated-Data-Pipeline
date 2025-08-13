# scripts/run_hybrid_analysis.py  (replace the previous file)

import sys, os, json
sys.path.insert(0, 'src')

from ai_pipeline.pipeline import validate_input_file
from ai_pipeline.core.ai_enhanced_classifier import AIEnhancedClassifier

CSV_PATH = "data/sample/customers.csv"        # change if needed
df  = validate_input_file(CSV_PATH)

clf = AIEnhancedClassifier()                  # pattern + Gemini

results = []
for col in df.columns:
    profile, insight = clf.analyze_column_with_ai(df, col)
    results.append((profile, insight))

print(f"\n✅ Loaded {CSV_PATH} – {len(df)} rows  {len(df.columns)} cols\n")
print("────────── COLUMN REPORT ──────────")
for profile, insight in results:
    print(f"{profile.suggested_name:<18} | "
          f"Pattern {profile.data_type.value:<12} | "
          f"AI {insight.ai_classification.value:<12} "
          f"(conf {insight.confidence_score:.2f})")

# crude summary
agree = sum(1 for p,i in results if p.data_type==i.ai_classification)
print("\n──────────── SUMMARY ─────────────")
print(json.dumps({
    "columns"          : len(results),
    "pattern_ai_agree" : agree,
    "agree_pct"        : round(agree/len(results)*100,1) if results else 0
}, indent=2))
