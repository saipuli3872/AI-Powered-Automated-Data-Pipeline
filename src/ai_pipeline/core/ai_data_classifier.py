# src/ai_pipeline/core/ai_data_classifier.py

import re
import pandas as pd
from enum import Enum
from dataclasses import dataclass, field
from typing import Any, List, Optional

class DataType(Enum):
    IDENTIFIER    = "identifier"
    BUSINESS_KEY  = "business_key"
    DATE          = "date"
    NUMERIC       = "numeric"
    TEXT          = "text"
    BOOLEAN       = "boolean"

class PIILevel(Enum):
    NONE          = "none"
    LOW           = "low"
    MEDIUM        = "medium"
    HIGH          = "high"

@dataclass
class ColumnProfile:
    suggested_name: str
    data_type: DataType
    is_primary_key: bool = False
    is_business_key: bool = False
    references: List[str] = field(default_factory=list)
    pii_level: PIILevel = PIILevel.NONE
    unique_ratio: float = 0.0
    sample_values: List[Any] = field(default_factory=list)

class AIDataClassifier:
    def __init__(self, sample_size: int = 1000):
        self.sample_size = sample_size

    def analyze_column(self, df: pd.DataFrame, col: str) -> ColumnProfile:
        # 1. Sample data
        series = df[col].dropna().astype(str)
        sample = series.sample(min(len(series), self.sample_size), random_state=0)

        # 2. Compute basic stats
        unique_ratio = sample.nunique() / len(sample) if len(sample) else 0

        # 3. Detect DataType by pattern
        if unique_ratio > 0.9 and re.search(r"id$", col, re.IGNORECASE):
            data_type = DataType.IDENTIFIER
            is_primary_key = True
        elif re.search(r"date|day|year", col, re.IGNORECASE):
            data_type = DataType.DATE
            is_primary_key = False
        elif sample.str.match(r"^-?\d+(\.\d+)?$").all():
            data_type = DataType.NUMERIC
            is_primary_key = False
        elif sample.str.lower().isin(["true","false","0","1"]).all():
            data_type = DataType.BOOLEAN
            is_primary_key = False
        else:
            data_type = DataType.TEXT
            is_primary_key = False

        # 4. Enhanced business key detection
        is_business_key = self._detect_business_key(col, sample, unique_ratio, data_type)

        # 5. Enhanced foreign key detection
        references = self._detect_foreign_keys(df, col, sample)

        # 6. Enhanced PII detection
        pii_level = self._detect_pii(sample, col)

        # 7. Build suggested name (snake_case)
        suggested_name = re.sub(r"[^0-9a-zA-Z]+", "_", col).lower()

        return ColumnProfile(
            suggested_name=suggested_name,
            data_type=data_type,
            is_primary_key=is_primary_key,
            is_business_key=is_business_key,
            references=references,
            pii_level=pii_level,
            unique_ratio=unique_ratio,
            sample_values=sample.tolist()[:5]
        )

    def _detect_foreign_keys(self, df: pd.DataFrame, col: str, sample: pd.Series) -> List[str]:
        """Enhanced foreign key detection using multiple heuristics"""
        references = []
        
        for other_col in df.columns:
            if other_col == col:
                continue
                
            # Heuristic 1: Name similarity
            if re.search(rf"{other_col}$", col, re.IGNORECASE):
                references.append(other_col)
                continue
            
            # Heuristic 2: Value overlap analysis
            other_sample = df[other_col].dropna().astype(str)
            if len(other_sample) > 0 and len(sample) > 0:
                overlap_ratio = len(set(sample) & set(other_sample)) / len(set(sample))
                
                # High overlap suggests foreign key relationship
                if overlap_ratio > 0.7:
                    references.append(other_col)
                    continue
            
            # Heuristic 3: Cardinality relationship
            if len(other_sample) > 0 and sample.nunique() >= other_sample.nunique() * 0.8:
                # Check if this could be a many-to-one relationship
                unique_pairs = df[[col, other_col]].drop_duplicates()
                if len(unique_pairs) == sample.nunique():
                    references.append(other_col)
        
        return references

    def _detect_business_key(self, col: str, sample: pd.Series, unique_ratio: float, data_type: DataType) -> bool:
        """Enhanced business key detection using domain patterns"""
        
        # Pattern 1: Common business key names
        business_patterns = [
            r".*code$", r".*type$", r".*status$", r".*category$",
            r".*class$", r".*group$", r".*dept", r".*region$"
        ]
        
        for pattern in business_patterns:
            if re.search(pattern, col, re.IGNORECASE):
                return True
        
        # Pattern 2: Low cardinality text with meaningful values
        if data_type == DataType.TEXT and unique_ratio < 0.3 and len(sample) > 10:
            # Check if values look like codes/categories
            typical_codes = sample.str.match(r"^[A-Z]{2,4}$|^[A-Z][0-9]{1,3}$").mean()
            if typical_codes > 0.5:
                return True
        
        # Pattern 3: Enum-like values
        if data_type == DataType.TEXT and unique_ratio < 0.1 and sample.nunique() < 20:
            return True
        
        # Pattern 4: Original logic fallback
        return (data_type == DataType.TEXT and unique_ratio < 0.5)

    def _detect_pii(self, sample: pd.Series, col: str) -> PIILevel:
        """Enhanced PII detection with multiple patterns"""
        
        # HIGH PII patterns
        high_patterns = [
            (r"^[\w.+-]+@[\w-]+\.[\w.-]+$", "email"),
            (r"^\d{3}-\d{2}-\d{4}$", "ssn"),
            (r"^\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}$", "credit_card"),
            (r"^\+?1?\d{9,15}$", "phone")
        ]
        
        for pattern, pii_type in high_patterns:
            if sample.str.match(pattern).any():
                return PIILevel.HIGH
        
        # MEDIUM PII patterns
        medium_patterns = [
            (r"^[A-Z][a-z]+ [A-Z][a-z]+$", "full_name"),
            (r"^\d{1,5} .+ (St|Ave|Rd|Dr|Blvd)", "address")
        ]
        
        for pattern, pii_type in medium_patterns:
            if sample.str.match(pattern).any():
                return PIILevel.MEDIUM
        
        # LOW PII - check column names
        low_pii_names = ["name", "first", "last", "address", "city", "zip"]
        if any(keyword in col.lower() for keyword in low_pii_names):
            return PIILevel.LOW
        
        return PIILevel.NONE
