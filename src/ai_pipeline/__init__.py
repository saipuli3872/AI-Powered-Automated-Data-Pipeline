"""
AI-Powered Automated Data Pipeline
==================================

A next-generation, fully autonomous data engineering platform that eliminates 
manual data pipeline creation through advanced AI and machine learning.

Key Features:
- AI-driven data classification and PII detection
- Zero-code Data Vault 2.0 implementation  
- Self-learning architecture with continuous adaptation
- Conversational interface for natural language pipeline creation
- Self-healing pipelines with automatic error recovery
- Real-time processing with sub-minute latency
- Advanced compliance with automated GDPR/CCPA monitoring

This package provides the core components for building automated data pipelines
that require minimal human intervention while delivering enterprise-grade
reliability and performance.
"""

__version__ = "0.1.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"
__license__ = "MIT"

# Import main components
from . import core
from . import agents  
from . import connectors
from . import utils

# Package metadata
__all__ = [
    "core",
    "agents", 
    "connectors",
    "utils",
    "__version__",
    "__author__",
    "__email__",
    "__license__"
]

def get_version():
    """Return the current version of the package."""
    return __version__

def get_info():
    """Return package information."""
    return {
        "name": "AI-Powered Automated Data Pipeline",
        "version": __version__,
        "author": __author__,
        "email": __email__,
        "license": __license__,
        "description": "Autonomous data engineering platform with AI-driven automation"
    }