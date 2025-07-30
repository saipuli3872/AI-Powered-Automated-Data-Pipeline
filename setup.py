#!/usr/bin/env python3
"""
Setup configuration for AI-Powered Automated Data Pipeline
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read long description from README
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding='utf-8')

# Read requirements from requirements.txt
with open('requirements.txt', 'r', encoding='utf-8') as f:
    requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]

setup(
    name="ai-automated-data-pipeline",
    version="0.1.0",
    author="Your Name",
    author_email="your.email@example.com",
    description="AI-Powered Automated Data Pipeline with Zero-Code Data Vault 2.0",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/AI-Powered-Automated-Data-Pipeline",
    project_urls={
        "Bug Tracker": "https://github.com/yourusername/AI-Powered-Automated-Data-Pipeline/issues",
        "Documentation": "https://github.com/yourusername/AI-Powered-Automated-Data-Pipeline/docs",
        "Source Code": "https://github.com/yourusername/AI-Powered-Automated-Data-Pipeline",
    },
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: System :: Distributed Computing",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.4.0",
            "pre-commit>=3.3.0",
        ],
        "docs": [
            "sphinx>=7.0.0",
            "sphinx-rtd-theme>=1.3.0",
            "myst-parser>=2.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "ai-pipeline=ai_pipeline.main:main",
            "generate-sample-data=generate_sample_data:main",
        ],
    },
    include_package_data=True,
    package_data={
        "ai_pipeline": ["config/*.yaml", "templates/*.sql"],
    },
    zip_safe=False,
    keywords=[
        "ai", "machine-learning", "data-pipeline", "data-vault", "automation",
        "etl", "data-engineering", "artificial-intelligence", "zero-code"
    ],
)