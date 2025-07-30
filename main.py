#!/usr/bin/env python3
"""
AI-Powered Automated Data Pipeline - Main Entry Point
====================================================

This module serves as the main entry point for the AI-Powered Automated Data Pipeline.
It provides a command-line interface for running the pipeline and managing its components.

Usage:
    python -m ai_pipeline.main --help
    python -m ai_pipeline.main --run-pipeline
    python -m ai_pipeline.main --generate-data
    python -m ai_pipeline.main --web-app
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('ai_pipeline.log')
    ]
)

logger = logging.getLogger(__name__)

def setup_logging(log_level: str = "INFO") -> None:
    """Configure logging for the application."""
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {log_level}')
    
    logging.getLogger().setLevel(numeric_level)
    logger.info(f"Logging level set to {log_level}")

def run_pipeline() -> None:
    """Run the main AI-powered data pipeline."""
    logger.info("Starting AI-Powered Automated Data Pipeline...")
    
    try:
        # This will be implemented in Step 2
        logger.info("Pipeline execution will be implemented in Step 2")
        logger.info("Current status: Step 1 (Foundation) completed")
        
        # Placeholder for pipeline execution
        print("🚀 AI-Powered Automated Data Pipeline")
        print("📊 Step 1: Foundation - COMPLETED")
        print("🔄 Step 2: Core AI Engine - PENDING")
        print("📋 Run 'python generate_sample_data.py' to create sample datasets")
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        sys.exit(1)

def generate_sample_data() -> None:
    """Generate sample data for testing and development."""
    logger.info("Generating sample data...")
    
    try:
        # Import and run the data generation script
        import subprocess
        result = subprocess.run([sys.executable, "generate_sample_data.py"], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            print(result.stdout)
            logger.info("Sample data generation completed successfully")
        else:
            print(result.stderr)
            logger.error("Sample data generation failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Failed to generate sample data: {e}")
        sys.exit(1)

def run_web_app() -> None:
    """Launch the web application interface."""
    logger.info("Starting web application...")
    
    try:
        # This will be implemented in later steps
        logger.info("Web application will be implemented in Step 5")
        print("🌐 Web Application")
        print("📊 Status: Will be implemented in Step 5")
        print("🔄 Current focus: Building core AI components first")
        
    except Exception as e:
        logger.error(f"Web application startup failed: {e}")
        sys.exit(1)

def check_environment() -> bool:
    """Check if the environment is properly configured."""
    logger.info("Checking environment configuration...")
    
    required_dirs = ["data", "src", "tests"]
    missing_dirs = []
    
    for dir_name in required_dirs:
        if not Path(dir_name).exists():
            missing_dirs.append(dir_name)
    
    if missing_dirs:
        logger.warning(f"Missing directories: {missing_dirs}")
        print(f"⚠️  Missing directories: {', '.join(missing_dirs)}")
        return False
    
    logger.info("Environment check passed")
    print("✅ Environment configuration OK")
    return True

def show_project_status() -> None:
    """Display current project status and next steps."""
    print("\n🎯 AI-Powered Automated Data Pipeline - Project Status")
    print("=" * 60)
    print("✅ Step 1: Project Foundation & Setup - COMPLETED")
    print("   - Professional project structure")
    print("   - Sample data generation (11,400 records)")
    print("   - CI/CD pipeline with GitHub Actions")
    print("   - Comprehensive documentation")
    print("")
    print("🔄 Next Steps:")
    print("   Step 2: Core AI Data Classifier")
    print("   Step 3: Data Vault Generator") 
    print("   Step 4: Pipeline Orchestrator")
    print("   Step 5: Web Interface")
    print("   Step 6: Advanced Features")
    print("")
    print("🚀 Ready for GitHub push and Step 2 development!")

def main() -> None:
    """Main function that handles command-line arguments."""
    parser = argparse.ArgumentParser(
        description="AI-Powered Automated Data Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m ai_pipeline.main --status          Show project status
  python -m ai_pipeline.main --run-pipeline    Run the data pipeline
  python -m ai_pipeline.main --generate-data   Generate sample datasets
  python -m ai_pipeline.main --web-app         Launch web interface
  python -m ai_pipeline.main --check-env       Check environment setup
        """
    )
    
    parser.add_argument(
        "--run-pipeline",
        action="store_true",
        help="Run the main AI-powered data pipeline"
    )
    
    parser.add_argument(
        "--generate-data", 
        action="store_true",
        help="Generate sample data for testing"
    )
    
    parser.add_argument(
        "--web-app",
        action="store_true", 
        help="Launch the web application interface"
    )
    
    parser.add_argument(
        "--check-env",
        action="store_true",
        help="Check environment configuration"
    )
    
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show current project status"
    )
    
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set logging level (default: INFO)"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    
    # If no arguments provided, show status
    if not any(vars(args).values()):
        show_project_status()
        return
    
    # Execute requested action
    if args.status:
        show_project_status()
    elif args.check_env:
        check_environment()
    elif args.generate_data:
        generate_sample_data()
    elif args.run_pipeline:
        run_pipeline()
    elif args.web_app:
        run_web_app()

if __name__ == "__main__":
    main()