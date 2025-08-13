#!/usr/bin/env python3
"""
AI-Powered Automated Data Pipeline - Main Entry Point
=====================================================

This script provides end-to-end automation for:
1. Loading and validating input CSV files
2. Running AI-powered data classification 
3. Generating complete Data Vault 2.0 models
4. Producing summary reports and metadata

Usage:
    python pipeline.py input_file.csv --model-name MyVault --output-dir results/
"""

# Standard library imports for file handling and command line processing
import argparse
import logging
import os
import sys
from pathlib import Path
from datetime import datetime
import json
from typing import List

# Data processing library
import pandas as pd

# Import our custom AI pipeline components
from ai_pipeline.core.ai_data_classifier import AIDataClassifier, ColumnProfile
from ai_pipeline.core.data_vault_generator import DataVaultGenerator

# Configure logging with timestamp and level information
logging.basicConfig(
    level=logging.INFO,                                    # Set minimum log level to INFO
    format="%(asctime)s [%(levelname)s] %(message)s",     # Add timestamp to each log message
    handlers=[
        logging.StreamHandler(sys.stdout),                 # Output logs to console
        logging.FileHandler("pipeline.log")               # Also save logs to file
    ]
)
logger = logging.getLogger(__name__)                       # Create logger for this module

def validate_input_file(file_path: str) -> pd.DataFrame:
    """
    Validate and load the input CSV file with error handling
    
    Args:
        file_path: Path to the CSV file to process
        
    Returns:
        pandas.DataFrame: Loaded and validated data
        
    Raises:
        FileNotFoundError: If the file doesn't exist
        ValueError: If the file is empty or invalid
    """
    # Check if the file exists before attempting to load it
    if not os.path.exists(file_path):
        logger.error(f"Input file not found: {file_path}")          # Log the error
        raise FileNotFoundError(f"Input file not found: {file_path}")
    
    logger.info(f"Loading data from: {file_path}")                  # Log the loading attempt
    
    try:
        # Attempt to load the CSV file with pandas
        df = pd.read_csv(file_path)
        
        # Validate that the file contains data
        if df.empty:
            raise ValueError("CSV file is empty")                   # Raise error for empty files
            
        # Validate that the file has columns
        if len(df.columns) == 0:
            raise ValueError("CSV file has no columns")             # Raise error for files without columns
            
        logger.info(f"Successfully loaded {len(df)} rows and {len(df.columns)} columns")  # Log success stats
        return df                                                   # Return the validated DataFrame
        
    except pd.errors.EmptyDataError:
        # Handle pandas-specific empty data error
        logger.error("CSV file is empty or contains no data")
        raise ValueError("CSV file is empty or contains no data")
        
    except pd.errors.ParserError as e:
        # Handle pandas-specific parsing errors
        logger.error(f"Error parsing CSV file: {str(e)}")
        raise ValueError(f"Error parsing CSV file: {str(e)}")

def run_classification(df: pd.DataFrame) -> List[ColumnProfile]:
    """
    Run AI-powered classification on all columns in the DataFrame
    
    Args:
        df: Input DataFrame to classify
        
    Returns:
        List[ColumnProfile]: Classification results for each column
    """
    logger.info("Starting AI-powered data classification...")       # Log classification start
    
    # Initialize the AI classifier with default settings
    classifier = AIDataClassifier(sample_size=1000)                # Use 1000 samples for analysis
    
    # Initialize empty list to store classification results
    profiles = []
    
    # Process each column in the DataFrame
    for i, col in enumerate(df.columns, 1):                        # Enumerate for progress tracking
        logger.info(f"Classifying column {i}/{len(df.columns)}: {col}")  # Log progress
        
        try:
            # Run classification analysis on the current column
            profile = classifier.analyze_column(df, col)            # Get column profile
            profiles.append(profile)                                # Add to results list
            
            # Log classification results for this column
            logger.info(f"  → Type: {profile.data_type.value}, "   # Data type detected
                       f"PK: {profile.is_primary_key}, "           # Primary key status
                       f"BK: {profile.is_business_key}, "          # Business key status  
                       f"FK: {profile.references}, "               # Foreign key references
                       f"PII: {profile.pii_level.value}")          # PII classification
                       
        except Exception as e:
            # Handle any errors during classification of individual columns
            logger.error(f"Error classifying column '{col}': {str(e)}")  # Log the error
            # Continue processing other columns rather than failing completely
            continue
    
    logger.info(f"Classification complete. Processed {len(profiles)} columns successfully")
    return profiles                                                 # Return all classification results

def generate_data_vault_model(profiles: List[ColumnProfile], model_name: str):
    """
    Generate Data Vault 2.0 model from column classifications
    
    Args:
        profiles: List of classified column profiles
        model_name: Name for the generated Data Vault model
        
    Returns:
        DataVaultModel: Generated Data Vault model with Hubs, Links, and Satellites
    """
    logger.info(f"Generating Data Vault 2.0 model: {model_name}")  # Log model generation start
    
    # Initialize the Data Vault generator
    generator = DataVaultGenerator()
    
    try:
        # Generate the complete Data Vault model
        model = generator.generate_model(profiles, model_name)      # Create model from profiles
        
        # Extract model components for logging
        hub_names = [h.name for h in model.hubs]                   # Get list of Hub names
        link_names = [l.name for l in model.links]                 # Get list of Link names  
        satellite_names = [s.name for s in model.satellites]       # Get list of Satellite names
        
        # Log the generated model summary
        logger.info(f"Data Vault model '{model_name}' generated successfully:")
        logger.info(f"  → Hubs: {len(hub_names)} ({hub_names})")              # Log Hub count and names
        logger.info(f"  → Links: {len(link_names)} ({link_names})")           # Log Link count and names
        logger.info(f"  → Satellites: {len(satellite_names)} ({satellite_names})")  # Log Satellite count and names
        
        return model                                               # Return the generated model
        
    except Exception as e:
        # Handle any errors during model generation
        logger.error(f"Error generating Data Vault model: {str(e)}")
        raise                                                      # Re-raise the error to stop pipeline

def save_results(model, profiles: List[ColumnProfile], output_dir: str, input_file: str):
    """
    Save pipeline results to output directory with detailed reporting
    
    Args:
        model: Generated Data Vault model
        profiles: Column classification results  
        output_dir: Directory to save results
        input_file: Original input file path for reference
    """
    logger.info(f"Saving results to: {output_dir}")               # Log save operation start
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)                        # Create directory recursively
    
    # 1. Save model summary as JSON
    model_summary = {
        "metadata": {
            "model_name": model.model_name,                        # Model name
            "generated_at": datetime.now().isoformat(),            # Generation timestamp
            "input_file": os.path.basename(input_file),            # Original file name
            "total_columns": len(profiles)                         # Number of columns processed
        },
        "hubs": [{"name": h.name, "business_keys": h.business_keys} for h in model.hubs],        # Hub details
        "links": [{"name": l.name, "hub_references": l.hub_references} for l in model.links],    # Link details  
        "satellites": [{"name": s.name, "parent_table": s.parent_table, "columns": s.columns} for s in model.satellites]  # Satellite details
    }
    
    # Write model summary to JSON file
    summary_path = os.path.join(output_dir, "model_summary.json") # Build file path
    with open(summary_path, 'w') as f:                            # Open file for writing
        json.dump(model_summary, f, indent=2)                     # Write JSON with formatting
    logger.info(f"Model summary saved to: {summary_path}")        # Log successful save
    
    # 2. Save detailed column profiles as CSV
    profile_data = []                                             # Initialize list for profile data
    for profile in profiles:                                      # Process each column profile
        profile_data.append({                                     # Create dictionary for each profile
            "column_name": profile.suggested_name,                # Column name
            "data_type": profile.data_type.value,                 # Detected data type
            "is_primary_key": profile.is_primary_key,             # Primary key flag
            "is_business_key": profile.is_business_key,           # Business key flag
            "foreign_key_references": "|".join(profile.references),  # FK references (pipe-separated)
            "pii_level": profile.pii_level.value,                 # PII classification
            "unique_ratio": round(profile.unique_ratio, 3),       # Uniqueness ratio (rounded)
            "sample_values": "|".join(map(str, profile.sample_values[:3]))  # Sample values (first 3)
        })
    
    # Convert to DataFrame and save as CSV
    profile_df = pd.DataFrame(profile_data)                       # Create DataFrame from profile data
    profile_path = os.path.join(output_dir, "column_profiles.csv")  # Build CSV file path
    profile_df.to_csv(profile_path, index=False)                 # Save to CSV without row indices
    logger.info(f"Column profiles saved to: {profile_path}")     # Log successful save
    
    # 3. Generate and save human-readable report
    report_lines = []                                             # Initialize report content list
    report_lines.append(f"AI Data Pipeline Results Report")      # Report header
    report_lines.append(f"{'='*50}")                             # Separator line
    report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")  # Timestamp
    report_lines.append(f"Input File: {os.path.basename(input_file)}")  # Input file reference
    report_lines.append(f"Model Name: {model.model_name}")       # Model name
    report_lines.append("")                                      # Empty line for spacing
    
    # Add model statistics to report
    report_lines.append("MODEL SUMMARY:")                        # Section header
    report_lines.append(f"  Total Columns Analyzed: {len(profiles)}")     # Column count
    report_lines.append(f"  Hubs Generated: {len(model.hubs)}")           # Hub count
    report_lines.append(f"  Links Generated: {len(model.links)}")         # Link count  
    report_lines.append(f"  Satellites Generated: {len(model.satellites)}")  # Satellite count
    report_lines.append("")                                      # Empty line
    
    # Add detailed Hub information
    if model.hubs:                                               # If hubs exist
        report_lines.append("HUBS:")                             # Section header
        for hub in model.hubs:                                   # Process each hub
            report_lines.append(f"  • {hub.name}")              # Hub name
            report_lines.append(f"    Business Keys: {', '.join(hub.business_keys)}")  # Business keys
        report_lines.append("")                                  # Empty line
    
    # Add detailed Link information  
    if model.links:                                              # If links exist
        report_lines.append("LINKS:")                            # Section header
        for link in model.links:                                 # Process each link
            report_lines.append(f"  • {link.name}")             # Link name
            report_lines.append(f"    Connects: {', '.join(link.hub_references)}")  # Connected hubs
        report_lines.append("")                                  # Empty line
    
    # Add detailed Satellite information
    if model.satellites:                                         # If satellites exist
        report_lines.append("SATELLITES:")                       # Section header
        for satellite in model.satellites:                      # Process each satellite
            report_lines.append(f"  • {satellite.name}")        # Satellite name
            report_lines.append(f"    Parent: {satellite.parent_table}")  # Parent table
            report_lines.append(f"    Columns: {', '.join(satellite.columns)}")  # Satellite columns
        report_lines.append("")                                  # Empty line
    
    # Write the report to text file
    report_path = os.path.join(output_dir, "pipeline_report.txt")  # Build report file path
    with open(report_path, 'w') as f:                            # Open file for writing
        f.write('\n'.join(report_lines))                         # Write all report lines
    logger.info(f"Pipeline report saved to: {report_path}")     # Log successful save

def main():
    """
    Main entry point for the AI Data Pipeline
    Handles command-line arguments and orchestrates the complete pipeline
    """
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(
        description="AI-Powered Automated Data Pipeline - Generate Data Vault models from CSV files",  # Description
        formatter_class=argparse.RawDescriptionHelpFormatter    # Allow multi-line descriptions
    )
    
    # Define required positional argument for input file
    parser.add_argument(
        "input_csv",                                             # Argument name
        help="Path to the input CSV file to process"            # Help text
    )
    
    # Define optional argument for model name
    parser.add_argument(
        "--model-name",                                          # Long option name
        default="AutoGeneratedVault",                            # Default value if not provided
        help="Name for the generated Data Vault model (default: AutoGeneratedVault)"  # Help text
    )
    
    # Define optional argument for output directory
    parser.add_argument(
        "--output-dir",                                          # Long option name  
        default="data/pipeline_results",                         # Default directory
        help="Directory to save pipeline results (default: data/pipeline_results)"  # Help text
    )
    
    # Parse command-line arguments
    args = parser.parse_args()                                   # Parse provided arguments
    
    logger.info("="*60)                                          # Log separator for clarity
    logger.info("AI-Powered Data Pipeline Starting...")          # Log pipeline start
    logger.info(f"Input File: {args.input_csv}")                # Log input file
    logger.info(f"Model Name: {args.model_name}")               # Log model name
    logger.info(f"Output Directory: {args.output_dir}")         # Log output directory
    logger.info("="*60)                                          # Log separator
    
    try:
        # Step 1: Validate and load input data
        logger.info("Step 1: Loading and validating input data...")  # Log step start
        df = validate_input_file(args.input_csv)                 # Load and validate CSV
        
        # Step 2: Run AI classification on all columns
        logger.info("Step 2: Running AI-powered data classification...")  # Log step start
        profiles = run_classification(df)                        # Classify all columns
        
        # Step 3: Generate Data Vault model
        logger.info("Step 3: Generating Data Vault 2.0 model...")  # Log step start
        model = generate_data_vault_model(profiles, args.model_name)  # Generate model
        
        # Step 4: Save all results
        logger.info("Step 4: Saving results and generating reports...")  # Log step start
        save_results(model, profiles, args.output_dir, args.input_csv)  # Save everything
        
        # Pipeline completion
        logger.info("="*60)                                      # Log separator
        logger.info("🎉 AI Data Pipeline completed successfully!")  # Log success
        logger.info(f"Results saved to: {os.path.abspath(args.output_dir)}")  # Log output location
        logger.info("="*60)                                      # Log separator
        
    except Exception as e:
        # Handle any unexpected errors during pipeline execution
        logger.error("="*60)                                     # Log separator for error
        logger.error(f"❌ Pipeline failed with error: {str(e)}")  # Log the error message
        logger.error("="*60)                                     # Log separator  
        sys.exit(1)                                              # Exit with error code

# Python entry point - only run main() if script is executed directly
if __name__ == "__main__":
    main()                                                       # Call main function
