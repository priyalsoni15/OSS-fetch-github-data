# flask-app/pipeline/run_pex.py

import os
import sys
import pandas as pd
import traceback
import logging
from dotenv import load_dotenv
from .update_pex import ensure_pex_generator_repo  # Ensures the repo is cloned/updated

# Load environment variables
load_dotenv()

# Get PEX_GENERATOR_DIR from .env
PEX_GENERATOR_DIR = os.getenv("PEX_GENERATOR_DIR")
if not PEX_GENERATOR_DIR:
    raise Exception("PEX_GENERATOR_DIR environment variable is not set in your .env file.")

# Ensure the repository is cloned/updated and installed
# PEX_GENERATOR_DIR = ensure_pex_generator_repo()

# Add the repository's root to sys.path (for safety, if not already there)
if PEX_GENERATOR_DIR not in sys.path:
    sys.path.insert(0, PEX_GENERATOR_DIR)

# Save the original working directory and change to the package root for import
original_cwd = os.getcwd()
os.chdir(PEX_GENERATOR_DIR)
try:
    # Import the forecasting function normally now.
    from decalfc.app.server import compute_forecast
finally:
    # Restore the original working directory.
    os.chdir(original_cwd)

def process_tech_data(tech_csv_path):
    """Reads the technical CSV into a DataFrame."""
    try:
        logging.info(f"Reading technical CSV from {tech_csv_path}")
        return pd.read_csv(tech_csv_path)
    except Exception as e:
        raise Exception(f"Error reading technical CSV: {e}")

def process_social_data(social_csv_path):
    """Reads the social CSV into a DataFrame."""
    try:
        logging.info(f"Reading social CSV from {social_csv_path}")
        return pd.read_csv(social_csv_path)
    except Exception as e:
        raise Exception(f"Error reading social CSV: {e}")

def run_forecast(tech_csv, social_csv, project, tasks, month_range):
    """Runs the forecasting pipeline and returns results."""
    try:
        tech_df = process_tech_data(tech_csv)
        social_df = process_social_data(social_csv)
    except Exception as e:
        return {"error": str(e)}
    
    # Prepare the payload for the forecasting engine.
    request_pkg = {
        "project_name": project,
        "tech_data": tech_df,
        "social_data": social_df,
        "tasks": tasks.split(","),
        "month_range": [int(x) for x in month_range.split(",")]
    }
    
    try:
        # **** Change: Temporarily change the working directory to PEX_GENERATOR_DIR
        # so that relative paths (like 'ref/params.json') are resolved correctly.
        original_dir = os.getcwd()
        os.chdir(PEX_GENERATOR_DIR)
        try:
            result = compute_forecast(request_pkg)
        finally:
            os.chdir(original_dir)
        return result
    except Exception as e:
        return {"error": str(e), "traceback": traceback.format_exc()}
