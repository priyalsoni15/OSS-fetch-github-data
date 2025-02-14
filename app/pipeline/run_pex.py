import os
import sys
import pandas as pd
import traceback
import logging
from dotenv import load_dotenv
from .update_pex import ensure_pex_generator_repo

load_dotenv()

PEX_GENERATOR_DIR = os.getenv("PEX_GENERATOR_DIR")
if not PEX_GENERATOR_DIR:
    raise Exception("PEX_GENERATOR_DIR environment variable is not set in your .env file.")
if PEX_GENERATOR_DIR not in sys.path:
    sys.path.insert(0, PEX_GENERATOR_DIR)

original_cwd = os.getcwd()
os.chdir(PEX_GENERATOR_DIR)
try:
    from decalfc.app.server import compute_forecast
finally:
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
    
    request_pkg = {
        "project_name": project,
        "tech_data": tech_df,
        "social_data": social_df,
        "tasks": tasks.split(","),
        "month_range": [int(x) for x in month_range.split(",")]
    }
    
    try:
        original_dir = os.getcwd()
        os.chdir(PEX_GENERATOR_DIR)
        try:
            result = compute_forecast(request_pkg)
        finally:
            os.chdir(original_dir)
        # Convert result if it is a DataFrame.
        if isinstance(result, pd.DataFrame):
            result = result.to_dict(orient='records')
        return result
    except Exception as e:
        return {"error": str(e), "traceback": traceback.format_exc()}
