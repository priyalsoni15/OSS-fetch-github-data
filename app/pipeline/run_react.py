# flask-app/pipeline/run_react.py
import os
import sys
import glob
import json
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def run_react():
    """
    Runs the ReACT extraction step:
      1. Ensures the REACT_API_DIR is in sys.path.
      2. Changes working directory to REACT_API_DIR.
      3. Loads the parent JSON from the react_extractor folder.
      4. Finds the CSV file from the net-caches folder of pex-forecaster.
      5. Calls the ReACT_Extractor function (imported from the react_extractor package).
      6. Returns the extracted JSON.
    """
    # Ensure REACT_API_DIR is in sys.path
    react_api_dir = os.getenv("REACT_API_DIR")
    if not react_api_dir:
        raise Exception("REACT_API_DIR is not set in your .env file.")
    if react_api_dir not in sys.path:
        sys.path.insert(0, react_api_dir)
    
    # Change working directory to REACT_API_DIR so that relative paths resolve correctly.
    original_dir = os.getcwd()
    os.chdir(react_api_dir)
    
    # Import the ReACT extractor function from the react_extractor package.
    try:
        from react_extractor.extractor import ReACT_Extractor
    except ImportError as ie:
        os.chdir(original_dir)
        raise Exception("Failed to import ReACT_Extractor. Ensure the ReACT-API package is installed properly.") from ie

    # Define the parent JSON path (assumed to be in react_extractor folder inside REACT_API_DIR)
    parent_json = os.path.join(react_api_dir, "react_extractor", "react_set.json")
    if not os.path.exists(parent_json):
        os.chdir(original_dir)
        raise Exception(f"Parent JSON file not found at {parent_json}")
    
    with open(parent_json, 'r') as f:
        original_data = json.load(f)
    
    # Find the CSV file from the net-caches folder in the pex-forecaster repository.
    pex_generator_dir = os.getenv("PEX_GENERATOR_DIR")
    if not pex_generator_dir:
        os.chdir(original_dir)
        raise Exception("PEX_GENERATOR_DIR is not set in your .env file.")
    
    net_caches_dir = os.path.join(pex_generator_dir, "net-caches")
    if not os.path.exists(net_caches_dir):
        os.chdir(original_dir)
        raise Exception(f"net-caches folder not found in {pex_generator_dir}")
    
    csv_files = glob.glob(os.path.join(net_caches_dir, "*.csv"))
    if not csv_files:
        os.chdir(original_dir)
        raise Exception("No CSV file found in the net-caches folder.")
    
    # Assume the first CSV file is the one to use.
    feature_csv = csv_files[0]
    feature_data = pd.read_csv(feature_csv)
    
    # Set the month parameter (you can modify or pass this dynamically if needed)
    month_n = 9
    
    # Run the ReACT extractor.
    reacts = ReACT_Extractor(original_data, feature_data, month_n)
    
    # Restore original working directory.
    os.chdir(original_dir)
    
    return reacts
