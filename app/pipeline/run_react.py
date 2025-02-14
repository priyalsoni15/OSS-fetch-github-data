# flask-app/pipeline/run_react.py
import os
import sys
import glob
import json
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def format_reacts(reacts):
    """
    Transforms the raw ReACT objects into a structure for the front‑end.
    Each entry includes:
      - title: The ReACT_title.
      - importance: The raw numeric Importance.
      - priority: A computed label:
            * "critical" for Importance 5 or 6,
            * "high" for Importance 3 or 4,
            * "medium" for Importance 1 or 2,
            * "unknown" otherwise.
      - refs: A list of objects with text "[REF]" and the DOI link.
    The list is sorted in descending order by importance.
    """
    formatted = []
    for entry in reacts:
        title = entry.get("ReACT_title", "")
        importance = entry.get("Importance", 0)
        if importance >= 5:
            priority = "critical"
        elif 3 <= importance <= 4:
            priority = "high"
        elif 1 <= importance <= 2:
            priority = "medium"
        else:
            priority = "unknown"
        refs = []
        for article in entry.get("articles", []):
            doi = article.get("doi", "#")
            refs.append({"text": "[REF]", "link": doi})
        formatted.append({
            "title": title,
            "importance": importance,
            "priority": priority,
            "refs": refs
        })
    formatted.sort(key=lambda x: x["importance"], reverse=True)
    return formatted

def run_react():
    """
    Executes the ReACT extraction:
      1. Adds REACT_API_DIR to sys.path and changes directory.
      2. Loads the parent JSON (react_set.json) from the react_extractor package.
      3. Finds the CSV file from the pex‑forecaster net-caches folder.
      4. Calculates the total number of rows in the CSV (excluding header) to determine the month.
      5. Calls the ReACT_Extractor with that month to obtain raw results.
      6. Formats the raw results into the desired structure.
      7. Returns the formatted ReACT list.
    """
    # Ensure REACT_API_DIR is in sys.path
    react_api_dir = os.getenv("REACT_API_DIR")
    if not react_api_dir:
        raise Exception("REACT_API_DIR is not set in your .env file.")
    if react_api_dir not in sys.path:
        sys.path.insert(0, react_api_dir)
    
    # Change working directory to REACT_API_DIR for relative path resolution.
    original_dir = os.getcwd()
    os.chdir(react_api_dir)
    
    try:
        from react_extractor.extractor import ReACT_Extractor
    except ImportError as ie:
        os.chdir(original_dir)
        raise Exception("Failed to import ReACT_Extractor. Ensure the ReACT-API package is installed properly.") from ie

    # Load parent JSON file
    parent_json = os.path.join(react_api_dir, "react_extractor", "react_set.json")
    if not os.path.exists(parent_json):
        os.chdir(original_dir)
        raise Exception(f"Parent JSON file not found at {parent_json}")
    
    with open(parent_json, 'r') as f:
        original_data = json.load(f)
    
    # Locate the CSV file from the net-caches folder in the pex-forecaster repo.
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
    
    feature_csv = csv_files[0]
    feature_data = pd.read_csv(feature_csv)
    
    # Calculate the total number of rows in the CSV (each row represents a month).
    # This count (which is already excluding the header) will be used as the month value.
    total_months = len(feature_data)
    # For example, if there are 52 rows, total_months will be 52.
    
    # Call ReACT_Extractor with the calculated month.
    reacts = ReACT_Extractor(original_data, feature_data, total_months)
    
    # Restore original directory.
    os.chdir(original_dir)
    
    formatted_reacts = format_reacts(reacts)
    return formatted_reacts
