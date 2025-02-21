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
      - priority: A computed label ("critical", "high", "medium", "unknown").
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
    Executes the ReACT extraction for a single month (like passing --month N).
    - Loads the parent JSON (react_set.json).
    - Locates the CSV file from net-caches.
    - Uses total rows as the 'month' argument.
    - Calls the ReACT_Extractor.
    - Formats and returns the data for the frontend.
    """
    react_api_dir = os.getenv("REACT_API_DIR")
    if not react_api_dir:
        raise Exception("REACT_API_DIR is not set in your .env file.")
    if react_api_dir not in sys.path:
        sys.path.insert(0, react_api_dir)

    original_dir = os.getcwd()
    os.chdir(react_api_dir)

    try:
        # Import the extractor
        from react_extractor.extractor import ReACT_Extractor
    except ImportError as ie:
        os.chdir(original_dir)
        raise Exception("Failed to import ReACT_Extractor. Check ReACT-API install.") from ie

    parent_json = os.path.join(react_api_dir, "react_extractor", "react_set.json")
    if not os.path.exists(parent_json):
        os.chdir(original_dir)
        raise Exception(f"Parent JSON file not found at {parent_json}")

    with open(parent_json, 'r') as f:
        original_data = json.load(f)

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

    # Use total number of rows as 'month'
    total_months = len(feature_data)

    reacts = ReACT_Extractor(original_data, feature_data, total_months)
    os.chdir(original_dir)

    formatted_reacts = format_reacts(reacts)
    return formatted_reacts

def run_react_all():
    """
    Executes the ReACT extraction for ALL months (like passing --all).
    - Loads the parent JSON (react_set.json).
    - Locates the CSV file from net-caches.
    - Collects unique 'month' values and calls ReACT_Extractor for each month.
    - Formats each month's results.
    - Returns a dict keyed by month, each containing the front-end formatted data.
    """
    react_api_dir = os.getenv("REACT_API_DIR")
    if not react_api_dir:
        raise Exception("REACT_API_DIR is not set in your .env file.")
    if react_api_dir not in sys.path:
        sys.path.insert(0, react_api_dir)

    original_dir = os.getcwd()
    os.chdir(react_api_dir)

    try:
        # Import the extractor (with the updated logic for optional multi-month use)
        from react_extractor.extractor import ReACT_Extractor
    except ImportError as ie:
        os.chdir(original_dir)
        raise Exception("Failed to import ReACT_Extractor. Check ReACT-API install.") from ie

    parent_json = os.path.join(react_api_dir, "react_extractor", "react_set.json")
    if not os.path.exists(parent_json):
        os.chdir(original_dir)
        raise Exception(f"Parent JSON file not found at {parent_json}")

    with open(parent_json, 'r') as f:
        original_data = json.load(f)

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

    # Get all unique months
    all_months = sorted(feature_data['month'].unique())

    # Dictionary keyed by month -> formatted ReACT data
    all_results = {}

    for m in all_months:
        # If your ReACT_Extractor has a 'write_output' argument, pass write_output=False
        # to avoid overwriting the extracted_react.json file multiple times.
        # Otherwise, just call it normally if you only have the single-month version.
        raw_reacts_for_month = ReACT_Extractor(
            original_data,
            feature_data,
            int(m),  # ensure int in case it's numpy.int64
        )
        # Format the extracted results for the front-end
        all_results[int(m)] = format_reacts(raw_reacts_for_month)

    os.chdir(original_dir)
    return all_results
