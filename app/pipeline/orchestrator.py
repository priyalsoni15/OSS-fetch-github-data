import os
import glob
import json
import logging
import concurrent.futures
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient
from .update_pex import update_pex_generator
from .rust_runner import run_rust_code
from .run_pex import run_forecast  # Still imported so forecast can run if needed
from .store_commit_issues import process_project_data  # Import MongoDB processing
from .github_metadata import get_github_metadata

load_dotenv()

executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)  # Ensures non-blocking execution

# ✅ **Correct MongoDB Connection**
MONGODB_URI = os.environ.get("MONGODB_URI")
# Change default from "testdb" to "decal-db" so that the correct DB is used
db_name = os.environ.get("MONGO_DB_NAME", "decal-db")  # Use the correct DB name
client = MongoClient(MONGODB_URI)
db = client[db_name]  # Explicitly select database

def extract_project_name(git_link):
    """Extract the project name from a git URL."""
    if git_link.endswith(".git"):
        git_link = git_link[:-4]
    return git_link.rstrip("/").split("/")[-1]

def generate_project_id(project_name):
    """Generate a project_id by removing non-alphanumeric characters and lowercasing."""
    return ''.join(c for c in project_name if c.isalnum()).lower()

def fetch_project_data_from_db(project_id):
    """Retrieve processed data from MongoDB for the given project_id.
       Returns sanitized keys (commit_data and issue_data) so that internal collection names are hidden.
    """
    result = {}

    # Note: using projection {"_id": 0} to hide internal MongoDB identifiers
    commit_data = db.local_commit_links.find_one({"project_id": project_id}, {"_id": 0})
    issue_data = db.local_issue_links.find_one({"project_id": project_id}, {"_id": 0})

    if commit_data:
        logging.info(f"Found commit links data in DB for project_id='{project_id}'")
        result["commit_data"] = commit_data
    if issue_data:
        logging.info(f"Found issue links data in DB for project_id='{project_id}'")
        result["issue_data"] = issue_data

    return result

def run_pipeline(git_link, tasks="ALL", month_range="0,-1"):
    """Orchestrates the entire pipeline and returns a structured JSON result."""
    result_summary = {}

    # Store the git link immediately.
    result_summary["git_link"] = git_link

    # Extract project_name and compute project_id early
    project_name = extract_project_name(git_link)
    project_id = generate_project_id(project_name)
    
    # --- Step 0: Fetch GitHub Repository Metadata ---
    try:
        metadata = get_github_metadata(git_link)
          # Add it to the final JSON response
    except Exception as e:
        metadata = {"error": str(e)}
    result_summary["metadata"] = metadata
    
    # --- Step 1: Update and ensure PEX‑Forecaster ---
    # try:
    #     pex_update = update_pex_generator()
    # except Exception as e:
    #     pex_update = {"error": str(e)}
    # result_summary["pex_update"] = pex_update

    # --- Step 2: Run the Rust scraper ---
    try:
        rust_result = run_rust_code(git_link)
    except Exception as e:
        rust_result = {"error": str(e)}
    result_summary["rust_result"] = rust_result

    # --- Verify output folder exists ---
    output_dir = rust_result.get("output_dir")
    if not output_dir or not os.path.exists(output_dir):
        result_summary["error"] = "Output directory not found after running OSS‑Scraper."
        return result_summary

    output_dir = os.path.abspath(output_dir)
    logging.info(f"Output directory: {output_dir}")
    try:
        files_in_output = os.listdir(output_dir)
        logging.info(f"Files in output directory: {files_in_output}")
    except Exception as e:
        logging.error(f"Error listing files in output directory: {e}")

    # ✅ **Blocking MongoDB Processing (Ensures Completion)**
    logging.info("Starting MongoDB processing...")
    # Pass project_id and project_name so the CSV processing uses a consistent identifier
    process_project_data(output_dir, project_id, project_name)  # Ensures data is stored before fetching

    # --- Step 3: Locate CSV files for social and technical networks ---
    social_csvs = glob.glob(os.path.join(output_dir, "*_issues.csv"))
    tech_csvs = glob.glob(os.path.join(output_dir, "*-commit-file-dev.csv"))
    if not social_csvs:
        result_summary["error"] = "No social network CSV (_issues.csv) found."
        return result_summary
    if not tech_csvs:
        result_summary["error"] = "No technical network CSV found."
        return result_summary

    social_csv = os.path.abspath(social_csvs[0])
    tech_csv = os.path.abspath(tech_csvs[0])
    
    # --- Step 4: Run pex‑forecaster forecast (run for side effects only) ---
    try:
        pex_result = run_forecast(tech_csv, social_csv, project_name, tasks, month_range)
        logging.info(f"pex-forecaster result {pex_result}")
    except Exception as e:
        logging.error("Forecast processing error: " + str(e))

    # ✅ Fetch Data from MongoDB and Add to Response (After Processing Completes)
    mongo_data = fetch_project_data_from_db(project_id)
    result_summary.update(mongo_data)
    
    # --- Step 4 - (Cache collection) Move CSV files to archive folder ---
    try:
        parent_dir = os.path.dirname(output_dir)
        archive_dir = os.path.join(parent_dir, "archive")

        if not os.path.exists(archive_dir):
            os.makedirs(archive_dir)

        # Move files
        social_csv_dest = os.path.join(archive_dir, os.path.basename(social_csv))
        tech_csv_dest = os.path.join(archive_dir, os.path.basename(tech_csv))
        
        os.rename(social_csv, social_csv_dest)
        os.rename(tech_csv, tech_csv_dest)

        logging.info(f"Moved {social_csv} to {social_csv_dest}")
        logging.info(f"Moved {tech_csv} to {tech_csv_dest}")
    except Exception as e:
        logging.error(f"Error moving CSV files to archive: {e}")

    

    # --- Step 5: Run ReACT extractor ---
    # try:
    #     from .run_react import run_react
    #     react_result = run_react()
    #     result_summary["react"] = react_result
    # except Exception as e:
    #     logging.error("ReACT extractor failed: " + str(e))
    #     result_summary["react"] = {"error": str(e)}
    
    # --- Step 5: Run ReACT extractor (all months)---
    try:
        from .run_react import run_react_all
        react_result = run_react_all()
        result_summary["react"] = react_result
        logging.info(f"ReACT result obtained.")
    except Exception as e:
        logging.error("ReACT extractor failed: " + str(e))
        result_summary["react"] = {"error": str(e)}

    # --- Step 6: Process net-vis JSON file ---
    try:
        pex_generator_dir = os.getenv("PEX_GENERATOR_DIR")
        net_vis_file = os.path.join(pex_generator_dir, "net-vis", f"{project_name}.json")
        if os.path.exists(net_vis_file):
            with open(net_vis_file, 'r') as f:
                net_vis_data = json.load(f)
            tech_net = net_vis_data.get("tech", {})
            social_net = net_vis_data.get("social", {})
            tech_net["project_name"] = project_name
            tech_net["project_id"] = project_id
            social_net["project_name"] = project_name
            social_net["project_id"] = project_id
            result_summary["tech_net"] = tech_net
            result_summary["social_net"] = social_net
        else:
            result_summary["tech_net"] = {"error": f"File {net_vis_file} not found"}
            result_summary["social_net"] = {"error": f"File {net_vis_file} not found"}
        logging.info(f"Process net-vis JSON file tech_net result: {result_summary['tech_net']}")
        logging.info(f"Process net-vis JSON file social_net result: {result_summary['social_net']}")
    except Exception as e:
        logging.error("Process net-vis JSON file failed: " + str(e))
        result_summary["tech_net"] = {"error": str(e)}
        result_summary["social_net"] = {"error": str(e)}

    # --- Step 7: Read forecasts JSON file ---
    try:
        forecasts_file = os.path.join(pex_generator_dir, "forecasts", f"{project_name}.json")
        if os.path.exists(forecasts_file):
            with open(forecasts_file, 'r') as f:
                forecasts_data = json.load(f)
            result_summary["forecast_json"] = forecasts_data
        else:
            result_summary["forecast_json"] = {"error": f"File {forecasts_file} not found"}
        logging.info(f"Read forecasts JSON file result: {result_summary['forecast_json']}")
    except Exception as e:
        logging.error("Read forecasts JSON file: " + str(e))
        result_summary["forecast_json"] = {"error": str(e)}

    return result_summary
