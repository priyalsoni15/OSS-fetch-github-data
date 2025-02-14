# flask-app/pipeline/orchestrator.py
import os
import glob
import json
import logging
from dotenv import load_dotenv
from .update_pex import update_pex_generator
from .rust_runner import run_rust_code
from .run_pex import run_forecast  # Still imported so forecast can run if needed

load_dotenv()

def extract_project_name(git_link):
    """Extract the project name from a git URL."""
    if git_link.endswith(".git"):
        git_link = git_link[:-4]
    return git_link.rstrip("/").split("/")[-1]

def generate_project_id(project_name):
    """Generate a project_id by removing non-alphanumeric characters and lowercasing."""
    return ''.join(c for c in project_name if c.isalnum()).lower()

def run_pipeline(git_link, tasks="ALL", month_range="0,-1"):
    """Orchestrates the entire pipeline and returns a structured JSON result."""
    result_summary = {}
    
    # Store the git link immediately.
    result_summary["git_link"] = git_link

    # --- Step 1: Update and ensure PEX‑Forecaster ---
    try:
        pex_update = update_pex_generator()
    except Exception as e:
        pex_update = {"error": str(e)}
    result_summary["pex_update"] = pex_update

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
    result_summary["social_csv"] = social_csv
    result_summary["tech_csv"] = tech_csv

    # --- Step 4: Run pex‑forecaster forecast (run for side effects only) ---
    try:
        project = extract_project_name(git_link)
        # Run forecast but do not store its result.
        _ = run_forecast(tech_csv, social_csv, project, tasks, month_range)
    except Exception as e:
        logging.error("Forecast processing error: " + str(e))
        # Do not add forecast data to the result.
    
    # --- Step 5: Run ReACT extractor ---
    try:
        from .run_react import run_react
        react_result = run_react()
        result_summary["react"] = react_result
    except Exception as e:
        logging.error("ReACT extractor failed: " + str(e))
        result_summary["react"] = {"error": str(e)}

    # --- Step 6: Process net-vis JSON file ---
    try:
        pex_generator_dir = os.getenv("PEX_GENERATOR_DIR")
        project_name = extract_project_name(git_link)
        project_id = generate_project_id(project_name)
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
    except Exception as e:
        result_summary["tech_net"] = {"error": str(e)}
        result_summary["social_net"] = {"error": str(e)}

    # --- Step 7: Read forecasts JSON file ---
    try:
        pex_generator_dir = os.getenv("PEX_GENERATOR_DIR")
        project_name = extract_project_name(git_link)
        forecasts_file = os.path.join(pex_generator_dir, "forecasts", f"{project_name}.json")
        if os.path.exists(forecasts_file):
            with open(forecasts_file, 'r') as f:
                forecasts_data = json.load(f)
            result_summary["forecast_json"] = forecasts_data
        else:
            result_summary["forecast_json"] = {"error": f"File {forecasts_file} not found"}
    except Exception as e:
        result_summary["forecast_json"] = {"error": str(e)}

    return result_summary
