import os
import glob
import logging
from .update_pex import update_pex_generator
from .rust_runner import run_rust_code
from .run_pex import run_forecast

def extract_project_name(git_link):
    """Extracts the project name from a git URL (e.g. 'https://github.com/apache/hunter.git' → 'hunter')."""
    if git_link.endswith(".git"):
        git_link = git_link[:-4]
    return git_link.rstrip("/").split("/")[-1]

def run_pipeline(git_link, tasks="ALL", month_range="0,-1"):
    """Orchestrates the entire pipeline."""
    result_summary = {}

    # Ensure and update PEX‑Forecaster.
    pex_update = update_pex_generator()
    result_summary["pex_update"] = pex_update

    # Run the Rust scraper.
    rust_result = run_rust_code(git_link)
    result_summary["rust_result"] = rust_result

    # Check output folder from OSS‑Scraper
    output_dir = rust_result.get("output_dir")
    if not output_dir or not os.path.exists(output_dir):
        result_summary["error"] = "Output directory not found after running OSS‑Scraper."
        return result_summary
    
    # Normalize and log output directory
    output_dir = os.path.abspath(output_dir)
    logging.info(f"Output directory: {output_dir}")
    try:
        files_in_output = os.listdir(output_dir)
        logging.info(f"Files in output directory: {files_in_output}")
    except Exception as e:
        logging.error(f"Error listing files in output directory: {e}")

    # Find CSV files for social and technical networks.
    social_csvs = glob.glob(os.path.join(output_dir, "*_issues.csv"))
    tech_csvs = glob.glob(os.path.join(output_dir, "*-commit-file-dev.csv"))
    
    if not tech_csvs:
        logging.info("No technical CSV found with pattern '*-commit-file-dev.csv'.")
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

    project = extract_project_name(git_link)
    forecast_result = run_forecast(tech_csv, social_csv, project, tasks, month_range)
    result_summary["forecast_result"] = forecast_result

    # --- New: Run the ReACT extractor ---
    try:
        from .run_react import run_react
        react_result = run_react()
        result_summary["react_result"] = react_result
    except Exception as e:
        logging.error("ReACT extractor failed: " + str(e))
        result_summary["react_result"] = {"error": str(e)}

    return result_summary
