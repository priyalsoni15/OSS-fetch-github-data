# WIP: Do not run
import csv
from datetime import datetime
import json
import os
import random
import requests
import logging
import time
from itertools import cycle
from bs4 import BeautifulSoup
from pymongo import MongoClient
import urllib.parse

class Config:
    REPOSITORIES = [
        "https://github.com/apache/curator.git",
    ]
    
    APACHE_REPOSITORIES = [
        "https://lists.apache.org/list.html?dev@arrow.apache.org",
    ]
    
    DATA_DIR_STATIC = os.path.join(os.getcwd(), 'data')
    # Encode username and password
    username = urllib.parse.quote_plus('oss-nav')
    password = urllib.parse.quote_plus('navuser@98')
    MONGODB_DB_NAME = 'decal-db'
    MONGODB_URI = f'mongodb://{username}:{password}@localhost:27017/{MONGODB_DB_NAME}?retryWrites=true&w=majority'

    # Automatically collect all GITHUB_TOKEN_* variables and put them into a list
    @staticmethod
    def collect_github_tokens():
        tokens = []
        index = 1
        while True:
            token = os.environ.get(f'GITHUB_TOKEN_{index}')
            if token:
                tokens.append(token)
                index += 1
            else:
                break
        return tokens

# After the class definition, assign GITHUB_TOKENS
Config.GITHUB_TOKENS = Config.collect_github_tokens()
#print("Loaded GitHub Tokens:", Config.GITHUB_TOKENS)

# Configure logging
logging.basicConfig(level=logging.INFO)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize MongoDB client
mongo_client = MongoClient(Config.MONGODB_URI)
db = mongo_client[Config.MONGODB_DB_NAME]

#################### 2024 Code for Eclipse loading to DB ###################

# Helper function to load JSON file
def load_json_file(filepath):
    """Load JSON data from a file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load JSON file {filepath}: {e}")
        return None

# Helper function to load CSV file
def load_csv_file(filepath):
    """Load CSV data from a file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            return [row for row in reader]
    except Exception as e:
        logger.error(f"Failed to load CSV file {filepath}: {e}")
        return None
    
# Helper function to retrieve project name from collection
def get_project_info(project_id):
    """
    Retrieve project information from eclipse_projects collection based on project_id.
    Returns a dictionary with 'project_id' and 'project_name'.
    """
    if not isinstance(project_id, str):
        logger.warning(f"Invalid project_id type: {project_id}")
        return None
    
    # Normalize project_id by stripping spaces and converting to lowercase
    normalized_project_id = project_id.strip().lower()
    
    project = db.eclipse_projects.find_one({'project_id': normalized_project_id})
    if project:
        return {
            'project_id': project.get('project_id'),
            'project_name': project.get('project_name')
        }
    else:
        logger.warning(f"Project '{normalized_project_id}' not found in 'eclispe_projects' collection.")
        return None


# Load eclipse technical network month by month
def load_eclipse_tech_net():
    collection = db.eclipse_tech_net
    base_path = os.path.join(Config.DATA_DIR_STATIC, 'new', 'tech_net', 'new_commit')
    
    # Ensure the base_path exists
    if not os.path.exists(base_path):
        logger.error(f"Tech network data directory not found: {base_path}")
        return

    # Dictionary to hold all data before insertion
    tech_net_data = {}

    # Iterate over all project directories in the base_path
    for project_dir in os.listdir(base_path):
        project_path = os.path.join(base_path, project_dir)
        if not os.path.isdir(project_path):
            logger.debug(f"Skipping non-directory item: {project_dir}")
            continue
        logger.info(f"Processing project directory: {project_dir}")
        
        # Iterate over all JSON files in the project directory
        for filename in os.listdir(project_path):
            if not filename.endswith('.json'):
                logger.debug(f"Skipping non-JSON file: {filename}")
                continue

            logger.info(f"Processing file: {filename} in project '{project_dir}'")
            
            # Extract project_id and month number from filename (e.g., '4diac-examples_1.json' -> '4diac-examples', '1')
            parts = filename.rsplit('_', 1)
            if len(parts) != 2:
                logger.warning(f"Filename '{filename}' does not conform to expected pattern 'projectid_month.json'. Skipping.")
                continue
            project_id_part = parts[0].strip().lower()  # Normalize to lowercase and strip spaces
            month_part = parts[1].replace('.json', '').strip()
            if not month_part.isdigit():
                logger.warning(f"Month part '{month_part}' in filename '{filename}' is not a digit. Skipping.")
                continue
            month_number = month_part

            logger.debug(f"Extracted project_id: '{project_id_part}', month_number: '{month_number}'")

            # Retrieve project information
            project_info = get_project_info(project_id_part)
            if not project_info:
                logger.warning(f"Skipping file '{filename}' due to missing project information.")
                continue
            project_id = project_info['project_id']
            project_name = project_info['project_name']
            logger.debug(f"Found project_info: project_id='{project_id}', project_name='{project_name}'")

            filepath = os.path.join(project_path, filename)
            raw_data = load_json_file(filepath)
            if raw_data is None:
                logger.error(f"Skipping file '{filename}' due to failed JSON load.")
                continue

            # Initialize project entry if not already present
            if project_id not in tech_net_data:
                tech_net_data[project_id] = {
                    'project_id': project_id,
                    'project_name': project_name,
                    'months': {}
                }

            # Assign data to the corresponding month
            tech_net_data[project_id]['months'][month_number] = raw_data
            logger.info(f"Loaded data for project '{project_id}' month '{month_number}' from '{filename}'.")

    # Insert or update documents in MongoDB
    for project_id, data in tech_net_data.items():
        try:
            # Upsert the document: insert if it doesn't exist, update if it does
            result = collection.update_one(
                {'project_id': project_id},
                {'$set': data},
                upsert=True
            )
            if result.upserted_id:
                logger.info(f"Inserted new tech_net data for project_id '{project_id}'.")
            else:
                logger.info(f"Updated existing tech_net data for project_id '{project_id}'.")
        except Exception as e:
            logger.error(f"Failed to insert/update tech_net data for project_id '{project_id}': {e}")

    logger.info("Completed loading eclipse tech_net data into MongoDB.")
    
    
# For loading eclipse social network data from static files into MongoDB
def load_eclipse_social_net():
    """Load social_net data into MongoDB grouped by project and month."""
    collection = db.eclipse_social_net
    base_path = os.path.join(Config.DATA_DIR_STATIC, 'new', 'social_net', 'new_emails')
    
    # Ensure the base_path exists
    if not os.path.exists(base_path):
        logger.error(f"Social network data directory not found: {base_path}")
        return

    # Dictionary to hold all data before insertion
    social_net_data = {}

    # Iterate over all JSON files in the directory
    for filename in os.listdir(base_path):
        if filename.endswith('.json'):
            logger.info(f"Processing file: {filename}")
            # Extract project_id and month number from filename (e.g., 'abdera_1.json' -> 'abdera', '1')
            parts = filename.split('_')
            if len(parts) != 2:
                logger.warning(f"Filename '{filename}' does not conform to expected pattern 'projectid_month.json'. Skipping.")
                continue
            project_id_part = parts[0].strip().lower()  # Normalize to lowercase and strip spaces
            month_part = parts[1].replace('.json', '').strip()
            if not month_part.isdigit():
                logger.warning(f"Month part '{month_part}' in filename '{filename}' is not a digit. Skipping.")
                continue
            month_number = month_part

            logger.debug(f"Extracted project_id: '{project_id_part}', month_number: '{month_number}'")

            # Retrieve project information
            project_info = get_project_info(project_id_part)
            if not project_info:
                logger.warning(f"Skipping file '{filename}' due to missing project information.")
                continue
            project_id = project_info['project_id']
            project_name = project_info['project_name']
            logger.debug(f"Found project_info: project_id='{project_id}', project_name='{project_name}'")

            filepath = os.path.join(base_path, filename)
            raw_data = load_json_file(filepath)
            if raw_data is None:
                logger.error(f"Skipping file '{filename}' due to failed JSON load.")
                continue

            # Initialize project entry if not already present
            if project_id not in social_net_data:
                social_net_data[project_id] = {
                    'project_id': project_id,
                    'project_name': project_name,
                    'months': {}
                }

            # Assign data to the corresponding month
            social_net_data[project_id]['months'][month_number] = raw_data
            logger.info(f"Loaded data for project '{project_id}' month '{month_number}' from '{filename}'.")

    # Insert or update documents in MongoDB
    for project_id, data in social_net_data.items():
        try:
            # Upsert the document: insert if it doesn't exist, update if it does
            collection.update_one(
                {'project_id': project_id},
                {'$set': data},
                upsert=True
            )
            logger.info(f"Inserted/Updated social_net data for project_id '{project_id}'.")
        except Exception as e:
            logger.error(f"Failed to insert/update social_net data for project_id '{project_id}': {e}")

    logger.info("Completed loading social_net data into MongoDB.")

def main():
    # print(fetch_apache_repositories_from_github())
    # print(fetch_all_podlings())
    print(load_eclipse_tech_net())
    # print(load_eclipse_social_net())
    # print(process_monthly_ranges())
    # print(load_project_info())
    # print(process_project_info())
    # print(load_email_links_data())
    # print(load_commit_links_data())
    # print(load_grad_forecast())
    # print(load_commit_measure())
    # print(load_email_measure())
    
    logger.info("All data has been processed and loaded into MongoDB.")

main()