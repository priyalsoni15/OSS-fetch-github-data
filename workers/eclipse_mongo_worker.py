import csv
import json
import os
import logging
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
    
    project = db.eclipse_project_info.find_one({'project_id': normalized_project_id})
    if project:
        return {
            'project_id': project.get('project_id'),
            'project_name': project.get('project_name')
        }
    else:
        logger.warning(f"Project '{normalized_project_id}' not found in 'eclispe_projects' collection.")
        return None

# [Fetch the Eclipse project details]
def process_eclipse_project_info():
    project_info_dir = os.path.join('data', 'new')
    about_data_dir = os.path.join(project_info_dir, 'new_about_data')
    month_interval_dir = os.path.join(project_info_dir, 'new_month_intervals')
    project_names_file = os.path.join(project_info_dir, 'project_names.json')

    projects = {}

    # Load project name mappings from project_names.json
    # Structure example:
    # {
    #   "Modeling": {
    #       "EMFStore": ["emfstore-website", "org.eclipse.emf.emfstore.core"],
    #       "Epsilon": ["epsilon-website", "epsilon"],
    #       ...
    #   },
    #   "AnotherCategory": {...}
    # }
    #
    # We will flatten this into a simple dict:
    # {
    #    "EMFStore": ["emfstore-website", "org.eclipse.emf.emfstore.core"],
    #    "Epsilon": ["epsilon-website", "epsilon"],
    #    ...
    # }
    with open(project_names_file, 'r') as f:
        project_names_data = json.load(f)

    project_name_mapping = {}
    for category, proj_map in project_names_data.items():
        for project_name, dependencies in proj_map.items():
            project_name_mapping[project_name] = dependencies

    # Process about_data
    # Each about_data file is expected to match a primary project name
    for filename in os.listdir(about_data_dir):
        if filename.endswith('.json'):
            project_name = filename.replace('.json', '')
            project_id = project_name.lower().replace(' ', '').replace('-', '').replace('_', '')
            with open(os.path.join(about_data_dir, filename), 'r') as f:
                about_data = json.load(f)

                # Add the project with known fields
                projects[project_name] = {
                    "project_id": project_id,
                    "project_name": project_name,
                    "project_url": about_data.get("project_url"),
                    "status": about_data.get("status"),
                    "tech": about_data.get("tech"),
                    "releases": about_data.get("releases", []),
                    # We'll add display after all intervals are processed
                }

                # If this project name appears in the mapping, add the dependencies
                if project_name in project_name_mapping:
                    projects[project_name]["dependencies"] = project_name_mapping[project_name]
                else:
                    projects[project_name]["dependencies"] = []

    # Process month intervals
    # Each month_interval file name might either be the project_name itself
    # or one of the dependencies of a known project.
    for filename in os.listdir(month_interval_dir):
        if filename.endswith('.json'):
            month_interval_project_name = filename.replace('.json', '')

            # Resolve the project name:
            # Check if it's a main project name first
            if month_interval_project_name in project_name_mapping:
                resolved_project_name = month_interval_project_name
            else:
                # Otherwise, search for which main project has this as a dependency
                resolved_project_name = None
                for main_name, dependencies in project_name_mapping.items():
                    if month_interval_project_name in dependencies:
                        resolved_project_name = main_name
                        break

            # If still not found, just use the raw month_interval_project_name
            # (This means no mapping was found, it could be a standalone project)
            if not resolved_project_name:
                resolved_project_name = month_interval_project_name

            project_id = resolved_project_name.lower().replace(' ', '').replace('-', '').replace('_', '')

            with open(os.path.join(month_interval_dir, filename), 'r') as f:
                month_interval_data = json.load(f)

                if resolved_project_name in projects:
                    # We found an existing project (from about_data)
                    projects[resolved_project_name]['month_intervals'] = month_interval_data
                    projects[resolved_project_name]['display'] = True
                else:
                    # No about_data found for this project, create a minimal entry
                    # Set display = true since we have intervals
                    dependencies = project_name_mapping.get(resolved_project_name, [])
                    projects[resolved_project_name] = {
                        "project_id": project_id,
                        "project_name": resolved_project_name,
                        "month_intervals": month_interval_data,
                        "dependencies": dependencies,
                        "display": True,
                        # Minimal fields, as about_data does not exist for this project
                    }

    # For any project without month_intervals, set display = false
    for p_name, p_data in projects.items():
        if 'month_intervals' not in p_data:
            p_data['display'] = False

    # Insert into MongoDB
    # Just insert the project docs, do not insert the mapping dictionary since we didn't store it in `projects`.
    # (We've only stored final projects in `projects`.)
    documents_to_insert = list(projects.values())

    if documents_to_insert:
        try:
            db.eclipse_project_info.insert_many(documents_to_insert)
            logger.info("Eclipse project info data saved to MongoDB collection 'eclipse_project_info'.")
        except Exception as e:
            logger.error(f"Error saving Eclipse project info to MongoDB: {e}")


# Process and load Eclipse technical network data
def load_eclipse_tech_net():
    collection = db.eclipse_tech_net
    base_path = os.path.join('data', 'new', 'tech_net','new_commit')

    # Ensure the base_path exists
    if not os.path.exists(base_path):
        logger.error(f"Tech network data directory not found: {base_path}")
        return

    tech_net_data = {}

    # Iterate over project folders in the base_path
    for project_folder in os.listdir(base_path):
        project_folder_path = os.path.join(base_path, project_folder)
        if not os.path.isdir(project_folder_path):
            continue

        project_id = project_folder.lower().replace(' ', '').replace('-', '').replace('_', '')
        logger.info(f"Processing project folder: {project_folder}")

        # Iterate over JSON files inside the project folder
        for filename in os.listdir(project_folder_path):
            if filename.endswith('.json'):
                logger.info(f"Processing file: {filename}")
                parts = filename.split('_')
                if len(parts) != 2:
                    logger.warning(f"Filename '{filename}' does not conform to expected pattern 'projectid_month.json'. Skipping.")
                    continue
                month_part = parts[1].replace('.json', '').strip()
                if not month_part.isdigit():
                    logger.warning(f"Month part '{month_part}' in filename '{filename}' is not a digit. Skipping.")
                    continue

                month_number = month_part
                filepath = os.path.join(project_folder_path, filename)
                raw_data = load_json_file(filepath)
                if raw_data is None:
                    logger.error(f"Skipping file '{filename}' due to failed JSON load.")
                    continue

                if project_id not in tech_net_data:
                    tech_net_data[project_id] = {
                        'project_id': project_id,
                        'project_name': project_folder,
                        'months': {}
                    }

                tech_net_data[project_id]['months'][month_number] = raw_data
                logger.info(f"Loaded data for project '{project_id}' month '{month_number}' from '{filename}'.")

    # Insert or update documents in MongoDB
    for project_id, data in tech_net_data.items():
        try:
            collection.update_one(
                {'project_id': project_id},
                {'$set': data},
                upsert=True
            )
            logger.info(f"Inserted/Updated tech_net data for project_id '{project_id}'.")
        except Exception as e:
            logger.error(f"Failed to insert/update tech_net data for project_id '{project_id}': {e}")

    logger.info("Completed loading tech_net data into MongoDB.")


# Process and load Eclipse social network data
def load_eclipse_social_net():
    collection = db.eclipse_social_net
    base_path = os.path.join('data', 'new', 'social_net','new_issues')

    # Ensure the base_path exists
    if not os.path.exists(base_path):
        logger.error(f"Social network data directory not found: {base_path}")
        return

    social_net_data = {}

    # Iterate over project folders in the base_path
    for project_folder in os.listdir(base_path):
        project_folder_path = os.path.join(base_path, project_folder)
        if not os.path.isdir(project_folder_path):
            continue

        project_id = project_folder.lower().replace(' ', '').replace('-', '').replace('_', '')
        logger.info(f"Processing project folder: {project_folder}")

        # Iterate over JSON files inside the project folder
        for filename in os.listdir(project_folder_path):
            if filename.endswith('.json'):
                logger.info(f"Processing file: {filename}")
                parts = filename.split('_')
                if len(parts) != 2:
                    logger.warning(f"Filename '{filename}' does not conform to expected pattern 'projectid_month.json'. Skipping.")
                    continue
                month_part = parts[1].replace('.json', '').strip()
                if not month_part.isdigit():
                    logger.warning(f"Month part '{month_part}' in filename '{filename}' is not a digit. Skipping.")
                    continue

                month_number = month_part
                filepath = os.path.join(project_folder_path, filename)
                raw_data = load_json_file(filepath)
                if raw_data is None:
                    logger.error(f"Skipping file '{filename}' due to failed JSON load.")
                    continue

                if project_id not in social_net_data:
                    social_net_data[project_id] = {
                        'project_id': project_id,
                        'project_name': project_folder,
                        'months': {}
                    }

                social_net_data[project_id]['months'][month_number] = raw_data
                logger.info(f"Loaded data for project '{project_id}' month '{month_number}' from '{filename}'.")

    # Insert or update documents in MongoDB
    for project_id, data in social_net_data.items():
        try:
            collection.update_one(
                {'project_id': project_id},
                {'$set': data},
                upsert=True
            )
            logger.info(f"Inserted/Updated social_net data for project_id '{project_id}'.")
        except Exception as e:
            logger.error(f"Failed to insert/update social_net data for project_id '{project_id}': {e}")

    logger.info("Completed loading tech_net data into MongoDB.")


# For loading the graduation forecast, i.e. the health of the projects:
def load_eclipse_grad_forecast():
    """Load grad_forecast data into MongoDB grouped by project and month."""
    collection = db.eclipse_grad_forecast
    base_path = os.path.join('data', 'new', 'new_forecast')

    # Ensure the base_path exists
    if not os.path.exists(base_path):
        logger.error(f"Grad forecast data directory not found: {base_path}")
        return

    # Dictionary to hold all data before insertion
    grad_forecast_data = {}

    # Iterate over all CSV files in the directory
    for filename in os.listdir(base_path):
        if filename.endswith('_f_data.csv'):
            logger.info(f"Processing file: {filename}")
            # Extract project_id from filename (e.g., 'abdera_f_data.csv' -> 'abdera')
            project_id = filename.split('_f_data.csv')[0].replace(' ', '').replace('-','').lower()
            logger.debug(f"Extracted project_id: '{project_id}'")

            # Retrieve project information
            logger.debug(f"Found project_info: project_id='{project_id}', project_name='{project_id}'")

            filepath = os.path.join(base_path, filename)
            raw_data = load_csv_file(filepath)
            if raw_data is None:
                logger.error(f"Skipping file '{filename}' due to failed CSV load.")
                continue

            # Initialize project entry if not already present
            if project_id not in grad_forecast_data:
                grad_forecast_data[project_id] = {
                    'project_id': project_id,
                    'forecast': {}
                }

            # Assign data to the corresponding month
            for row in raw_data:
                month = row.get('month')
                close = row.get('close')
                if not month or not close:
                    logger.warning(f"Missing 'date' or 'close' in file '{filename}', row: {row}. Skipping row.")
                    continue
                if not month.isdigit():
                    logger.warning(f"Invalid 'date' value '{month}' in file '{filename}'. Skipping row.")
                    continue
                try:
                    month_int = int(month)
                    close_float = float(close)
                except ValueError:
                    logger.warning(f"Invalid data types in file '{filename}', row: {row}. Skipping row.")
                    continue

                grad_forecast_data[project_id]['forecast'][str(month_int)] = {
                    'month': month_int,
                    'close': close_float
                }
                logger.debug(f"Added forecast for project '{project_id}', month '{month_int}': {close_float}")

            logger.info(f"Loaded forecast data for project '{project_id}' from '{filename}'.")

    # Insert or update documents in MongoDB
    for project_id, data in grad_forecast_data.items():
        try:
            # Upsert the document: insert if it doesn't exist, update if it does
            collection.update_one(
                {'project_id': project_id},
                {'$set': data},
                upsert=True
            )
            logger.info(f"Inserted/Updated grad_forecast data for project_id '{project_id}'.")
        except Exception as e:
            logger.error(f"Failed to insert/update grad_forecast data for project_id '{project_id}': {e}")

    logger.info("Completed loading grad_forecast data into MongoDB.")

# Load data for email_measure (data below the socio-tech net for each month)
def load_eclipse_email_measure():
    collection = db.eclipse_email_measure
    base_path = os.path.join('data', 'new', 'emails_measure')

    # Ensure the base_path exists
    if not os.path.exists(base_path):
        logger.error(f"Emails measure data directory not found: {base_path}")
        return

    # Dictionary to hold all data before insertion
    email_measure_data = {}
    
    for project_folder in os.listdir(base_path):
        project_folder_path = os.path.join(base_path, project_folder)
        if not os.path.isdir(project_folder_path):
            continue

        project_id = project_folder.lower().replace(' ', '').replace('-', '').replace('_', '')
        logger.info(f"Processing project folder: {project_folder}")

        # Iterate over JSON files inside the project folder
        for filename in os.listdir(project_folder_path):
            if filename.endswith('.json'):
                logger.info(f"Processing file: {filename}")
                parts = filename.split('_')
                if len(parts) != 2:
                    logger.warning(f"Filename '{filename}' does not conform to expected pattern 'projectid_month.json'. Skipping.")
                    continue
                month_part = parts[1].replace('.json', '').strip()
                if not month_part.isdigit():
                    logger.warning(f"Month part '{month_part}' in filename '{filename}' is not a digit. Skipping.")
                    continue

                month_number = month_part
                filepath = os.path.join(project_folder_path, filename)
                raw_data = load_json_file(filepath)
                if raw_data is None:
                    logger.error(f"Skipping file '{filename}' due to failed JSON load.")
                    continue

                # Initialize project entry if not already present
                if project_id not in email_measure_data:
                    email_measure_data[project_id] = {
                        'project_id': project_id,
                        'months': {}
                    }

            # Assign data to the corresponding month
            email_measure_data[project_id]['months'][month_number] = raw_data
            logger.info(f"Loaded data for project '{project_id}' month '{month_number}' from '{filename}'.")

    # Insert or update documents in MongoDB
    for project_id, data in email_measure_data.items():
        try:
            # Upsert the document: insert if it doesn't exist, update if it does
            collection.update_one(
                {'project_id': project_id},
                {'$set': data},
                upsert=True
            )
            logger.info(f"Inserted/Updated email measure data for project_id '{project_id}'.")
        except Exception as e:
            logger.error(f"Failed to insert/update email measure data for project_id '{project_id}': {e}")

    logger.info("Completed loading email_measure data into MongoDB.")


# Load data for email_measure (data below the socio-tech net for each month)
def load_eclipse_commit_measure():
    collection = db.eclipse_commit_measure
    base_path = os.path.join('data', 'new', 'commits_measure')

    # Ensure the base_path exists
    if not os.path.exists(base_path):
        logger.error(f"Commits measure directory not found: {base_path}")
        return

    # Dictionary to hold all data before insertion
    commit_measure_data = {}
    
    for project_folder in os.listdir(base_path):
        project_folder_path = os.path.join(base_path, project_folder)
        if not os.path.isdir(project_folder_path):
            continue

        project_id = project_folder.lower().replace(' ', '').replace('-', '').replace('_', '')
        logger.info(f"Processing project folder: {project_folder}")

        # Iterate over JSON files inside the project folder
        for filename in os.listdir(project_folder_path):
            if filename.endswith('.json'):
                logger.info(f"Processing file: {filename}")
                parts = filename.split('_')
                if len(parts) != 2:
                    logger.warning(f"Filename '{filename}' does not conform to expected pattern 'projectid_month.json'. Skipping.")
                    continue
                month_part = parts[1].replace('.json', '').strip()
                if not month_part.isdigit():
                    logger.warning(f"Month part '{month_part}' in filename '{filename}' is not a digit. Skipping.")
                    continue

                month_number = month_part
                filepath = os.path.join(project_folder_path, filename)
                raw_data = load_json_file(filepath)
                if raw_data is None:
                    logger.error(f"Skipping file '{filename}' due to failed JSON load.")
                    continue

                # Initialize project entry if not already present
                if project_id not in commit_measure_data:
                    commit_measure_data[project_id] = {
                        'project_id': project_id,
                        'months': {}
                    }

            # Assign data to the corresponding month
            commit_measure_data[project_id]['months'][month_number] = raw_data
            logger.info(f"Loaded data for project '{project_id}' month '{month_number}' from '{filename}'.")

    # Insert or update documents in MongoDB
    for project_id, data in commit_measure_data.items():
        try:
            # Upsert the document: insert if it doesn't exist, update if it does
            collection.update_one(
                {'project_id': project_id},
                {'$set': data},
                upsert=True
            )
            logger.info(f"Inserted/Updated commit measure data for project_id '{project_id}'.")
        except Exception as e:
            logger.error(f"Failed to insert/update commit measure data for project_id '{project_id}': {e}")

    logger.info("Completed loading commit_measure data into MongoDB.")


# Load data for email_measure (data below the socio-tech net for each month)
def load_eclipse_issues_measure():
    collection = db.eclipse_issue_measure
    base_path = os.path.join('data', 'new', 'issues_measure')

    # Ensure the base_path exists
    if not os.path.exists(base_path):
        logger.error(f"Commits measure directory not found: {base_path}")
        return

    # Dictionary to hold all data before insertion
    issues_measure_data = {}
    
    for project_folder in os.listdir(base_path):
        project_folder_path = os.path.join(base_path, project_folder)
        if not os.path.isdir(project_folder_path):
            continue

        project_id = project_folder.lower().replace(' ', '').replace('-', '').replace('_', '')
        logger.info(f"Processing project folder: {project_folder}")

        # Iterate over JSON files inside the project folder
        for filename in os.listdir(project_folder_path):
            if filename.endswith('.json'):
                logger.info(f"Processing file: {filename}")
                parts = filename.split('_')
                if len(parts) != 2:
                    logger.warning(f"Filename '{filename}' does not conform to expected pattern 'projectid_month.json'. Skipping.")
                    continue
                month_part = parts[1].replace('.json', '').strip()
                if not month_part.isdigit():
                    logger.warning(f"Month part '{month_part}' in filename '{filename}' is not a digit. Skipping.")
                    continue

                month_number = month_part
                filepath = os.path.join(project_folder_path, filename)
                raw_data = load_json_file(filepath)
                if raw_data is None:
                    logger.error(f"Skipping file '{filename}' due to failed JSON load.")
                    continue

                # Initialize project entry if not already present
                if project_id not in issues_measure_data:
                    issues_measure_data[project_id] = {
                        'project_id': project_id,
                        'months': {}
                    }

            # Assign data to the corresponding month
            issues_measure_data[project_id]['months'][month_number] = raw_data
            logger.info(f"Loaded data for project '{project_id}' month '{month_number}' from '{filename}'.")

    # Insert or update documents in MongoDB
    for project_id, data in issues_measure_data.items():
        try:
            # Upsert the document: insert if it doesn't exist, update if it does
            collection.update_one(
                {'project_id': project_id},
                {'$set': data},
                upsert=True
            )
            logger.info(f"Inserted/Updated issues measure data for project_id '{project_id}'.")
        except Exception as e:
            logger.error(f"Failed to insert/update issues measure data for project_id '{project_id}': {e}")

    logger.info("Completed loading issues_measure data into MongoDB.")


# [Query - There are no emails in Eclipse - why is this loaded? )Load data for email links
def load_eclipse_email_links_data():
    """Load commit_links data into MongoDB grouped by project and month."""
    collection = db.eclipse_email_links  
    base_path = os.path.join('data', 'new', 'new_emails')

    # Ensure the base_path exists
    if not os.path.exists(base_path):
        logger.error(f"Email links data directory not found: {base_path}")
        return

    # Dictionary to hold all data before insertion
    email_links_data = {}

    # Iterate over each project directory in commit_links
    for project_dir in os.listdir(base_path):
        project_path = os.path.join(base_path, project_dir)
        if not os.path.isdir(project_path):
            logger.warning(f"Skipping non-directory item: {project_dir}")
            continue
        
        project_id = project_dir.strip().lower()
        logger.info(f"Processing project: {project_id}")

        # Retrieve project information
        project_info = get_project_info(project_id)
        if not project_info:
            logger.warning(f"Skipping project '{project_id}' due to missing project information.")
            continue
        project_id_correct = project_info['project_id']
        project_name = project_info['project_name']
        logger.debug(f"Found project_info: project_id='{project_id_correct}', project_name='{project_name}'")

        # Initialize project entry if not already present
        if project_id_correct not in email_links_data:
            email_links_data[project_id_correct] = {
                'project_id': project_id_correct,
                'project_name': project_name,
                'months': {}
            }

        # Iterate over each month directory within the project
        for month_dir in os.listdir(project_path):
            month_path = os.path.join(project_path, month_dir)
            if not os.path.isdir(month_path):
                logger.warning(f"Skipping non-directory item: {month_dir} in project '{project_id_correct}'")
                continue

            month_number = month_dir.strip()
            if not month_number.isdigit():
                logger.warning(f"Invalid month directory name '{month_number}' in project '{project_id_correct}'. Skipping.")
                continue

            logger.info(f"Processing month: {month_number} for project: {project_id_correct}")

            # Initialize month entry if not already present
            if month_number not in email_links_data[project_id_correct]['months']:
                email_links_data[project_id_correct]['months'][month_number] = []

            # Iterate over each CSV file in the month directory
            for csv_file in os.listdir(month_path):
                if not csv_file.endswith('.csv'):
                    logger.warning(f"Skipping non-CSV file: {csv_file} in project '{project_id_correct}', month '{month_number}'")
                    continue

                csv_path = os.path.join(month_path, csv_file)
                logger.info(f"Processing file: {csv_file} in project '{project_id_correct}', month '{month_number}'")

                # Load CSV data
                csv_data = load_csv_file(csv_path)
                if csv_data is None:
                    logger.error(f"Skipping file '{csv_file}' in project '{project_id_correct}', month '{month_number}' due to failed CSV load.")
                    continue

                # Append each row to the month's list
                for row in csv_data:
                    human_date_time = row.get('human_date_time')
                    link = row.get('link')
                    dealised_author_full_name = row.get('dealised_author_full_name')

                    if not human_date_time or not link or not dealised_author_full_name:
                        logger.warning(f"Missing data in file '{csv_file}', project '{project_id_correct}', month '{month_number}'. Skipping row.")
                        continue

                    email_entry = {
                        'human_date_time': human_date_time,
                        'link': link,
                        'dealised_author_full_name': dealised_author_full_name
                    }

                    email_links_data[project_id_correct]['months'][month_number].append(email_entry)
                    logger.debug(f"Added email entry for project '{project_id_correct}', month '{month_number}': {email_entry}")

    # Insert or update documents in MongoDB
    for project_id, data in email_links_data.items():
        try:
            # Upsert the document: insert if it doesn't exist, update if it does
            collection.update_one(
                {'project_id': project_id},
                {'$set': data},
                upsert=True
            )
            logger.info(f"Inserted/Updated email_links data for project_id '{project_id}'.")
        except Exception as e:
            logger.error(f"Failed to insert/update email_links data for project_id '{project_id}': {e}")

    logger.info("Completed loading email_links data into MongoDB.")

# Load data for commit_links (for the formation of when you click on a certain node, it should load)
def load_commit_links_data():
    """Load commit_links data into MongoDB grouped by project and month."""
    collection = db.eclipse_commit_links  
    base_path = os.path.join('data', 'new', 'new_emails')

    # Ensure the base_path exists
    if not os.path.exists(base_path):
        logger.error(f"Commit links data directory not found: {base_path}")
        return

    # Dictionary to hold all data before insertion
    commit_links_data = {}

    # Iterate over each project directory in commit_links
    for project_dir in os.listdir(base_path):
        project_path = os.path.join(base_path, project_dir)
        if not os.path.isdir(project_path):
            logger.warning(f"Skipping non-directory item: {project_dir}")
            continue
        
        project_id = project_dir.strip().lower()
        logger.info(f"Processing project: {project_id}")

        # Retrieve project information
        project_info = get_project_info(project_id)
        if not project_info:
            logger.warning(f"Skipping project '{project_id}' due to missing project information.")
            continue
        project_id_correct = project_info['project_id']
        project_name = project_info['project_name']
        logger.debug(f"Found project_info: project_id='{project_id_correct}', project_name='{project_name}'")

        # Initialize project entry if not already present
        if project_id_correct not in commit_links_data:
            commit_links_data[project_id_correct] = {
                'project_id': project_id_correct,
                'project_name': project_name,
                'months': {}
            }

        # Iterate over each month directory within the project
        for month_dir in os.listdir(project_path):
            month_path = os.path.join(project_path, month_dir)
            if not os.path.isdir(month_path):
                logger.warning(f"Skipping non-directory item: {month_dir} in project '{project_id_correct}'")
                continue

            month_number = month_dir.strip()
            if not month_number.isdigit():
                logger.warning(f"Invalid month directory name '{month_number}' in project '{project_id_correct}'. Skipping.")
                continue

            logger.info(f"Processing month: {month_number} for project: {project_id_correct}")

            # Initialize month entry if not already present
            if month_number not in commit_links_data[project_id_correct]['months']:
                commit_links_data[project_id_correct]['months'][month_number] = []

            # Iterate over each CSV file in the month directory
            for csv_file in os.listdir(month_path):
                if not csv_file.endswith('.csv'):
                    logger.warning(f"Skipping non-CSV file: {csv_file} in project '{project_id_correct}', month '{month_number}'")
                    continue

                csv_path = os.path.join(month_path, csv_file)
                logger.info(f"Processing file: {csv_file} in project '{project_id_correct}', month '{month_number}'")

                # Load CSV data
                csv_data = load_csv_file(csv_path)
                if csv_data is None:
                    logger.error(f"Skipping file '{csv_file}' in project '{project_id_correct}', month '{month_number}' due to failed CSV load.")
                    continue

                # Append each row to the month's list
                for row in csv_data:
                    human_date_time = row.get('human_date_time')
                    link = row.get('link')
                    dealised_author_full_name = row.get('dealised_author_full_name')

                    if not human_date_time or not link or not dealised_author_full_name:
                        logger.warning(f"Missing data in file '{csv_file}', project '{project_id_correct}', month '{month_number}'. Skipping row.")
                        continue

                    commit_entry = {
                        'human_date_time': human_date_time,
                        'link': link,
                        'dealised_author_full_name': dealised_author_full_name
                    }

                    commit_links_data[project_id_correct]['months'][month_number].append(commit_entry)
                    logger.debug(f"Added commit entry for project '{project_id_correct}', month '{month_number}': {commit_entry}")

    # Insert or update documents in MongoDB
    for project_id, data in commit_links_data.items():
        try:
            # Upsert the document: insert if it doesn't exist, update if it does
            collection.update_one(
                {'project_id': project_id},
                {'$set': data},
                upsert=True
            )
            logger.info(f"Inserted/Updated commit_links data for project_id '{project_id}'.")
        except Exception as e:
            logger.error(f"Failed to insert/update commit_links data for project_id '{project_id}': {e}")

    logger.info("Completed loading commit_links data into MongoDB.")

def main():
    # For a fresh insertion to MongoDB, follow this order below
    
    # print(process_eclipse_project_info())
    # print(load_eclipse_tech_net())
    # print(load_eclipse_social_net())
    print(load_eclipse_grad_forecast())
    # print(load_eclipse_email_measure())
    # print(load_eclipse_commit_measure())
    # print(load_eclipse_issues_measure())
    # print(load_eclipse_email_links_data())
    
    logger.info("All data has been processed and loaded into MongoDB.")

main()