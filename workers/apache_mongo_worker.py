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
    
    DATA_DIR = os.path.join(os.getcwd(), 'out', 'apache', 'github')
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

# This fetches all the data from Apache website
def fetch_all_podlings():
    url = 'https://incubator.apache.org/projects/'
    headers = {'User-Agent': 'Mozilla/5.0'}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Error fetching the projects page: {e}")
        return []

    soup = BeautifulSoup(response.content, 'html.parser')

    # Sections to parse
    sections = [
        {'id': 'current', 'status': 'current'},
        {'id': 'graduated', 'status': 'graduated'},
        {'id': 'retired', 'status': 'retired'}
    ]

    all_projects = []

    for section in sections:
        projects = parse_podling_section(soup, section['id'], section['status'])
        all_projects.extend(projects)

    # Save all_projects data to MongoDB
    if all_projects:
        try:
            db.apache_projects.drop()
            db.apache_projects.insert_many(all_projects)
            logging.info("Apache projects data saved to MongoDB collection 'apache_projects'.")
        except Exception as e:
            logger.error(f"Error saving Apache projects to MongoDB: {e}")
            return []

    return all_projects

# This parses each project's Apache page and gets the relevant data
def parse_podling_section(soup, section_id, status):
    section_header = soup.find('h3', id=section_id)
    if not section_header:
        logger.warning(f"Could not find section with id '{section_id}'.")
        return []

    # Find the table immediately following the header
    table = section_header.find_next('table', class_='colortable')
    if not table:
        logger.warning(f"Could not find the projects table for section '{section_id}'.")
        return []

    projects = []

    # Iterate over the table rows, skipping the header row
    rows = table.find_all('tr')[1:]

    for row in rows:
        cols = row.find_all('td')
        if len(cols) < 6:
            continue  # Skip if not enough columns

        # Extract data from columns
        project_td = cols[0]
        aliases_td = cols[1]
        description_td = cols[2]
        sponsor_td = cols[3]
        mentors_td = cols[4]
        start_date_td = cols[5]

        # Project name and link
        project_link = project_td.find('a')
        if project_link:
            project_name = project_link.text.strip()
            project_url = 'https://incubator.apache.org' + project_link['href']
            project_id = project_link['href'].split('/')[-1].replace('.html', '').strip()
        else:
            project_name = project_td.text.strip()
            project_url = ''
            project_id = project_name.lower().replace(' ', '').replace('-', '').replace('_', '')

        # Aliases
        aliases = aliases_td.get_text(separator=', ').strip()

        # Description
        description = description_td.text.strip()

        # Sponsor and Champion
        # The sponsor and champion may be separated by <br/> tags
        sponsor_html = sponsor_td.decode_contents()
        sponsor_parts = sponsor_html.split('<br/>')
        sponsor = BeautifulSoup(sponsor_parts[0], 'html.parser').get_text(strip=True)
        champion = ''
        if len(sponsor_parts) > 1:
            champion_text = sponsor_parts[1]
            champion = BeautifulSoup(champion_text, 'html.parser').get_text(strip=True).strip('()')

        # Mentors
        mentors = [mentor.strip() for mentor in mentors_td.get_text(separator=',').split(',') if mentor.strip()]

        # Start Date
        start_date = start_date_td.text.strip()

        project_info = {
            'project_name': project_name,
            'project_id': project_id,
            'project_url': project_url,
            'aliases': aliases,
            'description': description,
            'sponsor': sponsor,
            'champion': champion,
            'mentors': mentors,
            'start_date': start_date,
            'status': status  # Add the status of the project
        }

        projects.append(project_info)

    return projects


# Fetch all repositories from the Apache GitHub organization and store them in MongoDB
def fetch_apache_repositories_from_github():
    logging.info("Fetching Apache repositories from GitHub...")
    tokens = Config.GITHUB_TOKENS or [Config.GITHUB_TOKEN]
    if not tokens or tokens == [None]:
        logging.error("No GitHub tokens found. Please set GITHUB_TOKENS or GITHUB_TOKEN in your environment variables.")
        return []

    token_cycle = cycle(tokens)
    headers = {"Authorization": f"Bearer {next(token_cycle)}"}
    query = """
    query($org: String!, $cursor: String) {
      organization(login: $org) {
        repositories(first: 100, after: $cursor, privacy: PUBLIC, orderBy: {field: NAME, direction: ASC}) {
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
            name
            url
            stargazerCount
            forkCount
            watchers {
              totalCount
            }
          }
        }
      }
      rateLimit {
        remaining
        resetAt
      }
    }
    """
    variables = {"org": "apache", "cursor": None}
    has_next_page = True
    repos = []
    api_calls = 0

    while has_next_page:
        try:
            response = requests.post(
                'https://api.github.com/graphql',
                json={"query": query, "variables": variables},
                headers=headers
            )
            api_calls += 1

            if response.status_code in [401, 403]:
                # Rotate to the next token
                try:
                    headers["Authorization"] = f"Bearer {next(token_cycle)}"
                    logging.warning("Rotated to the next token due to unauthorized or rate limit error.")
                except StopIteration:
                    logging.error("No more GitHub tokens available.")
                    break
                continue

            if response.status_code != 200:
                logging.error(f"Error fetching repositories from GitHub: {response.status_code} {response.text}")
                break

            result = response.json()
            if 'errors' in result:
                logging.error(f"GraphQL errors: {result['errors']}")
                break

            organization = result.get('data', {}).get('organization')
            if not organization:
                logging.error("No organization data found in GitHub response.")
                break

            repositories = organization['repositories']
            has_next_page = repositories['pageInfo']['hasNextPage']
            variables['cursor'] = repositories['pageInfo']['endCursor']

            for repo in repositories['nodes']:
                # Extract fields with default values if missing
                name = repo.get('name')
                url = repo.get('url')
                stargazer_count = repo.get('stargazerCount', 0)
                fork_count = repo.get('forkCount', 0)
                watchers = repo.get('watchers', {})
                watch_count = watchers.get('totalCount', 0) if watchers else 0

                if name and url:
                    repo_data = {
                        'name': name,
                        'url': url,
                        'stargazer_count': stargazer_count,
                        'fork_count': fork_count,
                        'watch_count': watch_count
                    }
                    repos.append(repo_data)
                else:
                    logging.warning(f"Repository missing 'name' or 'url': {repo}")

            logging.info(f"Fetched {len(repos)} repositories from GitHub so far.")

            # Check rate limit after GraphQL API call
            rate_limit = result.get('data', {}).get('rateLimit', {})
            rate_limit_remaining = int(rate_limit.get('remaining', 1))
            rate_limit_reset_at = rate_limit.get('resetAt', None)

            if rate_limit_remaining == 0 and rate_limit_reset_at:
                reset_time = datetime.strptime(rate_limit_reset_at, "%Y-%m-%dT%H:%M:%SZ")
                sleep_time = (reset_time - datetime.utcnow()).total_seconds()
                sleep_time = max(int(sleep_time) + 5, 0)  # Add buffer time
                logging.info(f"Rate limit reached. Sleeping for {sleep_time} seconds until reset.")
                time.sleep(sleep_time)
                api_calls = 0  # Reset API call count after waiting

        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}. Retrying...")
            time.sleep(random.uniform(1, 3))
            continue

    # Save repos data to MongoDB
    if repos:
        try:
            # Use upsert to update existing repositories and insert new ones
            for repo in repos:
                db.github_repositories.update_one(
                    {'name': repo['name']},
                    {'$set': repo},
                    upsert=True
                )
            logging.info("Repositories data saved to MongoDB collection 'github_repositories'.")
        except Exception as e:
            logging.error(f"Error saving repositories to MongoDB: {e}")
            return []

    # Return the repositories without '_id' fields
    return repos


#################### 2022 Code for Apache loading to DB ###################

# Loading data for processing social network for projects

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
    Retrieve project information from apache_projects collection based on project_id.
    Returns a dictionary with 'project_id' and 'project_name'.
    """
    if not isinstance(project_id, str):
        logger.warning(f"Invalid project_id type: {project_id}")
        return None
    
    # Normalize project_id by stripping spaces and converting to lowercase
    normalized_project_id = project_id.strip().lower()
    
    project = db.apache_projects.find_one({'project_id': normalized_project_id})
    if project:
        return {
            'project_id': project.get('project_id'),
            'project_name': project.get('project_name')
        }
    else:
        logger.warning(f"Project '{normalized_project_id}' not found in 'apache_projects' collection.")
        return None

# Get project IDs from DB for matching with files
def list_project_ids():
    try:
        project_ids = db.apache_projects.distinct("project_id")
        logger.info(f"Total projects found: {len(project_ids)}")
        for pid in project_ids:
            logger.info(f"Project ID: {pid}")
    except Exception as e:
        logger.error(f"Error fetching project_ids: {e}")

# Load technical network month by month
def load_tech_net():
    """Load tech_net data into MongoDB grouped by project and month."""
    """{
  "project_id": "abdera",
  "project_name": "Abdera",
  "months": {
    "1": [
      ["Elias Torres", "html", 1],
      ["Elias Torres", "java", 68],
      ...
    ],
    "2": [
      ["James Snell", "css", 3],
      ["James Snell", "html", 85],
      ...
    ],
  }
}
    """
    collection = db.tech_net
    base_path = os.path.join(Config.DATA_DIR_STATIC, 'new', 'tech_net', 'new_commit')
    
    # Ensure the base_path exists
    if not os.path.exists(base_path):
        logger.error(f"Tech network data directory not found: {base_path}")
        return

    # Dictionary to hold all data before insertion
    tech_net_data = {}

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
            collection.update_one(
                {'project_id': project_id},
                {'$set': data},
                upsert=True
            )
            logger.info(f"Inserted/Updated tech_net data for project_id '{project_id}'.")
        except Exception as e:
            logger.error(f"Failed to insert/update tech_net data for project_id '{project_id}': {e}")

    logger.info("Completed loading tech_net data into MongoDB.")

# For loading social network data from static files into MongoDB
def load_social_net():
    """Load social_net data into MongoDB grouped by project and month."""
    collection = db.social_net
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

# For loading the graduation forecast, i.e. the health of the projects:
def load_grad_forecast():
    """Load grad_forecast data into MongoDB grouped by project and month."""
    collection = db.grad_forecast
    base_path = os.path.join(Config.DATA_DIR_STATIC, 'new','grad_forecast')  # Adjust path if necessary

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
            project_id_part = filename.split('_f_data.csv')[0].strip().lower()
            logger.debug(f"Extracted project_id: '{project_id_part}'")

            # Retrieve project information
            project_info = get_project_info(project_id_part)
            if not project_info:
                logger.warning(f"Skipping file '{filename}' due to missing project information.")
                continue
            project_id = project_info['project_id']
            project_name = project_info['project_name']
            logger.debug(f"Found project_info: project_id='{project_id}', project_name='{project_name}'")

            filepath = os.path.join(base_path, filename)
            raw_data = load_csv_file(filepath)
            if raw_data is None:
                logger.error(f"Skipping file '{filename}' due to failed CSV load.")
                continue

            # Initialize project entry if not already present
            if project_id not in grad_forecast_data:
                grad_forecast_data[project_id] = {
                    'project_id': project_id,
                    'project_name': project_name,
                    'forecast': {}
                }

            # Assign data to the corresponding month
            for row in raw_data:
                date = row.get('date')
                close = row.get('close')
                if not date or not close:
                    logger.warning(f"Missing 'date' or 'close' in file '{filename}', row: {row}. Skipping row.")
                    continue
                if not date.isdigit():
                    logger.warning(f"Invalid 'date' value '{date}' in file '{filename}'. Skipping row.")
                    continue
                try:
                    date_int = int(date)
                    close_float = float(close)
                except ValueError:
                    logger.warning(f"Invalid data types in file '{filename}', row: {row}. Skipping row.")
                    continue

                grad_forecast_data[project_id]['forecast'][str(date_int)] = {
                    'date': date_int,
                    'close': close_float
                }
                logger.debug(f"Added forecast for project '{project_id}', month '{date_int}': {close_float}")

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
def load_email_measure():
    """Load tech_net data into MongoDB grouped by project and month."""
    collection = db.email_measure
    base_path = os.path.join(Config.DATA_DIR_STATIC, 'new', 'email_measure')
    
    # Ensure the base_path exists
    if not os.path.exists(base_path):
        logger.error(f"Email measure directory not found: {base_path}")
        return

    # Dictionary to hold all data before insertion
    email_measure_data = {}

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
            if project_id not in email_measure_data:
                email_measure_data[project_id] = {
                    'project_id': project_id,
                    'project_name': project_name,
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

# Load data for commit_measure (data below the socio-tech net for each month)
def load_commit_measure():
    """Load tech_net data into MongoDB grouped by project and month."""
    collection = db.commit_measure
    base_path = os.path.join(Config.DATA_DIR_STATIC, 'new', 'commit_measure')
    
    # Ensure the base_path exists
    if not os.path.exists(base_path):
        logger.error(f"Commit measure directory not found: {base_path}")
        return

    # Dictionary to hold all data before insertion
    email_measure_data = {}

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
            if project_id not in email_measure_data:
                email_measure_data[project_id] = {
                    'project_id': project_id,
                    'project_name': project_name,
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
            logger.info(f"Inserted/Updated commit measure data for project_id '{project_id}'.")
        except Exception as e:
            logger.error(f"Failed to insert/update commit measure data for project_id '{project_id}': {e}")

    logger.info("Completed loading commit_measure data into MongoDB.")

# Load data for commit_links (for the formation of when you click on a certain node, it should load)
def load_commit_links_data():
    """Load commit_links data into MongoDB grouped by project and month."""
    collection = db.commit_links  
    base_path = os.path.join(Config.DATA_DIR_STATIC, 'new','commit_links') 

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


# Load data for email links
def load_email_links_data():
    """Load commit_links data into MongoDB grouped by project and month."""
    collection = db.email_links  
    base_path = os.path.join(Config.DATA_DIR_STATIC, 'new','email_links') 

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


# Load project descriptions for projects
def process_project_info():
    project_info_dir = os.path.join('data', 'new', 'project_info')
    about_data_dir = os.path.join(project_info_dir, 'new_about_data')
    month_interval_dir = os.path.join(project_info_dir, 'new_month_interval')

    projects = {}

    # Process 'new_about_data'
    for filename in os.listdir(about_data_dir):
        if filename.endswith('.json'):
            project_name = filename.replace('.json', '')
            with open(os.path.join(about_data_dir, filename), 'r') as f:
                about_data = json.load(f)
                about_data['project_name'] = project_name
                projects[project_name] = about_data

    # Process 'new_month_interval'
    for filename in os.listdir(month_interval_dir):
        if filename.endswith('.json'):
            project_name = filename.replace('.json', '')
            with open(os.path.join(month_interval_dir, filename), 'r') as f:
                month_interval_data = json.load(f)
                if project_name in projects:
                    projects[project_name]['month_interval'] = month_interval_data
                else:
                    projects[project_name] = {
                        'project_name': project_name,
                        'month_interval': month_interval_data
                    }

    # Save to MongoDB
    if projects:
        try:
            db.project_info.drop()
            db.project_info.insert_many(projects.values())
            logger.info("Project info data saved to MongoDB collection 'project_info'.")
        except Exception as e:
            logger.error(f"Error saving project info to MongoDB: {e}")


# This is for loading the project description and apache project details in the database:
def load_project_info():
    """Load project_info data into MongoDB by combining data from new_about_data and new_month_intervals."""
    collection = db.project_info  # Define the collection for project_info
    base_path_about = os.path.join(Config.DATA_DIR_STATIC, 'new','project_info', 'new_about_data')
    base_path_intervals = os.path.join(Config.DATA_DIR_STATIC, 'new','project_info', 'new_month_intervals')

    # Ensure both base paths exist
    if not os.path.exists(base_path_about):
        logger.error(f"Project info 'new_about_data' directory not found: {base_path_about}")
        return
    if not os.path.exists(base_path_intervals):
        logger.error(f"Project info 'new_month_intervals' directory not found: {base_path_intervals}")
        return

    # List of project_ids based on files in new_about_data
    project_ids = [f[:-5].strip().lower() for f in os.listdir(base_path_about) if f.endswith('.json')]

    # Dictionary to hold all data before insertion
    project_info_data = {}

    for project_id in project_ids:
        logger.info(f"Processing project_info for: {project_id}")

        # Retrieve project information
        project_info = get_project_info(project_id)
        if not project_info:
            logger.warning(f"Skipping project_info for '{project_id}' due to missing project information.")
            continue
        project_id_correct = project_info['project_id']
        project_name = project_info['project_name']
        logger.debug(f"Found project_info: project_id='{project_id_correct}', project_name='{project_name}'")

        # Paths to the two JSON files
        about_file = os.path.join(base_path_about, f"{project_id}.json")
        intervals_file = os.path.join(base_path_intervals, f"{project_id}.json")

        # Load about data
        about_data = load_json_file(about_file)
        if about_data is None:
            logger.error(f"Skipping project_info for '{project_id_correct}' due to failed load of about data.")
            continue

        # Load month intervals data
        intervals_data = load_json_file(intervals_file)
        if intervals_data is None:
            logger.error(f"Skipping project_info for '{project_id_correct}' due to failed load of month intervals data.")
            continue

        # Combine data
        combined_data = {
            'project_id': project_id_correct,
            'project_name': project_name,
            'alias': about_data.get('alias'),
            'description': about_data.get('description'),
            'sponsor': about_data.get('sponsor'),
            'mentor': about_data.get('mentor'),
            'start_date': about_data.get('start_date'),
            'end_date': about_data.get('end_date'),
            'status': about_data.get('status'),
            'incubation_time': about_data.get('incubation_time'),
            'month_intervals': intervals_data
        }

        project_info_data[project_id_correct] = combined_data
        logger.info(f"Combined project_info for '{project_id_correct}'.")

    # Insert or update documents in MongoDB
    for project_id, data in project_info_data.items():
        try:
            # Upsert the document: insert if it doesn't exist, update if it does
            collection.update_one(
                {'project_id': project_id},
                {'$set': data},
                upsert=True
            )
            logger.info(f"Inserted/Updated project_info data for project_id '{project_id}'.")
        except Exception as e:
            logger.error(f"Failed to insert/update project_info data for project_id '{project_id}': {e}")

    logger.info("Completed loading project_info data into MongoDB.")

# This is to add the monthly ranges for the project to add it into the range sldier
def process_monthly_ranges():
    """Load monthly range JSON files and store them in MongoDB with projectID mapping."""
    base_path = os.path.join(Config.DATA_DIR_STATIC, 'new', 'month_intervals')
    if not os.path.exists(base_path):
        logger.error(f"Base path {base_path} does not exist.")
        return

    # Iterate through all JSON files in the directory
    for filename in os.listdir(base_path):
        if filename.endswith('.json'):
            project_id = os.path.splitext(filename)[0]  # Extract projectID from the filename
            file_path = os.path.join(base_path, filename)

            try:
                # Load the JSON data
                with open(file_path, 'r', encoding='utf-8') as f:
                    monthly_data = json.load(f)

                # Validate and process the data structure
                if not isinstance(monthly_data, dict):
                    logger.warning(f"Invalid data structure in file {filename}. Skipping...")
                    continue

                # Add projectID to the data
                processed_data = {
                    'project_id': project_id,
                    'monthly_ranges': monthly_data,
                    'last_updated': datetime.utcnow()
                }

                # Save the data to MongoDB
                db.monthly_ranges.update_one(
                    {'project_id': project_id},
                    {'$set': processed_data},
                    upsert=True
                )
                logger.info(f"Data for project '{project_id}' successfully saved to MongoDB.")

            except Exception as e:
                logger.error(f"Failed to process file {filename}: {e}")
                continue

def main():
    # print(fetch_apache_repositories_from_github())
    # print(fetch_all_podlings())
    # print(load_tech_net())
    # print(load_social_net())
    # print(process_monthly_ranges())
    # print(load_project_info())
    # print(process_project_info())
    # print(load_email_links_data())
    # print(load_commit_links_data())
    print(load_grad_forecast())
    # print(load_commit_measure())
    # print(load_email_measure())
    
    logger.info("All data has been processed and loaded into MongoDB.")

main()