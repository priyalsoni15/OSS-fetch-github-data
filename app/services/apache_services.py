from datetime import datetime, timedelta
import mailbox
import os
import random
import tempfile
import requests
import logging
import time
from itertools import cycle
from app.config import Config
from bs4 import BeautifulSoup
import difflib
from pymongo import MongoClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize MongoDB client
mongo_client = MongoClient(Config.MONGODB_URI)
db = mongo_client[Config.MONGODB_DB_NAME]

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
        repositories(first: 100, after: $cursor) {
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
            name
            url
          }
        }
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

            if response.status_code == 401 or response.status_code == 403:
                # Rotate to the next token
                headers["Authorization"] = f"Bearer {next(token_cycle)}"
                logging.warning("Rotated to the next token due to unauthorized or rate limit error.")
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
                repos.append({'name': repo['name'], 'url': repo['url']})

            logging.info(f"Fetched {len(repos)} repositories from GitHub so far.")

            # Check rate limit after GraphQL API call
            rate_limit_remaining = int(response.headers.get('X-RateLimit-Remaining', 1))
            rate_limit_reset = int(response.headers.get('X-RateLimit-Reset', time.time() + 60))

            if rate_limit_remaining == 0:
                sleep_time = max(rate_limit_reset - int(time.time()), 0)
                logging.info(f"Rate limit reached. Sleeping for {sleep_time} seconds.")
                time.sleep(sleep_time)
                api_calls = 0  # Reset API call count after waiting

        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}. Retrying...")
            time.sleep(random.uniform(1, 3))
            continue

    # Save repos data to MongoDB
    if repos:
        try:
            db.github_repositories.drop()
            db.github_repositories.insert_many(repos)
            logging.info("Repositories data saved to MongoDB collection 'github_repositories'.")
        except Exception as e:
            logging.error(f"Error saving repositories to MongoDB: {e}")
            return []

    # Return the repositories without '_id' fields
    return repos


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

# Fetch mailing list data for a repository and save to MongoDB
def fetch_mailing_list_data(repo_name):
    logger.info(f"Fetching mailing list data for repository: {repo_name}")
    mailing_data = []
    list_name = f"{repo_name}-dev"
    base_url = f"https://mail-archives.apache.org/mod_mbox/{list_name}/"

    # Define the date range you want to fetch
    start_date = datetime(2016, 1, 1)
    end_date = datetime.now()

    current_date = start_date
    while current_date <= end_date:
        year_month = current_date.strftime('%Y%m')
        mbox_url = f"{base_url}{year_month}.mbox"
        logger.info(f"Processing mbox file: {mbox_url}")

        try:
            response = requests.get(mbox_url, stream=True)
            if response.status_code == 200:
                # Save the mbox content to a temporary file
                with tempfile.NamedTemporaryFile(mode='wb', delete=False) as temp_mbox_file:
                    for chunk in response.iter_content(chunk_size=8192):
                        temp_mbox_file.write(chunk)
                    temp_mbox_file_path = temp_mbox_file.name

                # Use the mailbox module to parse the mbox file
                mbox = mailbox.mbox(temp_mbox_file_path)
                for message in mbox:
                    subject = message.get('subject', '')
                    sender = message.get('from', '')
                    date = message.get('date', '')
                    is_reply = subject.lower().startswith('re:')
                    message_id = message.get('message-id', '')
                    in_reply_to = message.get('in-reply-to', '')

                    mailing_data.append({
                        'repo_name': repo_name,
                        'subject': subject,
                        'sender': sender,
                        'date': date,
                        'is_reply': is_reply,
                        'message_id': message_id,
                        'in_reply_to': in_reply_to,
                    })

                # Remove the temporary mbox file
                os.remove(temp_mbox_file_path)
                logger.info(f"Processed {len(mbox)} emails from {year_month}")
            else:
                logger.warning(f"No mbox file found for {year_month} (HTTP {response.status_code})")
        except Exception as e:
            logger.error(f"Error processing mbox file {mbox_url}: {e}")

        # Move to the next month
        current_date += timedelta(days=31)
        current_date = current_date.replace(day=1)

    if mailing_data:
        try:
            # Insert mailing data into MongoDB
            db.mailing_list_data.insert_many(mailing_data)
            logger.info(f"Mailing list data saved to MongoDB collection 'mailing_list_data'.")
        except Exception as e:
            logger.error(f"Error saving mailing list data to MongoDB: {e}")
            return []
    else:
        logger.warning(f"No emails were extracted for repository: {repo_name}")
        return []

    return mailing_data

def fetch_apache_mailing_list_data():
    # Process the repositories (you can add more repositories to this list)
    merged_repos = ["arrow"]  # Add more repository names as needed
    for repo_name in merged_repos:
        # Fetch mailing list data for this repository
        fetch_mailing_list_data(repo_name)
    message = "Mailing list data fetched and saved to MongoDB."
    logger.info(message)
    return message

# This matches Apache projects ID with Github project link names
def fetch_all_podlings_with_github_repos():
    # Fetch all repositories under 'apache' organization
    repos = fetch_apache_repositories_from_github()
    repo_name_to_url = {repo['name'].lower(): repo['url'] for repo in repos}
    repo_names = list(repo_name_to_url.keys())

    # Fetch all podlings
    all_projects = fetch_all_podlings()

    for project in all_projects:
        # Use project_id for matching
        project_id = project['project_id'].lower()
        matched_repo_name = None

        # Attempt direct match
        if project_id in repo_name_to_url:
            matched_repo_name = project_id
        else:
            # Try matching with variations
            possible_names = [
                project_id,
                project_id.replace('-', ''),
                project_id.replace('_', ''),
                project_id.replace('-', '_'),
                project_id.replace('_', '-')
            ]

            # Use difflib to find close matches
            close_matches = difflib.get_close_matches(project_id, repo_names, n=1, cutoff=0.8)
            if close_matches:
                matched_repo_name = close_matches[0]
            else:
                # Try matching with possible variations
                for name in possible_names:
                    close_matches = difflib.get_close_matches(name, repo_names, n=1, cutoff=0.8)
                    if close_matches:
                        matched_repo_name = close_matches[0]
                        break

        if matched_repo_name:
            project['github_repo_name'] = matched_repo_name
            project['github_url'] = repo_name_to_url[matched_repo_name]
        else:
            project['github_repo_name'] = None
            project['github_url'] = None

    # Save the combined data to MongoDB
    if all_projects:
        try:
            db.projects_with_github_repos.drop()
            db.projects_with_github_repos.insert_many(all_projects)
            logging.info("Combined projects data saved to MongoDB collection 'projects_with_github_repos'.")
        except Exception as e:
            logger.error(f"Error saving combined projects data to MongoDB: {e}")
            return []

    return all_projects


# New apache services code here

