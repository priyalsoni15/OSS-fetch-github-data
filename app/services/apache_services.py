from datetime import datetime, timedelta
import mailbox
import os
import random
import tempfile
from bs4 import BeautifulSoup
import requests
import logging
import json
import time
from itertools import cycle
from urllib.parse import quote
from app.config import Config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_apache_repositories_from_website():
    logging.info("Fetching Apache repositories from the website...")
    repos = []
    api_calls = 0
    try:
        # Apache provides a JSON file with project metadata
        base_url = "https://projects.apache.org/json/foundation/projects.json"
        response = requests.get(base_url)
        api_calls += 1
        if response.status_code != 200:
            logging.error(f"Error fetching Apache repositories from website: {response.status_code} {response.text}")
            return repos
        
        projects_data = response.json()
        for project_name, project_info in projects_data.items():
            # Each project may have multiple repositories
            scm = project_info.get('scm', {})
            git_repos = scm.get('git', [])
            for repo in git_repos:
                repos.append(repo)
        
        logging.info(f"Fetched {len(repos)} repositories from the website.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
    
    # Save repos data to JSON file
    with open("apache_repos_website.json", "w") as json_file:
        json.dump(repos, json_file, indent=4)
    
    return repos

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
    variables = {"org": Config.ORG_NAME, "cursor": None}
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
                repos.append(repo['url'])

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

    # Save repos data to JSON file
    with open("apache_repos_github.json", "w") as json_file:
        json.dump(repos, json_file, indent=4)

    return repos

def merge_repositories(website_repos, github_repos):
    logging.info("Merging repositories from website and GitHub...")
    # Extract repository names from URLs for comparison
    website_repo_names = [repo.split('/')[-1].replace('.git', '') for repo in website_repos]
    github_repo_names = [repo.split('/')[-1] for repo in github_repos]
    merged_repos = list(set(website_repo_names + github_repo_names))
    logging.info(f"Total merged repositories: {len(merged_repos)}")
    
    # Save merged repos data to JSON file
    with open("apache_repos_merged.json", "w") as json_file:
        json.dump(merged_repos, json_file, indent=4)
    
    return merged_repos

def merge_repos_temp():
    # Fetch repositories from both sources
    website_repos = fetch_apache_repositories_from_website()
    github_repos = fetch_apache_repositories_from_github()
    merged_repos = merge_repositories(website_repos, github_repos)
    return merged_repos

# --- The functions below are for fetching the Apache Mailing list data ---


def fetch_mailing_list_data(repo_name):
    logger.info(f"Fetching mailing list data for repository: {repo_name}")
    mailing_data = {}
    list_name = f"{repo_name}-dev"
    base_url = f"https://mail-archives.apache.org/mod_mbox/{list_name}/"

    # Define the date range you want to fetch (e.g., from January 2016 to current month)
    start_date = datetime(2016, 1, 1)
    end_date = datetime.now()

    mailing_data[repo_name] = {'emails': []}

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

                    mailing_data[repo_name]['emails'].append({
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

    if mailing_data[repo_name]['emails']:
        # Save the data for this repository
        output_dir = os.path.join(os.getcwd(), 'out', 'apache', 'mailing_list', repo_name)
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, 'mailing_list_data.json')
        try:
            with open(output_file, "w") as json_file:
                json.dump(mailing_data[repo_name], json_file, indent=4)
            logger.info(f"Mailing list data saved to {output_file}")
        except Exception as e:
            logger.error(f"Error saving mailing list data to file: {e}")
            return {}
    else:
        logger.warning(f"No emails were extracted for repository: {repo_name}")
        return {}

    return mailing_data

def fetch_apache_mailing_list_data():
    mailing_data = {}
    # Process the 'arrow' repository (you can add more repositories to this list)
    merged_repos = ["arrow"]
    success_repos = []
    for repo_name in merged_repos:
        # Fetch mailing list data for this repository
        repo_mailing_data = fetch_mailing_list_data(repo_name)
        if repo_mailing_data.get(repo_name):
            mailing_data.update(repo_mailing_data)
            success_repos.append(repo_name)
        else:
            logger.warning(f"No mailing list data found for repository: {repo_name}")
    if success_repos:
        message = f"Mailing list data fetched and saved for repositories: {', '.join(success_repos)}"
    else:
        message = "No mailing list data fetched."
    logger.info(message)
    return message