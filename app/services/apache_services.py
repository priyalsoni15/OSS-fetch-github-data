from datetime import datetime, timedelta, timezone
import mailbox
import os
import random
import tempfile
import requests
import logging
import json
import time
from itertools import cycle
from app.config import Config
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
    output_dir = os.path.join(os.getcwd(), 'out', 'apache', 'parent')
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, 'apache_repos_github.json')
    with open(output_file, "w") as json_file:
        json.dump(repos, json_file, indent=4)

    return repos

# --- New function to fetch Apache projects data ---
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

    return all_projects

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
        else:
            project_name = project_td.text.strip()
            project_url = ''

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
