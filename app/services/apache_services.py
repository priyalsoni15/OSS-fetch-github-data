import requests
import logging
import time
import json
import random
from bs4 import BeautifulSoup
import asyncio
import aiohttp
from app.config import Config

def fetch_apache_repositories_from_website():
    logging.info("Fetching Apache repositories from the website...")
    base_url = "https://projects.apache.org/projects.html"
    repos = []
    api_calls = 0
    try:
        response = requests.get(base_url)
        api_calls += 1
        if response.status_code != 200:
            logging.error(f"Error fetching Apache repositories from website: {response.status_code} {response.text}")
            return repos
        
        soup = BeautifulSoup(response.content, 'html.parser')
        links = soup.find_all('a', href=True)
        for link in links:
            if "/projects/" in link['href']:
                repo_name = link['href'].split('/')[-1]
                if repo_name and not repo_name.startswith('javascript:'):
                    repos.append(repo_name)
        
        logging.info(f"Fetched {len(repos)} repositories from the website.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
    
    # Save repos data to JSON file
    with open("apache_repos_website_compare.json", "w") as json_file:
        json.dump(repos, json_file, indent=4)
    
    return repos

def fetch_apache_repositories_from_github():
    logging.info("Fetching Apache repositories from GitHub...")
    headers = {"Authorization": f"Bearer {Config.GITHUB_TOKEN}"}
    query = """
    query($org: String!, $cursor: String) {
      organization(login: $org) {
        repositories(first: 50, after: $cursor) {
          pageInfo {
            hasNextPage
            endCursor
          }
          nodes {
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

        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}. Retrying...")
            time.sleep(random.uniform(1, 3))
            continue

    # Save repos data to JSON file
    with open("apache_repos_github_compare.json", "w") as json_file:
        json.dump(repos, json_file, indent=4)

    return repos

def merge_repositories(website_repos, github_repos):
    logging.info("Merging repositories from website and GitHub...")
    # Convert URLs into repo names for easier comparison
    github_repo_names = [repo.split('/')[-1] for repo in github_repos]
    merged_repos = list(set(website_repos + github_repo_names))
    logging.info(f"Total merged repositories: {len(merged_repos)}")
    
    # Save merged repos data to JSON file
    with open("apache_repos_merged.json", "w") as json_file:
        json.dump(merged_repos, json_file, indent=4)
    
    return merged_repos

def merge_repos_temp():
    return merge_repositories

def fetch_available_mailing_lists():
    logging.info("Fetching available mailing lists...")
    base_url = "https://mail-archives.apache.org/mod_mbox/"
    try:
        response = requests.get(base_url)
        if response.status_code != 200:
            logging.error(f"Error fetching mailing lists: {response.status_code}")
            return []
        soup = BeautifulSoup(response.content, 'html.parser')
        mailing_lists = [a['href'].strip('/') for a in soup.find_all('a', href=True) if a['href'] != '../']
        logging.info(f"Found {len(mailing_lists)} available mailing lists.")
        return mailing_lists
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        return []

async def fetch(url, session):
    try:
        async with session.get(url) as response:
            if response.status != 200:
                logging.error(f"Error fetching URL {url}: {response.status}")
                return None
            return await response.text()
    except aiohttp.ClientError as e:
        logging.error(f"Request failed for URL {url}: {e}")
        return None

async def process_repository(session, repo_name, base_url, mailing_data):
    logging.info(f"Processing repository: {repo_name}")
    list_url = base_url + repo_name + "/"
    mailing_data[repo_name] = {}

    html_content = await fetch(list_url, session)
    if not html_content:
        logging.warning(f"No content fetched for repository: {repo_name}")
        return

    soup = BeautifulSoup(html_content, 'html.parser')
    # Only select links with valid href attributes
    archives = [a for a in soup.find_all('a', href=True) if not a['href'].startswith('javascript:')]

    tasks = [process_archive(session, repo_name, archive, list_url, mailing_data) for archive in archives]
    await asyncio.gather(*tasks)

async def process_archive(session, repo_name, archive, list_url, mailing_data):
    archive_name = archive.text.strip('/')
    archive_href = archive['href']

    # Skip invalid links
    if not archive_href or archive_href.startswith('javascript:'):
        return

    archive_url = list_url + archive_href
    mailing_data[repo_name][archive_name] = {
        'emails_count': 0,
        'senders': {},
        'receivers': {},
        'subjects': {}
    }

    html_content = await fetch(archive_url, session)
    if not html_content:
        logging.warning(f"No content fetched for archive: {archive_name} in repository: {repo_name}")
        return

    soup = BeautifulSoup(html_content, 'html.parser')
    emails = soup.find_all('a', href=True)

    for email in emails:
        if 'thread' not in email['href'] or email['href'].startswith('javascript:'):
            continue
        sender_info = email.text.strip()
        if '(' in sender_info and ')' in sender_info:
            sender_name = sender_info.split('(')[-1].strip(')')
            email_subject = sender_info.split('(')[0].strip()
        else:
            sender_name = "Unknown"
            email_subject = sender_info

        mailing_data[repo_name][archive_name]['emails_count'] += 1
        mailing_data[repo_name][archive_name]['subjects'][email_subject] = mailing_data[repo_name][archive_name]['subjects'].get(email_subject, 0) + 1
        mailing_data[repo_name][archive_name]['senders'][sender_name] = mailing_data[repo_name][archive_name]['senders'].get(sender_name, 0) + 1

async def _fetch_apache_mailing_list_data():
    base_url = "https://mail-archives.apache.org/mod_mbox/"
    mailing_data = {}

    # Fetch Apache repositories from both sources
    # For testing, we'll use two repositories
    website_repos = ['lucene', 'hadoop']
    
    logging.info("Using repositories: lucene, hadoop")

    # Fetch available mailing lists
    available_mailing_lists = fetch_available_mailing_lists()
    logging.debug(f"Available mailing lists: {available_mailing_lists}")

    # Filter repositories to only those that have mailing lists
    repos_with_mailing_lists = [repo for repo in website_repos if repo in available_mailing_lists]
    logging.info(f"Repositories with mailing lists: {repos_with_mailing_lists}")

    if not repos_with_mailing_lists:
        logging.error("No repositories with mailing lists found. Exiting.")
        return mailing_data

    async with aiohttp.ClientSession() as session:
        tasks = [process_repository(session, repo_name, base_url, mailing_data) for repo_name in repos_with_mailing_lists]
        await asyncio.gather(*tasks)

    # Save final mailing list data to JSON file
    with open("apache_mailing_list_data.json", "w") as json_file:
        json.dump(mailing_data, json_file, indent=4)

    logging.info("Mailing list data fetched successfully.")
    return mailing_data

def fetch_apache_mailing_list_data():
    return asyncio.run(_fetch_apache_mailing_list_data())
