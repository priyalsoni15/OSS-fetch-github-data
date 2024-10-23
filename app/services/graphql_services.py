import asyncio
import aiohttp
import requests
import logging
import time
import json
import random
import os
from datetime import datetime
from itertools import cycle
from app.config import Config

def fetch_commits_for_repo(repo):
    try:
        tokens = Config.GITHUB_TOKENS
        if not tokens:
            logging.error("No GitHub tokens found. Please set GITHUB_TOKEN_1, GITHUB_TOKEN_2, etc., in your environment variables.")
            return {}, 0, 0

        logging.info(f"Using tokens: {tokens}")
        token_index = 0

        headers = {"Authorization": f"Bearer {tokens[token_index]}"}
        has_next_page = True
        data = {}
        api_calls = 0
        start_time = time.time()

        # GraphQL query to fetch commit SHAs, authors, and dates
        query = """
        query($owner: String!, $name: String!, $cursor: String) {
          repository(owner: $owner, name: $name) {
            defaultBranchRef {
              target {
                ... on Commit {
                  history(first: 100, after: $cursor) {
                    pageInfo {
                      hasNextPage
                      endCursor
                    }
                    edges {
                      node {
                        committedDate
                        author {
                          name
                        }
                        oid  # Commit SHA
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """
        variables = {"owner": repo.owner, "name": repo.name, "cursor": None}

        commit_shas = []

        while has_next_page:
            try:
                logging.debug(f"Fetching commits with token index {token_index}")
                response = requests.post(
                    'https://api.github.com/graphql',
                    json={"query": query, "variables": variables},
                    headers=headers
                )
                api_calls += 1

                logging.debug(f"Response Status Code: {response.status_code}")
                logging.debug(f"Response Headers: {response.headers}")
                logging.debug(f"Response Text: {response.text}")

                if response.status_code == 401:
                    # Rotate to the next token
                    token_index = (token_index + 1) % len(tokens)
                    headers["Authorization"] = f"Bearer {tokens[token_index]}"
                    logging.warning("Rotated to the next token due to unauthorized error.")
                    continue

                if response.status_code != 200:
                    logging.error(f"Error fetching data for {repo.name}: {response.status_code} {response.text}")
                    break

                result = response.json()

                if 'errors' in result:
                    logging.error(f"GraphQL errors: {result['errors']}")
                    break

                repository = result.get('data', {}).get('repository')
                if not repository:
                    logging.error(f"No repository data returned for {repo.name}")
                    break

                history = repository['defaultBranchRef']['target']['history']
                edges = history['edges']
                has_next_page = history['pageInfo']['hasNextPage']
                variables['cursor'] = history['pageInfo']['endCursor']

                for edge in edges:
                    commit = edge['node']
                    commit_date = commit['committedDate']
                    commit_sha = commit['oid']
                    committer_name = commit['author']['name'] if commit['author'] else 'Unknown'
                    commit_datetime = datetime.strptime(commit_date, '%Y-%m-%dT%H:%M:%SZ')
                    year = commit_datetime.strftime('%Y')
                    month = commit_datetime.strftime('%B')

                    if year not in data:
                        data[year] = {}
                    if month not in data[year]:
                        data[year][month] = {'commits': 0, 'committers': {}}

                    # Update commits per month
                    data[year][month]['commits'] += 1

                    # Update committer data
                    if committer_name not in data[year][month]['committers']:
                        data[year][month]['committers'][committer_name] = {
                            'commits': 0,
                            'extensions': set()
                        }

                    data[year][month]['committers'][committer_name]['commits'] += 1

                    # Collect commit SHA for REST API call
                    commit_shas.append((commit_sha, committer_name, year, month))

                # Save partial data
                save_partial_data(data, api_calls, start_time, repo.name)

                # Optional: Limit the number of commits processed per repo
                # Uncomment the following lines to limit to, e.g., 50 commits
                # if len(commit_shas) >= 50:
                #     has_next_page = False

                # Check rate limit after GraphQL API call
                rate_limit_remaining = int(response.headers.get('X-RateLimit-Remaining', 1))
                rate_limit_reset = int(response.headers.get('X-RateLimit-Reset', time.time() + 60))

                if rate_limit_remaining == 0:
                    sleep_time = max(rate_limit_reset - int(time.time()), 0)
                    logging.info(f"Rate limit reached. Sleeping for {sleep_time} seconds.")
                    time.sleep(sleep_time)
                    api_calls = 0  # Reset API call count after waiting

            except Exception as e:
                logging.error(f"Request failed: {e}. Retrying...")
                logging.exception("Exception details:")
                time.sleep(random.uniform(1, 3))
                continue

        # Ensure we have commit SHAs to process
        if not commit_shas:
            logging.error(f"No commits found for repository {repo.name}.")
            return {}, time.time() - start_time, api_calls

        # Use asynchronous requests to fetch commit details
        asyncio.run(fetch_commit_details_async(commit_shas, data, tokens, repo, api_calls))

        end_time = time.time()
        total_time = end_time - start_time

        # Convert sets to lists for JSON serialization
        data = convert_sets_to_lists(data)

        return data, total_time, api_calls

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        logging.exception("Exception details:")
        return {}, 0, 0

async def fetch_commit_details_async(commit_shas, data, tokens, repo, api_calls):
    semaphore = asyncio.Semaphore(10)  # Limit concurrent connections
    token_cycle = cycle(tokens)
    api_calls_counter = api_calls  # Initialize with current API calls count

    async with aiohttp.ClientSession() as session:
        tasks = []
        for sha_info in commit_shas:
            task = fetch_commit_detail(session, sha_info, data, token_cycle, semaphore, repo)
            tasks.append(task)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        # Count API calls from results
        api_calls_counter += len(tasks)

def get_next_token(token_cycle):
    return next(token_cycle)

async def fetch_commit_detail(session, sha_info, data, token_cycle, semaphore, repo):
    async with semaphore:
        commit_sha, committer_name, year, month = sha_info
        headers = {"Authorization": f"Bearer {get_next_token(token_cycle)}"}
        commit_url = f"https://api.github.com/repos/{repo.owner}/{repo.name}/commits/{commit_sha}"

        while True:
            try:
                logging.debug(f"Fetching commit details for {commit_sha}")
                async with session.get(commit_url, headers=headers) as response:
                    if response.status == 401 or response.status == 403:
                        # Rotate to the next token
                        headers["Authorization"] = f"Bearer {get_next_token(token_cycle)}"
                        logging.warning(f"Rotated to the next token for commit {commit_sha}.")
                        continue

                    if response.status != 200:
                        text = await response.text()
                        logging.error(f"Error fetching commit {commit_sha} details: {response.status} {text}")
                        return

                    commit_data = await response.json()

                    # Get file extensions
                    files = commit_data.get('files', [])
                    for file in files:
                        filename = file['filename']
                        # Extract file extension
                        if '.' in filename:
                            extension = filename.rsplit('.', 1)[-1].lower()
                        else:
                            extension = ''
                        data[year][month]['committers'][committer_name]['extensions'].add(extension)
                    break
            except Exception as e:
                logging.error(f"Exception occurred while fetching commit {commit_sha}: {e}")
                logging.exception("Exception details:")
                break

def save_partial_data(data, api_calls, start_time, repo_name):
    # Convert sets to lists for JSON serialization
    data_serializable = convert_sets_to_lists(data)

    partial_data = {
        "fetch_time_seconds": time.time() - start_time,
        "api_calls_made": api_calls,
        "data": data_serializable
    }
    # Save to JSON file
    partial_dir = os.path.join('out', 'apache', 'partial')
    os.makedirs(partial_dir, exist_ok=True)
    with open(os.path.join(partial_dir, f"github_data_partial_{repo_name}.json"), "w") as json_file:
        json.dump(partial_data, json_file, indent=4)

def convert_sets_to_lists(obj):
    if isinstance(obj, dict):
        return {k: convert_sets_to_lists(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_sets_to_lists(element) for element in obj]
    elif isinstance(obj, set):
        return list(obj)
    else:
        return obj

def fetch_commits_service():
    repos = Config.REPOSITORIES
    for repo_uri in repos:
        repo_owner, repo_name = repo_uri.split('/')[-2], repo_uri.split('/')[-1].replace('.git', '')
        repo = type('Repo', (object,), {'owner': repo_owner, 'name': repo_name})()
        data, total_time, api_calls = fetch_commits_for_repo(repo)
        output = {
            "fetch_time_seconds": total_time,
            "api_calls_made": api_calls,
            "data": data
        }
        # Save final data to JSON file
        output_dir = os.path.join('out', 'apache')
        os.makedirs(output_dir, exist_ok=True)
        with open(os.path.join(output_dir, f"github_data_{repo.name}.json"), "w") as json_file:
            json.dump(output, json_file, indent=4)
    return "Data fetched for specified repositories."

# Fetch all repos for a particular foundation, for instance Apache, Eclipse or Linux Foundation
def fetch_repos_for_org():
    tokens = Config.GITHUB_TOKENS
    if not tokens:
        logging.error("No GitHub tokens found. Please set GITHUB_TOKEN_1, GITHUB_TOKEN_2, etc., in your environment variables.")
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
    start_time = time.time()
    api_calls = 0

    while has_next_page:
        try:
            response = requests.post(
                'https://api.github.com/graphql',
                json={"query": query, "variables": variables},
                headers=headers
            )
            api_calls += 1

            if response.status_code == 401:
                # Rotate to the next token
                headers["Authorization"] = f"Bearer {next(token_cycle)}"
                logging.warning("Rotated to the next token due to unauthorized error.")
                continue

            if response.status_code != 200:
                logging.error(f"Error fetching repositories: {response.status_code} {response.text}")
                break

            result = response.json()

            if 'errors' in result:
                logging.error(f"GraphQL errors: {result['errors']}")
                break

            organization = result.get('data', {}).get('organization')
            if not organization:
                break

            repositories = organization['repositories']
            has_next_page = repositories['pageInfo']['hasNextPage']
            variables['cursor'] = repositories['pageInfo']['endCursor']

            for repo in repositories['nodes']:
                repos.append({"name": repo['name'], "url": repo['url']})

            # Save partial data
            save_partial_repo_data(repos, api_calls, start_time)

            # Check rate limit after GraphQL API call
            rate_limit_remaining = int(response.headers.get('X-RateLimit-Remaining', 1))
            rate_limit_reset = int(response.headers.get('X-RateLimit-Reset', time.time() + 60))

            if rate_limit_remaining == 0:
                sleep_time = max(rate_limit_reset - int(time.time()), 0)
                logging.info(f"Rate limit reached. Sleeping for {sleep_time} seconds.")
                time.sleep(sleep_time)
                api_calls = 0  # Reset API call count after waiting

        except Exception as e:
            logging.error(f"Request failed: {e}. Retrying...")
            logging.exception("Exception details:")
            time.sleep(random.uniform(1, 3))
            continue

    # Save repos data to JSON file
    output_dir = os.path.join('out', 'apache')
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, f"{Config.ORG_NAME}_repos.json"), "w") as json_file:
        json.dump(repos, json_file, indent=4)

    return repos

def save_partial_repo_data(repos, api_calls, start_time):
    partial_data = {
        "fetch_time_seconds": time.time() - start_time,
        "api_calls_made": api_calls,
        "repos": repos
    }
    # Save to JSON file
    partial_dir = os.path.join('out', 'apache', 'partial')
    os.makedirs(partial_dir, exist_ok=True)
    with open(os.path.join(partial_dir, f"{Config.ORG_NAME}_repos_partial.json"), "w") as json_file:
        json.dump(partial_data, json_file, indent=4)
