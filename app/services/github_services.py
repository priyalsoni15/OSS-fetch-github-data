import requests
import logging
from app.config import Config
from itertools import cycle
from pymongo import MongoClient

# Initialize MongoDB client
mongo_client = MongoClient(Config.MONGODB_URI)
db = mongo_client[Config.MONGODB_DB_NAME]

def fetch_repos_service():
    try:
        api_url = "https://api.github.com/orgs/apache/repos"
        tokens = Config.GITHUB_TOKENS or [Config.GITHUB_TOKEN]
        if not tokens or tokens == [None]:
            logging.error("No GitHub tokens found.")
            return []

        token_cycle = cycle(tokens)
        headers = {'Authorization': f'token {next(token_cycle)}'}
        params = {'per_page': 100, 'page': 1}
        all_repos = []

        while True:
            response = requests.get(api_url, headers=headers, params=params)

            if response.status_code == 401 or response.status_code == 403:
                try:
                    headers['Authorization'] = f'token {next(token_cycle)}'
                    logging.warning("Rotated to the next token due to unauthorized or rate limit error.")
                    continue
                except StopIteration:
                    logging.error("All GitHub tokens have been exhausted.")
                    break

            if response.status_code == 200:
                repos = response.json()
                if not repos:
                    break
                for repo_data in repos:
                    repo_info = {
                        'name': repo_data.get('name'),
                        'owner': repo_data.get('owner', {}).get('login'),
                        'url': repo_data.get('html_url'),
                        'watchers_count': repo_data.get('watchers_count', 0),
                        'forks_count': repo_data.get('forks_count', 0),
                        'stargazers_count': repo_data.get('stargazers_count', 0)
                    }
                    all_repos.append(repo_info)

                params['page'] += 1
            else:
                logging.error(f"GitHub API Error: {response.status_code} - {response.text}")
                break

        # Save repos data to MongoDB
        if all_repos:
            try:
                db.github_repositories.drop()
                db.github_repositories.insert_many(all_repos)
                logging.info("Repositories data saved to MongoDB collection 'github_repositories'.")
            except Exception as e:
                logging.error(f"Error saving repositories to MongoDB: {e}")
                return []

        return all_repos

    except Exception as e:
        logging.error(f"Error fetching repositories: {str(e)}")
        raise
