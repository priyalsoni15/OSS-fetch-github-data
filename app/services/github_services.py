import requests
import logging
from app.config import Config
from app.utils.rate_limit import handle_rate_limit

def fetch_repos_service():
    try:
        api_url = f"https://api.github.com/orgs/{Config.ORG_NAME}/repos"
        headers = {'Authorization': f'token {Config.GITHUB_TOKEN}'}
        params = {'per_page': 100, 'page': 1}
        all_repos = []

        while True:
            response = requests.get(api_url, headers=headers, params=params)
            handle_rate_limit(response.headers)

            if response.status_code == 200:
                repos = response.json()
                if not repos:
                    break
                for repo_data in repos:
                    name = repo_data['name']
                    owner = repo_data['owner']['login']
                    url = repo_data['html_url']

                    # # Check if repository already exists
                    # repo = Repository.query.filter_by(name=name).first()
                    # if not repo:
                    #     repo = Repository(name=name, owner=owner, url=url)
                    #     db.session.add(repo)
                    #     db.session.commit()

                    all_repos.append({'name': name, 'owner': owner, 'url': url})

                params['page'] += 1
            else:
                logging.error(f"GitHub API Error: {response.status_code} - {response.text}")
                break

        return all_repos

    except Exception as e:
        logging.error(f"Error fetching repositories: {str(e)}")
        raise
