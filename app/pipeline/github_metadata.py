import requests
import logging
import itertools
from urllib.parse import urlparse
from app.config import Config  # Import the Config class

# Create a cycle iterator over available tokens to rotate them
token_cycle = itertools.cycle(Config.GITHUB_TOKENS) if Config.GITHUB_TOKENS else None

def get_github_metadata(repo_url):
    """Fetches repository metadata from GitHub API, using rotating tokens to avoid rate limits.
    
    Now includes:
    - Programming languages
    - Latest release info (if available)
    - Fallback for empty topics
    """

    try:
        if not token_cycle:
            logging.error("No GitHub tokens found in Config!")
            return {"error": "No GitHub tokens available"}

        # Ensure it's a valid GitHub repository URL
        parsed_url = urlparse(repo_url)
        path_parts = parsed_url.path.strip("/").split("/")
        
        if len(path_parts) < 2:
            logging.error(f"Invalid GitHub repository URL: {repo_url}")
            return {"error": "Invalid GitHub repository URL"}

        # Use the exact provided URL, just remove `.git` if present
        clean_repo_url = repo_url.rstrip(".git")
        api_base_url = clean_repo_url.replace("github.com", "api.github.com/repos")

        # Rotate tokens to distribute requests
        token = next(token_cycle)
        
        headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json"
        }

        # Fetch main repo data
        response = requests.get(api_base_url, headers=headers)

        # Handle API rate limits by switching tokens
        if response.status_code == 403:
            logging.warning(f"Rate limit exceeded with token: {token}, trying next token...")
            for _ in range(len(Config.GITHUB_TOKENS)):  # Try all available tokens
                token = next(token_cycle)
                headers["Authorization"] = f"token {token}"
                response = requests.get(api_base_url, headers=headers)
                if response.status_code != 403:  # If new token works, break the loop
                    break

        # If still not successful, return an error
        if response.status_code != 200:
            logging.error(f"GitHub API error: {response.status_code} - {response.text}")
            return {"error": f"GitHub API error: {response.status_code}"}

        repo_data = response.json()

        # Fetch languages used in the repository
        languages_response = requests.get(f"{api_base_url}/languages", headers=headers)
        languages = list(languages_response.json().keys()) if languages_response.status_code == 200 else []

        # Fetch latest release info
        releases_response = requests.get(f"{api_base_url}/releases/latest", headers=headers)
        if releases_response.status_code == 200:
            release_data = releases_response.json()
            latest_release = {
                "tag": release_data.get("tag_name"),
                "name": release_data.get("name"),
                "published_at": release_data.get("published_at"),
            }
        else:
            latest_release = "No releases available"

        # Extract relevant metadata
        metadata = {
            "name": repo_data.get("name"),
            "owner": repo_data.get("owner", {}).get("login"),
            "description": repo_data.get("description") or "No description provided",
            "stars": repo_data.get("stargazers_count", 0),
            "watchers": repo_data.get("watchers_count", 0),
            "forks": repo_data.get("forks_count", 0),
            "license": repo_data.get("license", {}).get("name", "No license"),
            "created_at": repo_data.get("created_at"),  # Start date of the repository
            "updated_at": repo_data.get("updated_at"),
            "open_issues": repo_data.get("open_issues_count", 0),
            "languages": languages,  # Programming languages used
            "latest_release": latest_release  # Latest release info
        }

        return metadata

    except requests.exceptions.RequestException as e:
        logging.error(f"Request error: {e}")
        return {"error": "Failed to connect to GitHub API"}

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return {"error": f"Unexpected error: {str(e)}"}
