import logging
import requests
from bs4 import BeautifulSoup
import time
import json
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Define output directory
DATA_DIR = os.path.join(os.getcwd(), 'out', 'eclipse', 'website')
os.makedirs(DATA_DIR, exist_ok=True)

def make_request_with_backoff(url, max_attempts=5):
    attempt = 0
    delay = 1
    while attempt < max_attempts:
        try:
            logger.info(f"Requesting URL: {url}")
            response = requests.get(url, timeout=10)
            if response.status_code // 100 == 2:
                return response
            else:
                logger.warning(f"Request failed with status code {response.status_code}. Retrying...")
                raise Exception("Request failed")
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(delay)
            attempt += 1
            delay *= 2
    raise Exception("Max attempts reached, request failed.")

def scrape_additional_info(url):
    try:
        response = make_request_with_backoff(url)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Extract Technology
        tech = soup.find("li", class_="ellipsis hierarchy-1")
        technology_1 = tech.find("a").text if tech else "N/A"
        technology = technology_1.replace("Eclipse", "").replace("Project", "").replace("®", "").strip() if technology_1 != "Eclipse Project" else technology_1

        # Extract State
        state_div = soup.find("div", class_="field-name-field-state")
        state = state_div.find("div", class_="field-item").text.strip() if state_div else "N/A"

        # Extract Releases or Reviews
        releases_url = url + "/governance"
        response = make_request_with_backoff(releases_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        data = []

        releases_div = soup.find("div", class_="field-name-field-releases")
        if releases_div:
            for row in releases_div.find_all("tr")[1:]:
                cols = row.find_all("td")
                release_name = cols[0].text.strip()
                release_url = "https://projects.eclipse.org" + cols[0].find("a")["href"].strip()
                release_date = cols[1].text.strip()
                data.append({"name": release_name, "url": release_url, "date": release_date})
        else:
            reviews_div = soup.find("div", class_="field-name-field-project-reviews")
            if reviews_div:
                for row in reviews_div.find_all("tr")[1:]:
                    cols = row.find_all("td")
                    review_name = cols[0].text.strip()
                    review_url = "https://projects.eclipse.org" + cols[0].find("a")["href"].strip()
                    review_date = cols[1].text.strip()
                    data.append({"name": review_name, "url": review_url, "date": review_date})

        # Extract mailing list
        mailing_list_url = url + "/developer"
        response = make_request_with_backoff(mailing_list_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        mailing_list_links = soup.select('a[href*="mailman/listinfo"], a[href*="mailing-list"]')
        mailing_list_name = mailing_list_links[0].get("href").split('/')[-1] if mailing_list_links else "N/A"

        # Extract GitHub repositories
        github_reposs_str = "N/A"
        github_section = soup.find('div', class_='field-name-field-project-github-org') or soup.find('div', class_='field-name-field-project-github-repos')
        if github_section:
            github_reposs = github_section.select('a[href*="github.com"]')
            github_reposs_str = ", ".join(link.get('href') for link in github_reposs).replace('https://github.com/', '')

        return {
            "technology": technology,
            "state": state,
            "github_repositories": github_reposs_str,
            "releases_or_reviews": data,
            "mailing_list_name": mailing_list_name
        }
    except Exception as e:
        logger.error(f"Error while scraping additional info for {url}: {e}")
        return {
            "technology": "N/A",
            "state": "N/A",
            "github_repositories": "N/A",
            "releases_or_reviews": [],
            "mailing_list_name": "N/A"
        }

def scrape_projects(base_url, total_pages):
    all_projects = []
    for page in range(total_pages):
        try:
            if page == 0:
                url = base_url
            else:
                url = f"{base_url}&page={page}"
            
            response = make_request_with_backoff(url)
            soup = BeautifulSoup(response.text, 'html.parser')

            for project_div in soup.find_all("div", class_="project-teaser-body"):
                project_name = project_div.find("h4").text.replace('™', '').replace('®', '').replace('Eclipse ', '').replace('Jakarta ', '').replace('LocationTech ', '').strip()
                project_url = "https://projects.eclipse.org" + project_div.find("a")["href"]
                additional_info = scrape_additional_info(project_url)
                project_data = {
                    "name": project_name,
                    "url": project_url,
                    **additional_info
                }
                all_projects.append(project_data)

            logger.info(f"Scraped all projects from page {page}")
        except Exception as e:
            logger.error(f"Error while scraping page {page}: {e}")
    
    # Save to JSON file
    output_file = os.path.join(DATA_DIR, "eclipse_projects.json")
    with open(output_file, "w") as f:
        json.dump(all_projects, f, indent=4)
    logger.info(f"All projects saved to {output_file}")

if __name__ == "__main__":
    base_url = "https://projects.eclipse.org/list-of-projects?combine=&field_project_techology_types_tid=All&field_state_value_2=All&field_archived_projects%5Barchived%5D=archived"
    total_pages = 32
    logger.info("Starting to scrape Project Data")
    scrape_projects(base_url, total_pages)
