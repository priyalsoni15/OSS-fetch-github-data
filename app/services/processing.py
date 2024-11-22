import re
import math
from datetime import datetime
from pymongo import MongoClient
from app.config import Config

# Initialize MongoDB client
mongo_client = MongoClient(Config.MONGODB_URI)
db = mongo_client[Config.MONGODB_DB_NAME]

def process_sankey_data_all(project_name):
    """
    Processes the data for a given project to generate nodes and links for the Sankey diagram,
    including date information for filtering on the frontend.
    """
    commit_data_doc = db.commit_data.find_one({'repo_name': project_name})
    if not commit_data_doc:
        return None

    data = commit_data_doc.get('data', {})
    if not isinstance(data, dict):
        return None

    nodes = []
    links = []
    node_map = {}
    current_node_id = 0

    # Keep track of all dates
    all_dates = set()

    for year, months in data.items():
        if not isinstance(months, dict):
            continue

        for month, details in months.items():
            if not isinstance(details, dict):
                continue

            # Construct the date string
            current_date_str = f"{year}-{month}"
            all_dates.add(current_date_str)

            committers = details.get('committers', {})
            if not isinstance(committers, dict):
                continue

            for committer, comm_data in committers.items():
                if not isinstance(comm_data, dict):
                    continue

                # Standardize committer name
                committer_name = committer.strip()

                # Add committer node if not already added
                if committer_name not in node_map:
                    node_map[committer_name] = current_node_id
                    nodes.append({"name": committer_name})
                    current_node_id += 1

                extensions = comm_data.get('extensions', [])
                if not isinstance(extensions, list):
                    extensions = []

                num_extensions = len(extensions) or 1  # Avoid division by zero
                commits = comm_data.get('commits', 0)
                if not isinstance(commits, (int, float)):
                    commits = 0

                for ext in extensions:
                    # Standardize extension name
                    ext_name = str(ext).strip()

                    # Add extension node if not already added
                    if ext_name not in node_map:
                        node_map[ext_name] = current_node_id
                        nodes.append({"name": ext_name})
                        current_node_id += 1

                    # Calculate weight for the link
                    weight = commits / num_extensions

                    # Create the link from committer to file extension with date
                    links.append({
                        "source": node_map[committer_name],
                        "target": node_map[ext_name],
                        "value": weight,
                        "date": current_date_str  # Include date information
                    })

    # Sort dates
    sorted_dates = sorted(list(all_dates), key=lambda x: datetime.strptime(x, "%Y-%B"))

    return {"nodes": nodes, "links": links, "dates": sorted_dates}

# Sanitize the project name to allow only alphanumeric characters, dashes, and underscores.
def sanitize_project_name(project_name):
    return re.sub(r'[^\w\-]', '', project_name)

# This is to fetch the data below the technical network graph, such as the number of commits, committers, commits per committers
def fetch_commit_data_service(project_name):
    try:
        commit_data_doc = db.commit_data.find_one({'repo_name': project_name})
        if not commit_data_doc:
            raise FileNotFoundError(f"Commit data for project '{project_name}' not found.")

        commit_data = commit_data_doc.get('data', {})

        # Extract necessary information
        output = []
        for year, months in commit_data.items():
            for month, data in months.items():
                month_data = {
                    "year": year,
                    "month": month,
                    "total_commits": data.get('commits', 0),
                    "committers": []
                }

                for committer, committer_data in data.get('committers', {}).items():
                    # Skip bot committers
                    if committer.endswith("[bot]"):
                        continue
                    month_data["committers"].append({
                        "name": committer,
                        "commits": committer_data.get('commits', 0)
                    })

                month_data["total_committers"] = len(month_data["committers"])
                if month_data["total_committers"] > 0:
                    month_data["commits_per_committer"] = math.ceil(month_data["total_commits"] / month_data["total_committers"])
                else:
                    month_data["commits_per_committer"] = 0

                output.append(month_data)

        return output
    except FileNotFoundError:
        return {"error": "File not found."}
    except Exception:
        return {"error": "Internal server error."}
