import os
import json
import re
from datetime import datetime
import math

def extract_number(input_string):
    res = re.findall(r'\d+', input_string)
    return res[0] if res else None

def extract_name(ans1):
    regex = re.compile('[^a-zA-Z]')
    a = regex.sub('', ans1.split('_')[0])
    return a

def make_the_nested_list(df):
    df_list = df['content'].tolist()
    result = []
    for i in df_list:
        myorder = [1, 0, 2]
        mylist = i.split('##', 3)
        if len(mylist) < 3:
            continue
        mylist = [mylist[i] for i in myorder]
        result.append(mylist)
    return result

def process_sankey_data_all(project_name, data_dir):
    """
    Processes the data for a given project to generate nodes and links for the Sankey diagram,
    including date information for filtering on the frontend.
    """
    data_path = os.path.join(data_dir, f"{project_name}.json")
    if not os.path.exists(data_path):
        return None

    try:
        with open(data_path, 'r') as f:
            json_data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON for project {project_name}: {e}")
        return None

    data = json_data.get('data', {})
    if not isinstance(data, dict):
        print(f"'data' key is missing or not a dictionary in project {project_name}")
        return None

    nodes = []
    links = []
    node_map = {}
    current_node_id = 0

    # Keep track of all dates
    all_dates = set()

    for year, months in data.items():
        if not isinstance(months, dict):
            print(f"Invalid data format for months in year {year} of project {project_name}")
            continue

        for month, details in months.items():
            if not isinstance(details, dict):
                print(f"Invalid data format for details in month {month} of project {project_name}")
                continue

            # Construct the date string
            current_date_str = f"{year}-{month}"
            all_dates.add(current_date_str)

            committers = details.get('committers', {})
            if not isinstance(committers, dict):
                print(f"'committers' key is missing or not a dictionary in month {month} of project {project_name}")
                continue

            for committer, comm_data in committers.items():
                if not isinstance(comm_data, dict):
                    print(f"Invalid data format for committer data of {committer} in project {project_name}")
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
                    print(f"'extensions' key is missing or not a list for committer {committer_name} in project {project_name}")
                    extensions = []

                num_extensions = len(extensions) or 1  # Avoid division by zero
                commits = comm_data.get('commits', 0)
                if not isinstance(commits, (int, float)):
                    print(f"'commits' key is not a number for committer {committer_name} in project {project_name}")
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
def get_commit_statistics(project_name, data_dir):
   
   try:
        # Construct the path to the project's JSON file
        json_file_path = os.path.join(data_dir, f"{project_name}.json")
        
        if not os.path.exists(json_file_path):
            return {"error": f"Commit data for project '{project_name}' not found."}
        
        with open(project_name, 'r') as f:
            commit_data = json.load(f)
        
        aggregated_data = {}
        
        # Iterate through each year and month
        for year, months in commit_data.get('data', {}).items():
            if year not in aggregated_data:
                aggregated_data[year] = {}
            
            for month, details in months.items():
                if month not in aggregated_data[year]:
                    aggregated_data[year][month] = {
                        "total_commits": 0,
                        "committers": {}
                    }
                
                # Update total commits for the month
                aggregated_data[year][month]["total_commits"] += details.get("commits", 0)
                
                # Update committers data
                for committer, committer_details in details.get("committers", {}).items():
                    if committer not in aggregated_data[year][month]["committers"]:
                        aggregated_data[year][month]["committers"][committer] = {
                            "commit_count": 0
                        }
                    
                    aggregated_data[year][month]["committers"][committer]["commit_count"] += committer_details.get("commits", 0)
        
        # Optionally, include fetch metadata
        response = {
            "fetch_time_seconds": commit_data.get("fetch_time_seconds", 0),
            "api_calls_made": commit_data.get("api_calls_made", 0),
            "aggregated_data": aggregated_data
        }
        
        return response
   except Exception:
    return {"error": "Internal server error."}


# This is the new function for fetching commit data after technical network 
def fetch_commit_data_service(project_name):
    try:
        # Load data from existing project-specific JSON file
        file_path = os.path.join(os.getcwd(), f'out/apache/github/{project_name}.json')
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Commit data file for project '{project_name}' not found.")

        with open(file_path, 'r') as file:
            commit_data = json.load(file)

        # Extract necessary information
        output = []
        for year, months in commit_data.get('data', {}).items():
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

        # Save the output data to a new JSON file
        output_file_path = os.path.join(os.getcwd(), f'out/apache/github/tn_data/{project_name}_commit_stats.json')
        with open(output_file_path, 'w') as output_file:
            json.dump(output, output_file, indent=4)

        return output
    except FileNotFoundError:
        return {"error": "File not found."}
    except Exception:
        return {"error": "Internal server error."}