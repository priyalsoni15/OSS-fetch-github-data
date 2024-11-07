# processing.py

import os
import json
import re

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

def process_sankey_data(project_name, data_dir):
    """
    Processes the data for a given project to generate nodes and links for the Sankey diagram.

    Parameters:
    - project_name (str): The name of the project.
    - data_dir (str): The directory where the project JSON files are stored.

    Returns:
    - dict: A dictionary containing 'nodes' and 'links' for the Sankey diagram.
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

    # Access the 'data' key in the JSON
    data = json_data.get('data', {})
    if not isinstance(data, dict):
        print(f"'data' key is missing or not a dictionary in project {project_name}")
        return None

    nodes = []
    links = []
    node_map = {}
    current_node_id = 0

    for year, months in data.items():
        if not isinstance(months, dict):
            print(f"Invalid data format for months in year {year} of project {project_name}")
            continue

        for month, details in months.items():
            if not isinstance(details, dict):
                print(f"Invalid data format for details in month {month} of project {project_name}")
                continue

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

                    # Create the link from committer to file extension
                    links.append({
                        "source": node_map[committer_name],
                        "target": node_map[ext_name],
                        "value": weight
                    })

    return {"nodes": nodes, "links": links}
