from flask import Blueprint, jsonify, redirect, url_for, current_app
from app.services.graphql_services import fetch_commits_service
from app.services.apache_services import fetch_apache_mailing_list_data, fetch_apache_repositories_from_github, fetch_all_podlings
from app.services.processing import fetch_commit_data_service, process_sankey_data_all, get_commit_statistics, sanitize_project_name
import os
import logging
import math

main_routes = Blueprint('main_routes', __name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# [Tested] Homepage
@main_routes.route('/')
def landing_page():
    return "Welcome to the Apache Organization Repository Fetcher!"

# [Tested] This would fetch all the repos from the Apache organization
@main_routes.route('/fetch_repos', methods=['GET'])
def fetch_repos():
    repos = fetch_apache_repositories_from_github()
    return jsonify(repos), 200

# [Tested] This will fetch all the commits for a github repo
@main_routes.route('/fetch_commits', methods=['GET'])
def fetch_commits():
    message = fetch_commits_service()
    return jsonify({'message': message}), 200

# [Tested] Create technical network for Apache projects [1 project each]
# Remember the limitation here is that the .json file should be present to be processed further
@main_routes.route('/api/tech_net/<project_name>', methods=['GET'])
def get_sankey_data(project_name):
    # Define the path to your data directory
    DATA_DIR = os.path.join('out', 'apache', 'github')  # Adjust the path as needed

    sankey_data = process_sankey_data_all(project_name, DATA_DIR)
    if sankey_data is None:
        return jsonify({'error': 'Project not found'}), 404
    return jsonify(sankey_data), 200

# This will fetch all the projects from Apache website
@main_routes.route('/api/projects', methods=['GET'])
def get_all_projects():
    projects = fetch_all_podlings()
    if not projects:
        return jsonify({'error': 'Failed to fetch Apache projects data'}), 500
    return jsonify({'projects': projects}), 200

# [Tested] This will fetch the mailing list data for Apache organization
# [Additional functionality] Currently, the repo list is manual, once this is complete, I want to fetch the repos from the json or stored files.
@main_routes.route('/fetch_mailing_list', methods=['GET'])
def fetch_mailing_list_apache():
    message = fetch_apache_mailing_list_data()
    return jsonify({'message': message}), 200

# [Tested] For any other API routes than the one mentioned, redirect it to the landing page/home-page
@main_routes.route('/<path:invalid_path>')
def handle_invalid_path(invalid_path):
    if invalid_path.startswith('api/'):
        return jsonify({'error': 'Invalid API endpoint'}), 404
    return redirect(url_for('main_routes.landing_page'))

# [Discarded] [New] Endpoint to fetch commit statistics for a specific project (technical network data)
@main_routes.route('/api/old/<project_name>', methods=['GET'])
def get_commit_statistics_endpoint(project_name):
    """
    Endpoint to fetch commit statistics for a specific project.

    URL Example: /api/tech_net/ProjectName
    """
    try:
        # Sanitize the project name
        project_name = sanitize_project_name(project_name)
        
        # Access DATA_DIR from the config
        DATA_DIR = current_app.config['DATA_DIR']

        # Call the service function to get commit statistics
        commit_stats = get_commit_statistics(project_name, DATA_DIR)
        
        if "error" in commit_stats:
            return jsonify(commit_stats), 404
        
        return jsonify(commit_stats), 200
    
    except Exception as e:
        logging.error(f"An error occurred in the endpoint for project '{project_name}': {e}")
        logging.exception("Exception details:")
        return jsonify({"error": "Internal server error."}), 500
    
@main_routes.route('/api/tech_net/other/<project_name>', methods=['GET'])
def fetch_commit_data(project_name):
    try:
        output = fetch_commit_data_service(project_name)
        # Add commits per committer calculation and total committers
        for month_data in output:
            # Filter out bot committers
            filtered_committers = [committer for committer in month_data["committers"] if not committer["name"].endswith("[bot]")]
            total_committers = len(filtered_committers)
            month_data["total_committers"] = total_committers
            if total_committers > 0:
                month_data["commits_per_committer"] = math.ceil(month_data["total_commits"] / total_committers)
            else:
                month_data["commits_per_committer"] = 0
            month_data["committers"] = filtered_committers
        return jsonify(output), 200
    except FileNotFoundError:
        return jsonify({"error": f"Commit data for project '{project_name}' not found."}), 404
    except Exception as e:
        logging.error(f"An error occurred while fetching commit data for project '{project_name}': {e}")
        return jsonify({"error": "An error occurred while fetching commit data."}), 500