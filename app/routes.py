from flask import Blueprint, jsonify, redirect, url_for
from app.services.graphql_services import fetch_commits_service
from app.services.apache_services import fetch_apache_mailing_list_data, fetch_apache_repositories_from_github, fetch_apache_projects_data 
import os, json

from app.services.processing import process_sankey_data_all

main_routes = Blueprint('main_routes', __name__)

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
def get_apache_projects():
    # Define the path to your projects data
    output_file = os.path.join(os.getcwd(), 'out', 'apache', 'projects', 'apache_projects.json')

    # If the file exists, read from it
    if os.path.exists(output_file):
        with open(output_file, 'r') as f:
            projects = json.load(f)
    else:
        # If the file doesn't exist, fetch the data
        projects = fetch_apache_projects_data()

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
    return redirect(url_for('main_routes.landing_page'))