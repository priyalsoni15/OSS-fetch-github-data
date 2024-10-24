from flask import Blueprint, jsonify, redirect, url_for
from app.services.graphql_services import fetch_commits_service, fetch_repos_for_org
from app.services.apache_services import fetch_apache_mailing_list_data, merge_repos_temp

main_routes = Blueprint('main_routes', __name__)

# [Tested] Homepage
@main_routes.route('/')
def landing_page():
    return "Welcome to the Apache Organization Repository Fetcher!"

# [Tested] This would fetch all the repos from the Apache organization
@main_routes.route('/fetch_repos', methods=['GET'])
def fetch_repos():
    repos = fetch_repos_for_org()
    return jsonify(repos), 200

# [Tested] This will fetch all the commits for a github repo
@main_routes.route('/fetch_commits', methods=['GET'])
def fetch_commits():
    message = fetch_commits_service()
    return jsonify({'message': message}), 200

# [Pending]
@main_routes.route('/merge_repos', methods=['GET'])
def fetch_merge_repositories():
    message = merge_repos_temp()
    return jsonify({'message': message}), 200

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