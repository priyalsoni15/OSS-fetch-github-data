# src/routes.py

import math
from flask import Blueprint, jsonify, redirect, url_for
from flask_cors import cross_origin
from app.config import Config
from pymongo import MongoClient
from app.services.graphql_services import fetch_commits_service
from app.services.apache_services import fetch_all_podlings, fetch_apache_repositories_from_github
from app.services.processing import fetch_commit_data_service, process_sankey_data_all
from app.services.github_services import fetch_repos_service
import logging

main_routes = Blueprint('main_routes', __name__)

# Initialize MongoDB client
mongo_client = MongoClient(Config.MONGODB_URI)
db = mongo_client[Config.MONGODB_DB_NAME]

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# This is to prevent any error occurring because of NaN value - this is converted to null
def sanitize_document(doc):
    """
    Recursively sanitize the document by replacing NaN with None.
    """
    for key, value in doc.items():
        if isinstance(value, float) and math.isnan(value):
            doc[key] = None
        elif isinstance(value, dict):
            sanitize_document(value)
        elif isinstance(value, list):
            for idx, item in enumerate(value):
                if isinstance(item, dict):
                    sanitize_document(item)
                elif isinstance(item, float) and math.isnan(item):
                    value[idx] = None
    return doc

# Homepage
@main_routes.route('/')
@cross_origin(origin='*') 
def landing_page():
    return "Welcome to the Apache Organization Repository Fetcher!"

# Redirect invalid API endpoints
@main_routes.route('/<path:invalid_path>')
def handle_invalid_path(invalid_path):
    if invalid_path.startswith('api/'):
        return jsonify({'error': 'Invalid API endpoint'}), 404
    return redirect(url_for('main_routes.landing_page'))

# Fetch all the repos from the Apache organization
@main_routes.route('/fetch_repos', methods=['GET'])
def fetch_repos():
    fetch_apache_repositories_from_github()
    repos = list(db.github_repositories.find({}, {'_id': 0}))
    repos = [sanitize_document(repo) for repo in repos]
    return jsonify(repos), 200

# Fetch all the projects (combined from Apache and Github)
@main_routes.route('/api/projects', methods=['GET'])
@cross_origin(origin='*') 
def get_all_projects():
    try:
        projects = list(db.github_repositories.find({}, {'_id': 0}))
        projects = [sanitize_document(project) for project in projects]
        return jsonify({'projects': projects}), 200
    except Exception as e:
        logger.error(f"Error fetching projects from MongoDB: {e}")
        return jsonify({'error': 'Failed to fetch projects.'}), 500

# Fetch all github repositories - include fetching stars, forks and watch for each repo
@main_routes.route('/api/github_stars', methods=['GET'])
def get_github_stars():
    try:
        repos = list(db.github_repositories.find({}, {'_id': 0}))
        repos = [sanitize_document(repo) for repo in repos]
        return jsonify({'repositories': repos}), 200
    except Exception as e:
        logger.error(f"Error fetching repositories from MongoDB: {e}")
        return jsonify({'error': 'Failed to fetch repositories.'}), 500

# Fetch all the repos from GitHub
@main_routes.route('/api/github_repositories', methods=['GET'])
def get_github_repositories():
    try:
        repos = list(db.github_repositories.find({}, {'_id': 0}))
        repos = [sanitize_document(repo) for repo in repos]
        return jsonify({'repositories': repos}), 200
    except Exception as e:
        logger.error(f"Error fetching repositories from MongoDB: {e}")
        return jsonify({'error': 'Failed to fetch repositories.'}), 500

# [Tested] [Currently used by Vue.js] Fetch project descriptions from Apache scraping
@main_routes.route('/api/project_description', methods=['GET'])
@cross_origin(origin='*') 
def get_project_description():
    try:
        description = list(db.apache_projects.find({}, {'_id': 0}))
        description = [sanitize_document(doc) for doc in description]
        return jsonify({'description': description}), 200
    except Exception as e:
        logger.error(f"Error fetching project descriptions from MongoDB: {e}")
        return jsonify({'error': 'Failed to fetch project descriptions.'}), 500

# Fetch all project_info
@main_routes.route('/api/project_info', methods=['GET'])
@cross_origin(origin='*') 
def get_all_project_info():
    """
    Fetch all project information.
    """
    try:
        projects = list(db.project_info.find({}, {'_id': 0}))
        projects = [sanitize_document(project) for project in projects]
        return jsonify({'projects': projects}), 200
    except Exception as e:
        logger.error(f"Error fetching project_info from MongoDB: {e}")
        return jsonify({'error': 'Failed to fetch project information.'}), 500


# Fetch all month ranges for a project
@main_routes.route('/api/monthly_ranges', methods=['GET'])
@cross_origin(origin='*') 
def get_all_monthly_ranges():
    """
    Fetch all monthly ranges for all projects.
    """
    try:
        projects = list(db.monthly_ranges.find({}, {'_id': 0}))
        projects = [sanitize_document(project) for project in projects]
        return jsonify({'project_ranges': projects}), 200
    except Exception as e:
        logger.error(f"Error fetching project_ranges from MongoDB: {e}")


# ------------------ New API Endpoint: Tech Net Data ------------------

@main_routes.route('/api/tech_net/<project_id>/<int:month>', methods=['GET'])
@cross_origin(origin='*') 
def get_tech_net(project_id, month):
    """
    Fetch technical network data for a specific project and month.
    """
    try:
        normalized_project_id = project_id.strip().lower()
        project = db.tech_net.find_one({'project_id': normalized_project_id})
        if not project:
            return jsonify({'error': f"Project '{project_id}' not found."}), 404
        
        month_str = str(month)
        if 'months' not in project or month_str not in project['months']:
            return jsonify({'error': f"Month '{month}' data not found for project '{project_id}'."}), 404
        
        data = project['months'][month_str]
        # Sanitize data if necessary (assuming data is list of lists with [string, string, number])
        sanitized_data = []
        for entry in data:
            if isinstance(entry, list) and len(entry) == 3:
                name, tech, value = entry
                sanitized_entry = [
                    name if isinstance(name, str) else '',
                    tech if isinstance(tech, str) else '',
                    value if isinstance(value, (int, float)) else 0
                ]
                sanitized_data.append(sanitized_entry)
            else:
                # Handle unexpected data formats
                sanitized_data.append(['', '', 0])
        
        return jsonify({
            'project_id': project['project_id'],
            'project_name': project['project_name'],
            'month': month,
            'data': sanitized_data
        }), 200
    except Exception as e:
        logger.error(f"Error fetching tech_net data for project '{project_id}', month '{month}': {e}")
        return jsonify({'error': 'Internal server error.'}), 500

# This is to fetch the social network data for a specific project and month
@main_routes.route('/api/social_net/<project_id>/<int:month>', methods=['GET'])
@cross_origin(origin='*')
def get_social_net(project_id, month):
    """
    Fetch social network data for a specific project and month.
    """
    try:
        # Normalize project ID
        normalized_project_id = project_id.strip().lower()

        # Fetch project from the database
        project = db.social_net.find_one({'project_id': normalized_project_id})
        if not project:
            return jsonify({'error': f"Project '{project_id}' not found."}), 404

        # Convert the month parameter to string for key lookup
        month_str = str(month)

        # Check if the month exists in the project's "months" field
        if 'months' not in project or month_str not in project['months']:
            return jsonify({'error': f"Month '{month}' data not found for project '{project_id}'."}), 404

        # Fetch data for the specified month
        data = project['months'][month_str]

        # Sanitize the data
        sanitized_data = []
        for entry in data:
            if isinstance(entry, list) and len(entry) == 3:
                name, relation, value = entry

                # Convert the value field to an integer or float
                try:
                    value = int(value) if isinstance(value, str) and value.isdigit() else float(value)
                except ValueError:
                    logger.warning(f"Invalid value in entry: {entry}")
                    continue  # Skip this entry if value conversion fails

                sanitized_entry = [
                    name if isinstance(name, str) else '',
                    relation if isinstance(relation, str) else '',
                    value  # Use the converted numeric value
                ]
                sanitized_data.append(sanitized_entry)
            else:
                logger.warning(f"Skipping invalid entry structure: {entry}")

        # Return the processed data
        return jsonify({
            'project_id': project['project_id'],
            'project_name': project.get('project_name', 'Unknown Project'),
            'month': month,
            'data': sanitized_data
        }), 200

    except Exception as e:
        logger.error(f"Error fetching social_net data for project '{project_id}', month '{month}': {e}")
        return jsonify({'error': 'Internal server error.'}), 500

# This is to fetch commit links data for a particular project for a particular month
@main_routes.route('/api/commit_links/<project_id>/<int:month>', methods=['GET'])
@cross_origin(origin='*') 
def get_commit_links(project_id, month):
    """
    Fetch commit links data for a specific project and month.
    """
    try:
        normalized_project_id = project_id.strip().lower()
        project = db.commit_links.find_one({'project_id': normalized_project_id})
        if not project:
            return jsonify({'error': f"Project '{project_id}' not found."}), 404
        
        month_str = str(month)
        if 'months' not in project or month_str not in project['months']:
            return jsonify({'error': f"Month '{month}' data not found for project '{project_id}'."}), 404
        
        commits = project['months'][month_str]
        # Assuming commits is a list of dictionaries or lists; sanitize accordingly
        sanitized_commits = []
        for commit in commits:
            if isinstance(commit, dict):
                sanitized_commit = sanitize_document(commit)
                sanitized_commits.append(sanitized_commit)
            elif isinstance(commit, list):
                # Example: [commit_id, author, message]
                sanitized_commit = [
                    commit[0] if len(commit) > 0 and isinstance(commit[0], str) else '',
                    commit[1] if len(commit) > 1 and isinstance(commit[1], str) else '',
                    commit[2] if len(commit) > 2 and isinstance(commit[2], str) else ''
                ]
                sanitized_commits.append(sanitized_commit)
            else:
                sanitized_commits.append({})
        
        return jsonify({
            'project_id': project['project_id'],
            'project_name': project['project_name'],
            'month': month,
            'commits': sanitized_commits
        }), 200
    except Exception as e:
        logger.error(f"Error fetching commit_links data for project '{project_id}', month '{month}': {e}")
        return jsonify({'error': 'Internal server error.'}), 500

# This is to fetch email links data for a particular project for a particular month
@main_routes.route('/api/email_links/<project_id>/<int:month>', methods=['GET'])
@cross_origin(origin='*') 
def get_email_links(project_id, month):
    """
    Fetch email links data for a specific project and month.
    """
    try:
        normalized_project_id = project_id.strip().lower()
        project = db.email_links.find_one({'project_id': normalized_project_id})
        if not project:
            return jsonify({'error': f"Project '{project_id}' not found."}), 404
        
        month_str = str(month)
        if 'months' not in project or month_str not in project['months']:
            return jsonify({'error': f"Month '{month}' data not found for project '{project_id}'."}), 404
        
        commits = project['months'][month_str]
        # Assuming commits is a list of dictionaries or lists; sanitize accordingly
        sanitized_commits = []
        for commit in commits:
            if isinstance(commit, dict):
                sanitized_commit = sanitize_document(commit)
                sanitized_commits.append(sanitized_commit)
            elif isinstance(commit, list):
                # Example: [email, relation, count]
                sanitized_commit = [
                    commit[0] if len(commit) > 0 and isinstance(commit[0], str) else '',
                    commit[1] if len(commit) > 1 and isinstance(commit[1], str) else '',
                    commit[2] if len(commit) > 2 and isinstance(commit[2], (int, float)) else 0
                ]
                sanitized_commits.append(sanitized_commit)
            else:
                sanitized_commits.append({})
        
        return jsonify({
            'project_id': project['project_id'],
            'project_name': project['project_name'],
            'month': month,
            'commits': sanitized_commits
        }), 200
    except Exception as e:
        logger.error(f"Error fetching email_links data for project '{project_id}', month '{month}': {e}")
        return jsonify({'error': 'Internal server error.'}), 500

# Fetch project_info for a specific project_id
@main_routes.route('/api/project_info/<project_id>', methods=['GET'])
@cross_origin(origin='*') 
def get_project_info_api(project_id):
    """
    Fetch combined project information for a specific project.
    """
    try:
        normalized_project_id = project_id.strip().lower()
        project = db.project_info.find_one({'project_id': normalized_project_id})
        if not project:
            return jsonify({'error': f"Project '{project_id}' not found."}), 404
        
        # Remove MongoDB's _id field and sanitize
        project = sanitize_document(project)
        project.pop('_id', None)
        return jsonify(project), 200
    except Exception as e:
        logger.error(f"Error fetching project_info for project '{project_id}': {e}")
        return jsonify({'error': 'Internal server error.'}), 500

# This is to fetch the commits measure data for a project and the corresponding month
@main_routes.route('/api/commit_measure/<project_id>/<int:month>', methods=['GET'])
@cross_origin(origin='*') 
def get_commit_measure(project_id, month):
    """
    Fetch commit measure data for a specific project and month.
    """
    try:
        normalized_project_id = project_id.strip().lower()
        project = db.commit_measure.find_one({'project_id': normalized_project_id})
        if not project:
            return jsonify({'error': f"Project '{project_id}' not found."}), 404
        
        month_str = str(month)
        if 'months' not in project or month_str not in project['months']:
            return jsonify({'error': f"Month '{month}' data not found for project '{project_id}'."}), 404
        
        data = project['months'][month_str]
        # Assuming data is a list of measures; sanitize if necessary
        sanitized_data = []
        for measure in data:
            if isinstance(measure, dict):
                sanitized_measure = sanitize_document(measure)
                sanitized_data.append(sanitized_measure)
            elif isinstance(measure, list):
                # Example: [metric, value]
                sanitized_measure = [
                    measure[0] if len(measure) > 0 and isinstance(measure[0], str) else '',
                    measure[1] if len(measure) > 1 and isinstance(measure[1], (int, float)) else 0
                ]
                sanitized_data.append(sanitized_measure)
            else:
                sanitized_data.append({})
        
        return jsonify({
            'project_id': project['project_id'],
            'project_name': project['project_name'],
            'month': month,
            'data': sanitized_data
        }), 200
    except Exception as e:
        logger.error(f"Error fetching commit_measure data for project '{project_id}', month '{month}': {e}")
        return jsonify({'error': 'Internal server error.'}), 500

# This is to fetch the emails measure data for a month and project   
@main_routes.route('/api/email_measure/<project_id>/<int:month>', methods=['GET'])
@cross_origin(origin='*') 
def get_email_measure(project_id, month):
    """
    Fetch email measure data for a specific project and month.
    """
    try:
        normalized_project_id = project_id.strip().lower()
        project = db.email_measure.find_one({'project_id': normalized_project_id})
        if not project:
            return jsonify({'error': f"Project '{project_id}' not found."}), 404
        
        month_str = str(month)
        if 'months' not in project or month_str not in project['months']:
            return jsonify({'error': f"Month '{month}' data not found for project '{project_id}'."}), 404
        
        data = project['months'][month_str]
        # Assuming data is a list of email measures; sanitize if necessary
        sanitized_data = []
        for measure in data:
            if isinstance(measure, dict):
                sanitized_measure = sanitize_document(measure)
                sanitized_data.append(sanitized_measure)
            elif isinstance(measure, list):
                # Example: [email, metric, value]
                sanitized_measure = [
                    measure[0] if len(measure) > 0 and isinstance(measure[0], str) else '',
                    measure[1] if len(measure) > 1 and isinstance(measure[1], str) else '',
                    measure[2] if len(measure) > 2 and isinstance(measure[2], (int, float)) else 0
                ]
                sanitized_data.append(sanitized_measure)
            else:
                sanitized_data.append({})
        
        return jsonify({
            'project_id': project['project_id'],
            'project_name': project['project_name'],
            'month': month,
            'data': sanitized_data
        }), 200
    except Exception as e:
        logger.error(f"Error fetching email_measure data for project '{project_id}', month '{month}': {e}")
        return jsonify({'error': 'Internal server error.'}), 500

# Fetch grad_forecast for a specific project_id
@main_routes.route('/api/grad_forecast/<project_id>', methods=['GET'])
@cross_origin(origin='*') 
def get_grad_forecast_api(project_id):
    """
    Fetch forecast data for a specific project.
    """
    try:
        normalized_project_id = project_id.strip().lower()
        project = db.grad_forecast.find_one({'project_id': normalized_project_id}, {'forecast': 1, '_id': 0})
        if not project or 'forecast' not in project:
            return jsonify({'error': f"Forecast data for project '{project_id}' not found."}), 404
        
        # Return only the forecast data
        return jsonify(project['forecast']), 200
    except Exception as e:
        logger.error(f"Error fetching forecast data for project '{project_id}': {e}")
        return jsonify({'error': 'Internal server error.'}), 500