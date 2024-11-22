# Open source sustainability tracker for ASF


Open source sustainability tracker for ASF
is a Flask application that fetches and processes github repos and mailing list data from Apache Software Foundation projects (incubated, mature, or incubating). It uses Github GraphQL API and GitHub REST API to collect and analyze data from GitHub repo commits, issues, etc. It also downloads mailing list archives, parses them, and extracts useful information for analysis. 

## Installation

### Clone the Repository

```bash
git clone https://github.com/yourusername/apache-mailing-list-fetcher.git](https://github.com/priyalsoni15/OSS-fetch-github-data.git
cd OSS-fetch-github-data
```

### Create a Virtual Environment
It's recommended to use a virtual environment to manage your project's dependencies.

For Unix/Linux/MacOS

```bash
python3 -m venv venv
source venv/bin/activate
```

For Windows
```bash
python -m venv venv
venv\Scripts\activate
```

### Install Dependencies

Install the required Python packages using pip:

```bash
pip install -r requirements.txt
```

### Usage
Running the Flask Application

Start the Flask application using the following command:

```bash
flask run
```
By default, the application will run on http://localhost:5000/.

### Defined end-points
Access the following endpoint in your web browser or use a tool like curl:

``` bash
http://127.0.0.1:5000/
```

/api/projects - This endpoint fetches all the github repos stored under the organization 'apache'

/api/github_stars - This endpoint fetches stars, forks and watch for each github repo

/api/project_description - This endpoint fetches the project info, mentors, project status, etc from the Apache website for all projects

/api/project_info - This fetches all the combined project information from the endpoints above

/api/tech_net/<project_id>/<int:month> - This fetches the technical network for a particular project, month-wise

/api/social_net/<project_id>/<int:month> - This fetches the social network for a particular project, month-wise

/api/commit_links/<project_id>/<int:month> - This fetches the commit information for a particular project, month-wise

/api/email_links/<project_id>/<int:month> - This fetches the email information for a particular project, month-wise

/api/commit_measure/<project_id>/<int:month> - This fetches the commit measure information for a particular project, month-wise

/api/email_measure/<project_id>/<int:month> - This fetches the email measure information for a particular project, month-wise

/api/monthly_ranges - This fetches the monthly range for all projects available for Apache

### Database worker

Run the scripts for uploading data into MongoDB using this command (Please note that this takes in static .json/.csv files from the data folder, available on the server and creates collections accordingly)

``` bash
python3 ./workers/apache_mongo_worker.py
```

### Required

Ensure you have the following installed on your system:

Python 3.6 or higher
pip package manager

### Contributing

Contributions are welcome! Please feel free to open a Pull Request describing your changes. For major changes, please open an issue first to discuss what you'd like to change.

### Contact
In case of any questions, feel free to reach out to priyal15.soni@gmail.com or pdsoni@ucdavis.edu

### License
This project is licensed under the Apache License 2.0.
