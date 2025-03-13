# Open source sustainability Web server for OSPEX


This servers as the host API Web server, providing data for projects belonging to either Apache Software Foundation , or Eclipse Software Foundation. Now, it is also facilitated to support the Local mode for OSPEX (Open source sustainability project explorer), which means, it can process data for any Github repository! Apart from serving Github REST APIs, which fetch social network, technical network, commits history, emails/issues history, graduation forecast, project details, number of senders, total emails/issues, and emails/issues per sender, commits, committers and commits per committer, it also doubles up as the sole point of control where OSPEX functionality is hosted from. This means supporting POST request for Github APIs, orchestrating that different functionalities work together, (ReACTs, RUST scraper and pex-forecaster), it also fetches and stores data to different collections in MongoDB.

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


## API Endpoints Documentation

This document provides an overview of the available API endpoints and their functionality.

### Fetching GitHub Repository Data

```bash
GET /api/projects
```
- **Description**: Fetches all GitHub repositories stored under the organization `apache`.

```bash
GET /api/github_stars
```
- **Description**: Fetches stars, forks, and watch information for each GitHub repository.

### Fetching Project Information

```bash
GET /api/project_description
```
- **Description**: Fetches project information such as mentors, project status, etc., from the Apache website for all projects.

```bash
GET /api/project_info
```
- **Description**: Fetches all combined project information from the endpoints above.

### Technical and Social Networks (Month-wise)

```bash
GET /api/tech_net/<project_id>/int:month
```
- **Description**: Fetches the technical network for a specific project, filtered by month.

```bash
GET /api/social_net/<project_id>/int:month
```
- **Description**: Fetches the social network for a specific project, filtered by month.

### Commit and Email Information (Month-wise)

```bash
GET /api/commit_links/<project_id>/int:month
```
- **Description**: Fetches commit information for a specific project, filtered by month.

```bash
GET /api/email_links/<project_id>/int:month
```
- **Description**: Fetches email information for a specific project, filtered by month.

### Commit and Email Measures (Month-wise)

```bash
GET /api/commit_measure/<project_id>/int:month
```
- **Description**: Fetches commit measure information for a specific project, filtered by month.

```bash
GET /api/email_measure/<project_id>/int:month
```
- **Description**: Fetches email measure information for a specific project, filtered by month.

### Fetching Monthly Ranges

```bash
GET /api/monthly_ranges
```
- **Description**: Fetches the monthly range for all available Apache projects.


### Notes
- Replace `<project_id>` with the unique identifier for the project.
- Replace `int:month` with the specific month you want to query.

---

## [Feature] Database worker

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
