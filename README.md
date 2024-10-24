# Open source sustainability tracker for ASF


Open source sustainability tracker for ASF
is a Flask application that fetches and processes mailing list data from Apache projects. It downloads mailing list archives, parses them, and extracts useful information for analysis.

## Installation

### Clone the Repository

```bash
git clone https://github.com/yourusername/apache-mailing-list-fetcher.git
cd apache-mailing-list-fetcher
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

### Fetching Mailing List Data
To initiate the process of fetching and processing the mailing list data, access the following endpoint in your web browser or use a tool like curl:

``` bash
http://localhost:5000/fetch_mailing_list
```

This will start downloading the mailing list archives, parse them, and save the extracted data to the out/apache/mailing_list/ directory in JSON format.

### Required

Ensure you have the following installed on your system:

Python 3.6 or higher
pip package manager

### Contributing

Contributions are welcome! Please feel free to open a Pull Request describing your changes. For major changes, please open an issue first to discuss what you'd like to change.

### Contact
In case of any questions, feel free to reach out to priyal15.soni@gmail.com.

### License
This project is licensed under the Apache License 2.0.
