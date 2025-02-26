#!/usr/bin/env python3
import csv
import os
import concurrent.futures
from datetime import datetime
from unidecode import unidecode
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables from .env file (if present)
load_dotenv()

# Get the MongoDB URI from the environment (default if not set)
MONGODB_URI = os.environ.get("MONGODB_URI", "mongodb://localhost:27017/testdb")

def process_csv_and_store(csv_file: str, project_id: str = None, project_name: str = None):
    """
    Reads a CSV file of commit/issue data, detects type, groups by 'month index',
    and upserts into MongoDB under either 'commit_links' or 'issue_links'.
    """

    def clean_author_name(name: str) -> str:
        """Remove diacritics and extra spaces from an author's name."""
        return unidecode(name).strip() if name else ""

    def get_month_index(event_dt: datetime, earliest_dt: datetime) -> int:
        """Determine which 'month index' a given event_dt belongs to."""
        return (event_dt.year - earliest_dt.year) * 12 + (event_dt.month - earliest_dt.month) + 1

    def human_readable_date(dt: datetime) -> str:
        """Convert a datetime into a human-readable format."""
        return dt.strftime("%a %b %d %H:%M:%S %Y")

    def detect_file_type(header_fields: list) -> str:
        """Determine if the CSV contains commit or issue data by analyzing headers."""
        lower_fields = [field.lower() for field in header_fields]
        if "commit_sha" in lower_fields or "commit_url" in lower_fields:
            return "commit"
        if "issue_url" in lower_fields:
            return "issue"
        print(f"WARNING: Could not determine file type for {csv_file}. Defaulting to 'commit'.")
        return "commit"

    def parse_datetime(date_str: str, possible_formats: list) -> datetime:
        """Try parsing a datetime string using multiple formats."""
        for fmt in possible_formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                pass
        return None

    if not os.path.isfile(csv_file):
        raise FileNotFoundError(f"CSV file not found: {csv_file}")

    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        print(f"No data in {csv_file}. Nothing to process.")
        return

    file_type = detect_file_type(reader.fieldnames or [])
    date_field = "date" if file_type == "commit" else "created_at"
    # Use internal collection names in the DB but do not expose these names to API clients.
    link_type = "local_commit_links" if file_type == "commit" else "local_issue_links"

    # Auto-detect project_id from CSV if not provided
    if not project_id:
        first_row = rows[0]
        project_id = first_row.get("project", "").strip().lower() or first_row.get("repo_name", "").strip().lower() or "unknown_project"

    # Auto-detect project_name if not provided
    if not project_name:
        project_name = project_id.capitalize()

    datetime_formats = {
        "commit": ["%Y-%m-%d %H:%M:%S %Z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S%z"],
        "issue": ["%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S %Z", "%Y-%m-%d %H:%M:%S"]
    }

    all_datetimes = [parse_datetime(row.get(date_field, "").strip(), datetime_formats[file_type]) for row in rows if row.get(date_field)]
    all_datetimes = [dt for dt in all_datetimes if dt]

    if not all_datetimes:
        print(f"No valid {file_type} date/times found in {csv_file}. Aborting.")
        return

    earliest_dt = min(all_datetimes)
    
    # ✅ Add `last_fetched` outside `months` (human-readable format)
    final_doc = {
        "project_id": project_id,
        "project_name": project_name,
        "last_fetched": human_readable_date(datetime.utcnow()),  # Current timestamp
        "months": {}
    }

    for row in rows:
        raw_date = row.get(date_field, "").strip()
        dt = parse_datetime(raw_date, datetime_formats[file_type]) if raw_date else None
        if not dt:
            continue

        m_index = str(get_month_index(dt, earliest_dt))
        hr_date = human_readable_date(dt)
        author = row.get("name", "") or row.get("user_name", "") or row.get("user_login", "")
        cleaned_author = clean_author_name(author)
        link = row.get("commit_url") or row.get("issue_url") or ""

        entry = {
            "human_date_time": hr_date,
            "link": link,
            "dealised_author_full_name": cleaned_author
        }

        final_doc["months"].setdefault(m_index, []).append(entry)

    client = MongoClient(MONGODB_URI)
    db = client.get_default_database()
    collection = db[link_type]

    result = collection.update_one({"project_id": project_id}, {"$set": final_doc}, upsert=True)
    client.close()

    print(f"File classified as: {file_type.upper()}")
    print(f"Successfully upserted data for project_id='{project_id}'.")
    print(f"MongoDB upsert: matched_count={result.matched_count}, modified_count={result.modified_count}")

def process_project_data(folder_path: str, project_id: str = None, project_name: str = None):
    """
    Detects commit and issue CSVs in a folder and processes them in parallel.
    Now accepts optional project_id and project_name so that all CSVs are processed with a consistent identifier.
    """

    commit_csv = None
    issue_csv = None

    # Auto-detect CSVs in folder
    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file)
        if file.endswith(".csv"):
            with open(file_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                headers = next(reader, None)
                if headers:
                    if "commit_sha" in [h.lower() for h in headers] or "commit_url" in [h.lower() for h in headers]:
                        commit_csv = file_path
                    elif "issue_url" in [h.lower() for h in headers]:
                        issue_csv = file_path

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = {}

        if commit_csv:
            futures[executor.submit(process_csv_and_store, commit_csv, project_id, project_name)] = "commit"

        if issue_csv:
            futures[executor.submit(process_csv_and_store, issue_csv, project_id, project_name)] = "issue"

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # Raises exceptions if any occur
            except Exception as e:
                print(f"Error in processing {futures[future]} file: {e}")
