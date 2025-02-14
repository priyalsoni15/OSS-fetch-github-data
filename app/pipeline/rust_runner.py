# flask-app/pipeline/rust_runner.py
import subprocess
import os
import logging
from dotenv import load_dotenv

load_dotenv()

OSS_SCRAPER_REPO_URL = os.getenv("OSS_SCRAPER_REPO_URL")
OSS_SCRAPER_DIR = os.getenv("OSS_SCRAPER_DIR")

def ensure_oss_scraper_repo():
    """Ensures the OSS‑Scraper repository is cloned locally.
       If the target directory does not exist, it is created and the repo is cloned.
       If it exists and is a git repository, perform a git pull to update it.
    """
    if not OSS_SCRAPER_DIR:
        raise Exception("OSS_SCRAPER_DIR is not set in your .env file.")
    
    if not os.path.exists(OSS_SCRAPER_DIR):
        try:
            os.makedirs(OSS_SCRAPER_DIR, exist_ok=True)
            logging.info(f"Directory {OSS_SCRAPER_DIR} did not exist; attempting to clone repository.")
            subprocess.run(
                ["git", "clone", OSS_SCRAPER_REPO_URL, OSS_SCRAPER_DIR],
                check=True
            )
        except Exception as e:
            logging.error(f"Failed to create or clone into {OSS_SCRAPER_DIR}: {e}")
            raise
    else:
        git_dir = os.path.join(OSS_SCRAPER_DIR, ".git")
        if os.path.exists(git_dir):
            logging.info(f"Directory {OSS_SCRAPER_DIR} exists and is a git repository; updating repository.")
            try:
                subprocess.run(["git", "pull"], cwd=OSS_SCRAPER_DIR, check=True)
            except Exception as e:
                logging.error(f"Failed to update repository at {OSS_SCRAPER_DIR}: {e}")
                raise
        else:
            logging.warning(f"Directory {OSS_SCRAPER_DIR} exists but is not a git repository. Skipping clone/update.")
    
    return os.path.abspath(OSS_SCRAPER_DIR)

def run_rust_code(git_link):
    """
    Given a .git URL, this function:
      1. Ensures the OSS‑Scraper repository is cloned/updated.
      2. Runs `cargo clean` and `cargo build`.
      3. Executes two miner commands to generate CSV outputs.
    Returns a dictionary with the outputs.
    """
    try:
        scraper_dir = OSS_SCRAPER_DIR
        logging.info("OSS‑Scraper directory: " + scraper_dir)

        # Ensure the output folder exists (if not, create it)
        output_folder = os.path.join(scraper_dir, "output")
        if not os.path.exists(output_folder):
            logging.info(f"Output folder {output_folder} does not exist. Creating it.")
            os.makedirs(output_folder, exist_ok=True)

        """
        # Please note: This is a blocking operation that may take a while. Only enable for debuggin purposes
        logging.info("Running cargo clean...")
        subprocess.run(["cargo", "clean"], cwd=scraper_dir, check=True)

        # Please note: This is a blocking operation that may take a while. Only enable for debuggin purposes
        logging.info("Running cargo build...")
        build_result = subprocess.run(
            ["cargo", "build"],
            cwd=scraper_dir,
            capture_output=True,
            text=True,
            check=True
        )
        logging.info("Cargo build output: " + build_result.stdout)

        # Please note: This is a blocking operation that may take a while. Only enable for debuggin purposes
        logging.info("Running cargo fix bin biner...")
        build_result = subprocess.run(
            ["cargo", "fix", "--bin", "miner", "--allow-dirty"],
            cwd=scraper_dir,
            capture_output=True,
            text=True,
            check=True
        )
        logging.info("Cargo fix output: " + build_result.stdout)
        """        
        
        cmd1 = [
            os.path.join("target", "debug", "miner"),
            "--fetch-github-issues",
            f"--github-url={git_link}",
            "--github-output-folder=output"
        ]
        logging.info("Running command: " + " ".join(cmd1))
        cmd1_result = subprocess.run(
            cmd1,
            cwd=scraper_dir,
            capture_output=True,
            text=True,
            check=True
        )
        logging.info("Command 1 output: " + cmd1_result.stdout)

        cmd2 = [
            os.path.join("target", "debug", "miner"),
            "--commit-devs-files",
            "--time-window=30",
            "--threads=2",
            "--output-folder=output",
            f"--git-online-url={git_link}"
        ]
        logging.info("Running command: " + " ".join(cmd2))
        cmd2_result = subprocess.run(
            cmd2,
            cwd=scraper_dir,
            capture_output=True,
            text=True,
            check=True
        )
        logging.info("Command 2 output: " + cmd2_result.stdout)
        logging.info("Final output directory: " + os.path.abspath(output_folder))
        
        return {
            "fetch_github_issues": cmd1_result.stdout,
            "commit_devs_files": cmd2_result.stdout,
            "output_dir": os.path.abspath(output_folder)
        }
    except subprocess.CalledProcessError as e:
        logging.error("Rust tool execution failed: " + str(e))
        return {"error": "Rust tool execution failed: " + str(e)}
    except Exception as ex:
        logging.error("Unexpected error: " + str(ex))
        return {"error": "Unexpected error: " + str(ex)}
