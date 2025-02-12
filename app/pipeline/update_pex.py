# flask-app/pipeline/update_pex.py

import subprocess
import os
import logging
from dotenv import load_dotenv

load_dotenv()

GITHUB_USERNAME = os.getenv("GITHUB_USERNAME")
GITHUB_PAT = os.getenv("GITHUB_TOKEN_1")
PEX_GENERATOR_REPO_URL = os.getenv("PEX_GENERATOR_REPO_URL")
PEX_GENERATOR_DIR = os.getenv("PEX_GENERATOR_DIR")

def ensure_pex_generator_repo():
    """Ensure the PEX‑Forecaster repository is cloned locally.
       If the target directory does not exist, create it and clone the repo using a PAT.
       If it exists and is a git repository, update it.
       Then, install the package in editable mode (pip install -e .).
    """
    if not PEX_GENERATOR_DIR:
        raise Exception("PEX_GENERATOR_DIR is not set in your .env file.")
    
    # If the directory does not exist, clone the repository.
    if not os.path.exists(PEX_GENERATOR_DIR):
        try:
            os.makedirs(PEX_GENERATOR_DIR, exist_ok=True)
            logging.info(f"Directory {PEX_GENERATOR_DIR} did not exist; cloning repository.")
            # Build the clone URL with token
            if GITHUB_PAT is None:
                raise Exception("GITHUB_TOKEN_1 is not set in your .env file.")
            clone_url = PEX_GENERATOR_REPO_URL.replace("https://", f"https://{GITHUB_USERNAME}:{GITHUB_PAT}@")
            subprocess.run(
                ["git", "clone", clone_url, PEX_GENERATOR_DIR],
                check=True
            )
        except Exception as e:
            logging.error(f"Failed to clone into {PEX_GENERATOR_DIR}: {e}")
            raise
    else:
        git_dir = os.path.join(PEX_GENERATOR_DIR, ".git")
        if os.path.exists(git_dir):
            logging.info(f"Directory {PEX_GENERATOR_DIR} exists and is a git repository; updating repository.")
            try:
                subprocess.run(["git", "pull"], cwd=PEX_GENERATOR_DIR, check=True)
            except Exception as e:
                logging.error(f"Failed to update repository at {PEX_GENERATOR_DIR}: {e}")
                raise
        else:
            logging.warning(f"Directory {PEX_GENERATOR_DIR} exists but is not a git repository. Skipping clone/update.")
    
    # # Install the package in editable mode.
    # try:
    #     logging.info("Installing pex-forecaster package in editable mode...")
    #     subprocess.run(["pip", "install", "-e", "."], cwd=PEX_GENERATOR_DIR, check=True)
    # except Exception as e:
    #     logging.error(f"Failed to install pex-forecaster in editable mode: {e}")
    #     raise

    return os.path.abspath(PEX_GENERATOR_DIR)

def update_pex_generator():
    """Pulls the latest changes from PEX‑Forecaster using a GitHub token."""
    try:
        if not GITHUB_PAT:
            return {"error": "GitHub token not set in .env"}
        
        ensure_pex_generator_repo()
        
        # Optionally, you can run a git pull again to ensure the repository is current.
        subprocess.run(["git", "pull"], cwd=PEX_GENERATOR_DIR, check=True)
        # (Optional) Re-install dependencies if necessary.
        subprocess.run(["pip", "install", "-r", "requirements.txt"], cwd=PEX_GENERATOR_DIR, check=True)
        return {"message": "PEX‑Forecaster updated successfully."}
    except subprocess.CalledProcessError as e:
        logging.error(f"Git pull failed: {e}")
        return {"error": f"Git pull failed: {e}"}
    except Exception as ex:
        logging.error(f"Unexpected error: {ex}")
        return {"error": f"Unexpected error: {ex}"}
