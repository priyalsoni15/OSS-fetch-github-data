import subprocess
import logging
import os

def run_rust_code(git_link):
    """
    Given a .git URL, this function performs the following steps in the OSS-scraper directory:
      1. Runs 'cargo clean'
      2. Runs 'cargo build'
      3. Executes:
           ./target/debug/miner --fetch-github-issues --github-url=<git_link> --github-output-folder=output
         and then:
           ./target/debug/miner --commit-devs-files --time-window=30 --threads=2 --output-folder=output --git-online-url=<git_link>
    
    Returns a dictionary with the outputs of the build and both commands.
    """
    try:
        # Determine the current file's directory.
        # Current file is in OSS-fetch-github-data/app/services/
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # Go up three levels to reach the common root:
        #   1. from services -> app
        #   2. from app -> OSS-fetch-github-data
        #   3. from OSS-fetch-github-data -> common root
        common_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
        # The Rust tool (OSS-scraper) is a sibling of OSS-fetch-github-data:
        rust_dir = os.path.join(common_root, "OSS-scraper")
        rust_dir = os.path.abspath(rust_dir)
        logging.info("Rust tool directory: " + rust_dir)

        # 1. Run cargo clean
        logging.info("Running cargo clean...")
        subprocess.run(["cargo", "clean"], cwd=rust_dir, check=True)

        # 2. Run cargo build
        logging.info("Running cargo build...")
        build_result = subprocess.run(
            ["cargo", "build"],
            cwd=rust_dir,
            capture_output=True,
            text=True,
            check=True
        )
        logging.info("Cargo build output: " + build_result.stdout)

        # 3. Run miner command for fetching GitHub issues
        cmd1 = [
            os.path.join("target", "debug", "miner"),
            "--fetch-github-issues",
            f"--github-url={git_link}",
            "--github-output-folder=output"
        ]
        logging.info("Running command: " + " ".join(cmd1))
        cmd1_result = subprocess.run(
            cmd1,
            cwd=rust_dir,
            capture_output=True,
            text=True,
            check=True
        )
        logging.info("Command 1 output: " + cmd1_result.stdout)

        # 4. Run miner command for committing developer files
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
            cwd=rust_dir,
            capture_output=True,
            text=True,
            check=True
        )
        logging.info("Command 2 output: " + cmd2_result.stdout)

        # Return a summary of outputs
        result_summary = {
            "cargo_build": build_result.stdout,
            "fetch_github_issues": cmd1_result.stdout,
            "commit_devs_files": cmd2_result.stdout
        }
        return result_summary

    except subprocess.CalledProcessError as e:
        logging.error("Rust tool execution failed: " + str(e))
        return {"error": "Rust tool execution failed: " + str(e)}
    except Exception as ex:
        logging.error("Unexpected error: " + str(ex))
        return {"error": "Unexpected error: " + str(ex)}