import os
import logging

class Config:
    ORG_NAME = os.environ.get('ORG_NAME')
    REPOSITORIES = [
        "https://github.com/apache/curator.git",
    ]
    
    APACHE_REPOSITORIES = [
        "https://lists.apache.org/list.html?dev@arrow.apache.org",
    ]

    # Automatically collect all GITHUB_TOKEN_* variables and put them into a list
    @staticmethod
    def collect_github_tokens():
        tokens = []
        index = 1
        while True:
            token = os.environ.get(f'GITHUB_TOKEN_{index}')
            if token:
                tokens.append(token)
                index += 1
            else:
                break
        return tokens

# After the class definition, assign GITHUB_TOKENS
Config.GITHUB_TOKENS = Config.collect_github_tokens()
#print("Loaded GitHub Tokens:", Config.GITHUB_TOKENS)

# Create output directories if they don't exist
os.makedirs('out/apache', exist_ok=True)
os.makedirs('out/apache/partial', exist_ok=True)

# Configure logging
logging.basicConfig(level=logging.INFO)
