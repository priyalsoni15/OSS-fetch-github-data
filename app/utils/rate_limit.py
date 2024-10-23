import time
import logging

def handle_rate_limit(response_headers):
    remaining = int(response_headers.get('X-RateLimit-Remaining', 1))
    reset_time = int(response_headers.get('X-RateLimit-Reset', time.time()))
    if remaining < 10:
        sleep_time = reset_time - int(time.time()) + 1
        logging.info(f"Rate limit approaching. Sleeping for {sleep_time} seconds.")
        time.sleep(sleep_time)
