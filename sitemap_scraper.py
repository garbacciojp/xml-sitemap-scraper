import csv
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# Setup logging
logging.basicConfig(filename='sitemap_scraper.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Function to scrape the URLs and extract <loc> tags
def scrape_and_extract_locs(url):
    locs = []
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, timeout=10, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'xml')
        locs = [loc_tag.text for loc_tag in soup.find_all('loc')]
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err} - URL: {url}")
    except requests.exceptions.ConnectionError as conn_err:
        logging.error(f"Connection error occurred: {conn_err} - URL: {url}")
    except requests.exceptions.Timeout as timeout_err:
        logging.error(f"Timeout error occurred: {timeout_err} - URL: {url}")
    except requests.exceptions.RequestException as req_err:
        logging.error(f"An error occurred: {req_err} - URL: {url}")
    except Exception as err:
        logging.error(f"An unexpected error occurred: {err} - URL: {url}")
    finally:
        return locs

# Generate a unique output file name with a timestamp
timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
output_file_name = f'extracted_locs_{timestamp}.csv'

# Read the CSV with URLs
with open('input_urls.csv', mode='r', newline='') as file:
    reader = csv.reader(file)
    url_list = [row[0] for row in reader]

# Use ThreadPoolExecutor for parallel processing
sitemap_locs = []
with ThreadPoolExecutor(max_workers=5) as executor:
    future_to_url = {executor.submit(scrape_and_extract_locs, url): url for url in url_list}
    for future in as_completed(future_to_url):
        url = future_to_url[future]
        try:
            locs = future.result()
            for loc in locs:
                sitemap_locs.append([url, loc])
        except Exception as e:
            logging.error(f"Error processing URL {url}: {e}")

# Save the extracted locs to a CSV file
df = pd.DataFrame(sitemap_locs, columns=['Sitemap', 'Loc'])
df.to_csv(output_file_name, index=False)

logging.info(f"Data extracted and saved to {output_file_name}")