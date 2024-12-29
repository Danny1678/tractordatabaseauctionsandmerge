#!/usr/bin/env python3

import time
import json
import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import multiprocessing as mp
from functools import partial

BASE_URL = "https://www.tractordata.com"
BRANDS = [
    "AGCO", "Allis Chalmers", "Belarus", "Big Bud", "Case", "Case IH", "Caterpillar", 
    "Challenger", "Claas", "Deutz", "Deutz-Fahr", "Fendt", "Ford", "International Harvester", 
    "John Deere", "Kioti", "Kubota", "Landini", "Mahindra", "Massey Ferguson", "McCormick", 
    "Minneapolis-Moline", "New Holland", "Oliver", "Same", "Steiger", "Valtra", "Versatile", 
    "White", "Yanmar", "Zetor"
]

def get_soup(url):
    """
    Helper to fetch a URL and parse into BeautifulSoup
    """
    try:
        print(f"Fetching URL: {url}")  # Debug output
        time.sleep(1)  # be kind and wait 1 second between requests
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {str(e)}")
        return None

def get_brand_url(brand):
    """Convert brand name to URL format"""
    # Special cases for brand URLs
    brand_map = {
        "Case IH": "caseih",
        "International Harvester": "ih",
        "Massey Ferguson": "massey-ferguson",
        "Minneapolis-Moline": "minneapolis-moline",
        "New Holland": "newholland",
        "Deutz-Fahr": "deutz-fahr",
        "McCormick": "mccormick",
        "Big Bud": "bigbud"
    }
    
    if brand in brand_map:
        brand_formatted = brand_map[brand]
    else:
        # Remove special characters and spaces, convert to lowercase
        brand_formatted = brand.lower().replace(" ", "").replace("-", "").replace(".", "")
    
    url = f"{BASE_URL}/farm-tractors/tractor-brands/{brand_formatted}/{brand_formatted}-tractors.html"
    print(f"Generated URL for {brand}: {url}")  # Debug output
    return url

def scrape_brand_models(brand_name, brand_url):
    """
    For a given brand's tractors page, parse the table of tractor models/hp/years
    """
    soup = get_soup(brand_url)
    if not soup:
        print(f"Failed to get page for {brand_name}")
        return []
        
    model_data = []

    # Find the main data table
    tables = soup.find_all("table", class_="tdMenu1")
    if not tables:
        print(f"No data tables found for {brand_name}")
        return []
    
    print(f"Found {len(tables)} tables for {brand_name}")  # Debug output
    
    for table in tables:
        rows = table.find_all("tr")
        print(f"Found {len(rows)} rows in table")  # Debug output
        
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            try:
                model_link = cells[0].find("a")
                if not model_link:
                    continue
                    
                model_name = model_link.get_text(strip=True)
                model_url = BASE_URL + model_link["href"]
                power = cells[1].get_text(strip=True)
                years = cells[2].get_text(strip=True)

                # Clean and standardize horsepower data
                hp = power.lower().replace("hp", "").strip()
                if "(" in hp:
                    hp = hp.split("(")[0].strip()
                
                item = {
                    "brand": brand_name,
                    "model": model_name,
                    "horsepower": hp,
                    "years": years,
                    "url": model_url
                }
                model_data.append(item)
                print(f"Added {brand_name} {model_name}")  # Debug output
            except Exception as e:
                print(f"Error processing row for {brand_name}: {str(e)}")
                continue

    return model_data

def scrape_brand_chunk(brand_chunk, log_file=None):
    """Scrape a chunk of brands in a separate process"""
    scraper = TractorScraper(log_file)
    results = []
    for brand in brand_chunk:
        brand_results = scraper.scrape_brand_models(brand)
        if brand_results:
            results.extend(brand_results)
    return results

class TractorScraper:
    def __init__(self, log_file=None):
        self.results = []
        self.log_file = log_file or f"scraper_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        
    @staticmethod
    def chunk_list(lst, chunk_size):
        """Split list into chunks of specified size"""
        return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]
        
    def scrape_all_brands(self, num_processes=4):
        """Scrape all brands using multiple processes"""
        # Split brands into chunks
        chunk_size = max(1, len(BRANDS) // num_processes)
        brand_chunks = self.chunk_list(BRANDS, chunk_size)
        
        # Create a pool of workers
        with mp.Pool(processes=num_processes) as pool:
            # Use partial to pass the log file to each worker
            worker_func = partial(scrape_brand_chunk, log_file=self.log_file)
            # Map chunks to workers and collect results
            chunk_results = pool.map(worker_func, brand_chunks)
            
        # Combine results from all chunks
        for chunk in chunk_results:
            self.results.extend(chunk)
            
        self.save_results()
        self.log(f"Completed scraping {len(self.results)} total tractors")

    def scrape_brand_models(self, brand):
        """
        For a given brand's tractors page, parse the table of tractor models/hp/years
        """
        brand_url = get_brand_url(brand)
        return scrape_brand_models(brand, brand_url)

    def save_results(self):
        # Save to JSON
        json_file = "tractordata_all_tractors.json"
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(self.results, f, indent=2)

        # Save to CSV
        csv_file = "tractor_data.csv"
        if self.results:
            with open(csv_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.results[0].keys())
                writer.writeheader()
                writer.writerows(self.results)

    def log(self, message):
        with open(self.log_file, "a", encoding="utf-8") as log:
            log.write(f"{message}\n")

if __name__ == '__main__':
    scraper = TractorScraper()
    # Use number of CPU cores minus 1 to avoid overloading
    num_processes = max(1, mp.cpu_count() - 1)
    scraper.scrape_all_brands(num_processes=num_processes)
