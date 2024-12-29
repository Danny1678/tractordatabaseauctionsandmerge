#!/usr/bin/env python3

import time
import json
import csv
from datetime import datetime
import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager
import random
import concurrent.futures
from queue import Queue
from threading import Lock
import logging
import urllib3
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any, List
import multiprocessing
import backoff
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from fake_useragent import UserAgent
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# List of free proxy providers (you can add more)
PROXY_PROVIDERS = [
    'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt',
    'https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt',
    'https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt',
]

class ProxyRotator:
    def __init__(self):
        self.proxies = []
        self.current_index = 0
        self.lock = Lock()
        self.last_update = 0
        self.update_interval = 300
        self.logger = logging.getLogger('scraper')

    def update_proxies(self):
        """Fetch and validate new proxies"""
        self.logger.info("Updating proxy list...")
        new_proxies = set()
        
        # Only use the first proxy provider for faster initialization
        provider = PROXY_PROVIDERS[0]
        try:
            self.logger.info(f"Fetching proxies from {provider}")
            response = requests.get(provider, timeout=10)
            if response.status_code == 200:
                proxies = response.text.strip().split('\n')
                new_proxies.update(proxies)
                self.logger.info(f"Found {len(proxies)} proxies")
        except Exception as e:
            self.logger.error(f"Error fetching proxies: {str(e)}")
        
        # Test only a subset of proxies
        valid_proxies = []
        test_proxies = list(new_proxies)[:50]  # Test only first 50 proxies
        self.logger.info(f"Testing {len(test_proxies)} proxies...")
        
        for proxy in test_proxies:
            if self._test_proxy(proxy.strip()):
                valid_proxies.append(proxy.strip())
                if len(valid_proxies) >= 10:  # Stop after finding 10 valid proxies
                    break
        
        with self.lock:
            self.proxies = valid_proxies
            self.current_index = 0
            self.last_update = time.time()
            self.logger.info(f"Updated proxy list with {len(valid_proxies)} valid proxies")

    def _test_proxy(self, proxy):
        """Test if proxy is working"""
        try:
            test_url = 'https://www.machinerypete.com'
            response = requests.get(
                test_url,
                proxies={'http': f'http://{proxy}', 'https': f'http://{proxy}'},
                timeout=3,  # Reduced timeout
                verify=False
            )
            return response.status_code == 200
        except:
            return False

    def get_proxy(self):
        """Get next working proxy"""
        with self.lock:
            if not self.proxies or time.time() - self.last_update > self.update_interval:
                self.update_proxies()
            
            if not self.proxies:
                self.logger.warning("No valid proxies available")
                return None
            
            proxy = self.proxies[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.proxies)
            self.logger.debug(f"Using proxy: {proxy}")
            return proxy

@dataclass
class ListingData:
    """Data structure for listing information"""
    brand: Optional[str] = None
    model: Optional[str] = None
    price: Optional[float] = None
    sold_date: Optional[str] = None
    hours: Optional[float] = None
    specs: Optional[str] = None
    location: Optional[str] = None
    condition: Optional[str] = None

class RetrySession:
    """Session with retry logic for handling connection issues"""
    def __init__(self, proxy_rotator=None):
        self.session = requests.Session()
        self.proxy_rotator = proxy_rotator
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=50, pool_maxsize=50)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        self.session.verify = False
        
        # Randomize headers
        self.ua = UserAgent()
        self.update_headers()

    def update_headers(self):
        """Update session headers with random user agent and other variations"""
        self.session.headers.update({
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
        })

    def get(self, url, **kwargs):
        """Make GET request with proxy rotation"""
        if self.proxy_rotator:
            proxy = self.proxy_rotator.get_proxy()
            if proxy:
                kwargs['proxies'] = {
                    'http': f'http://{proxy}',
                    'https': f'http://{proxy}'
                }
        self.update_headers()
        return self.session.get(url, **kwargs)

class PageScraper:
    """Individual page scraper that runs in its own thread"""
    def __init__(self, base_url: str, logger: logging.Logger, proxy_rotator: ProxyRotator = None):
        self.base_url = base_url
        self.logger = logger
        self.driver = None
        self.setup_driver()

    def setup_driver(self):
        """Set up Chrome WebDriver with appropriate options"""
        try:
            chrome_options = Options()
            chrome_options.add_argument('--headless=new')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_argument('--disable-extensions')
            
            # Add random user agent
            ua = UserAgent()
            chrome_options.add_argument(f'user-agent={ua.random}')
            
            # Additional stealth settings
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            try:
                # Try using ChromeDriverManager with specific version
                driver_path = ChromeDriverManager(version="114.0.5735.90").install()
                service = Service(driver_path)
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
            except Exception as e:
                self.logger.error(f"Failed to initialize Chrome WebDriver with service: {str(e)}")
                try:
                    # Fallback: try with default ChromeDriver
                    self.driver = webdriver.Chrome(options=chrome_options)
                except Exception as e:
                    self.logger.error(f"Failed to initialize Chrome WebDriver with fallback: {str(e)}")
                    raise
            
            # Additional stealth
            self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": ua.random})
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
        except Exception as e:
            self.logger.error(f"Error setting up WebDriver: {str(e)}")
            raise

    def _random_sleep(self, min_seconds=1, max_seconds=2):
        """Random sleep to mimic human behavior"""
        time.sleep(random.uniform(min_seconds, max_seconds))

    def _random_scroll(self):
        """Simplified scrolling behavior"""
        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        self._random_sleep(0.5, 1)

    @backoff.on_exception(backoff.expo, 
                         (WebDriverException, TimeoutException),
                         max_tries=3,
                         max_time=30)
    def _scroll_page(self):
        """Optimized scroll with retry logic and random behavior"""
        self._random_scroll()
        self._random_sleep(1, 2)

    def scrape_page(self, page_num: int) -> List[Dict[str, Any]]:
        """Scrape a single page with simplified anti-ban measures"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                if not self.driver or not hasattr(self.driver, 'current_url'):
                    self.setup_driver()
                
                url = (f"{self.base_url}?"
                       f"category=tractors&"
                       f"sort_term=auction_listing_sold_date_recent_first&"
                       f"limit=72&"
                       f"page={page_num}")
                
                self._random_sleep(1, 2)
                self.driver.get(url)
                
                # Wait for listings with increased timeout
                try:
                    wait = WebDriverWait(self.driver, 10)  # Increased timeout
                    listings = wait.until(
                        EC.presence_of_all_elements_located((By.CSS_SELECTOR, ".listing-wrapper.US-listing"))
                    )
                except TimeoutException:
                    self.logger.error(f"Timeout waiting for listings on page {page_num}")
                    return []
                
                if not listings:
                    return []
                
                results = []
                for listing in listings:
                    try:
                        data = self.extract_listing_data(listing)
                        if data:
                            parsed = self.parse_listing_data(data)
                            if parsed:
                                results.append(asdict(parsed))
                    except StaleElementReferenceException:
                        continue
                    except Exception as e:
                        self.logger.error(f"Error processing listing: {str(e)}")
                        continue
                
                self.logger.info(f"Successfully scraped page {page_num} with {len(results)} listings")
                return results
                
            except Exception as e:
                retry_count += 1
                self.logger.error(f"Error scraping page {page_num} (attempt {retry_count}/{max_retries}): {str(e)}")
                
                # Clean up the driver
                if self.driver:
                    try:
                        self.driver.quit()
                    except Exception:
                        pass
                self.driver = None
                
                if retry_count < max_retries:
                    self.logger.info(f"Retrying page {page_num}...")
                    time.sleep(random.uniform(2, 5))  # Wait before retry
                else:
                    self.logger.error(f"Failed to scrape page {page_num} after {max_retries} attempts")
                    return []
            finally:
                if self.driver:
                    try:
                        self.driver.quit()
                    except Exception:
                        pass
                    self.driver = None

    def extract_listing_data(self, listing) -> Dict[str, Any]:
        """Extract data from listing element with retry logic"""
        try:
            data = {}
            
            # Title
            try:
                title_el = listing.find_element(By.CSS_SELECTOR, ".listing-name h3")
                data['title_text'] = title_el.text.strip() if title_el else None
            except Exception:
                data['title_text'] = None
            
            # Price
            try:
                price_el = listing.find_element(By.CSS_SELECTOR, ".auction-listing-price > .listing-price")
                data['price_text'] = price_el.text.strip() if price_el else None
            except Exception:
                data['price_text'] = None
            
            # Sold Date
            try:
                sold_el = listing.find_element(By.XPATH, ".//div[@class='listing-field-label'][contains(., 'Sold:')]")
                sold_span = sold_el.find_element(By.CSS_SELECTOR, ".basic-non-bold")
                data['sold_date'] = sold_span.text.strip() if sold_span else None
            except Exception:
                data['sold_date'] = None
            
            # Hours - Try multiple approaches with specs field first
            try:
                data['hours_text'] = None
                
                # First try extracting from specs since that's where most hours info is
                try:
                    specs_el = listing.find_element(By.XPATH, ".//div[@class='listing-field-label'][contains(., 'Specs:')]")
                    if specs_el:
                        specs_text = specs_el.find_element(By.CSS_SELECTOR, ".listing-field-value").text.strip()
                        
                        # Look for hours in specs text with expanded patterns
                        hours_patterns = [
                            # Direct hours mentions
                            r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:hrs?|hours?|hr)\b',  # 1,234 hrs, 1234.5 hours
                            r'(?:hrs?|hours?|hr)[^\d]*(\d{1,3}(?:,\d{3})*(?:\.\d+)?)',  # hrs: 1234, hours - 1,234
                            
                            # Hours showing/reading patterns
                            r'(?:showing|shows?|reading|reads?)\s+(\d{1,3}(?:,\d{3})*(?:\.\d+)?)',  # showing 1,234
                            r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:hrs?|hours?|hr)?\s*(?:showing|indicated)',  # 1234 hrs showing
                            
                            # Special cases
                            r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:engine|original|actual)\s*(?:hrs?|hours?|hr)',  # 1,234 engine hours
                            r'(?:engine|original|actual)\s*(?:hrs?|hours?|hr)[^\d]*(\d{1,3}(?:,\d{3})*(?:\.\d+)?)',  # engine hours: 1234
                            
                            # Broader patterns (use only if others fail)
                            r'(?:hrs?|hours?|hr).*?(\d{1,3}(?:,\d{3})*(?:\.\d+)?)',  # hours followed by number anywhere
                            r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)[^\d\n]*(?:hrs?|hours?|hr)',  # number followed by hours
                            r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?=.*?(?:hrs?|hours?|hr))'  # number with hours later in text
                        ]
                        
                        # Try each pattern in order
                        for pattern in hours_patterns:
                            match = re.search(pattern, specs_text, re.IGNORECASE)
                            if match:
                                hours_text = match.group(1).strip()
                                # Validate the extracted hours
                                try:
                                    hours_clean = re.sub(r'[^\d.,]', '', hours_text)
                                    hours_clean = hours_clean.replace(',', '')
                                    hours_value = float(hours_clean)
                                    if 0 <= hours_value <= 100000:  # Reasonable range check
                                        data['hours_text'] = hours_text
                                        break
                                except (ValueError, TypeError):
                                    continue
                except Exception as e:
                    self.logger.debug(f"Error extracting hours from specs: {str(e)}")
                
                # If no hours found in specs, try direct hours field as backup
                if not data.get('hours_text'):
                    try:
                        hours_el = listing.find_element(By.XPATH, ".//div[@class='listing-field-label'][contains(., 'Hours:')]")
                        hours_val = hours_el.find_element(By.CSS_SELECTOR, ".listing-field-value")
                        data['hours_text'] = hours_val.text.strip() if hours_val else None
                    except Exception:
                        pass
                
            except Exception as e:
                self.logger.error(f"Error extracting hours: {str(e)}")
                data['hours_text'] = None
            
            # Condition
            try:
                condition_el = listing.find_element(By.XPATH, ".//div[@class='listing-field-label'][contains(., 'Condition:')]")
                condition_val = condition_el.find_element(By.CSS_SELECTOR, ".listing-field-value")
                data['condition'] = condition_val.text.strip() if condition_val else None
            except Exception:
                data['condition'] = None
            
            # Specs
            try:
                specs_el = listing.find_element(By.XPATH, ".//div[@class='listing-field-label'][contains(., 'Specs:')]")
                specs_val = specs_el.find_element(By.CSS_SELECTOR, ".listing-field-value")
                data['specs'] = specs_val.text.strip() if specs_val else None
            except Exception:
                data['specs'] = None
            
            # Location
            try:
                location_el = listing.find_element(By.CSS_SELECTOR, ".auction-event-details .auction-event-details")
                data['location'] = location_el.text.strip() if location_el else None
            except Exception:
                data['location'] = None
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error extracting listing data: {str(e)}")
            return None

    def parse_listing_data(self, raw_data: Dict[str, Any]) -> Optional[ListingData]:
        """Parse raw listing data into structured format"""
        try:
            if not raw_data:
                return None
                
            listing = ListingData()
            
            # Process title
            if raw_data.get('title_text'):
                parts = raw_data['title_text'].split(maxsplit=1)
                listing.brand = parts[0]
                if len(parts) > 1:
                    listing.model = parts[1]
            
            # Process price
            if raw_data.get('price_text'):
                numeric = re.sub(r'[^\d.]', '', raw_data['price_text'])
                try:
                    listing.price = float(numeric) if numeric else None
                except ValueError:
                    pass
            
            # Process hours with better handling
            if raw_data.get('hours_text'):
                try:
                    # Remove any non-numeric characters except decimal points and commas
                    hours_clean = re.sub(r'[^\d.,]', '', raw_data['hours_text'])
                    # Remove commas
                    hours_clean = hours_clean.replace(',', '')
                    # Convert to float
                    listing.hours = float(hours_clean) if hours_clean else None
                except ValueError:
                    # If conversion fails, try to find any number in the string
                    match = re.search(r'(\d{1,3}(?:,\d{3})*(?:\.\d+)?)', raw_data['hours_text'])
                    if match:
                        try:
                            hours_str = match.group(1).replace(',', '')
                            listing.hours = float(hours_str)
                        except ValueError:
                            pass
            
            # Copy direct fields
            listing.condition = raw_data.get('condition')
            listing.specs = raw_data.get('specs')
            listing.sold_date = raw_data.get('sold_date')
            listing.location = raw_data.get('location')
            
            return listing
            
        except Exception as e:
            self.logger.error(f"Error parsing listing data: {str(e)}")
            return None

class MachineryPeteScraper:
    def __init__(self, base_url=None, log_file=None, max_workers=None):
        self.base_url = base_url or "https://www.machinerypete.com/auction_results"
        self.results = []
        self.results_lock = Lock()
        self.log_file = log_file or f"scraper_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        self.max_workers = max_workers or 2  # Fixed to 2 workers
        self.logger = self.setup_logging()
        self.logger.info(f"Initialized scraper with {self.max_workers} worker threads")

    def setup_logging(self):
        """Configure logging with proper format and return logger instance"""
        logger = logging.getLogger('scraper')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            formatter = logging.Formatter('%(asctime)s - %(message)s')
            
            file_handler = logging.FileHandler(self.log_file)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
        
        return logger

    def scrape_pages(self, start_page=1, end_page=2):
        """Scrape multiple pages with simplified anti-ban measures"""
        try:
            failed_pages = []
            all_results = []
            
            # Process pages in smaller batches
            batch_size = 2
            
            for batch_start in range(start_page, end_page + 1, batch_size):
                batch_end = min(batch_start + batch_size - 1, end_page)
                self.logger.info(f"Processing batch: pages {batch_start} to {batch_end}")
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                    future_to_page = {
                        executor.submit(self._scrape_single_page, page_num): page_num
                        for page_num in range(batch_start, batch_end + 1)
                    }
                    
                    for future in concurrent.futures.as_completed(future_to_page):
                        page_num = future_to_page[future]
                        try:
                            page_results = future.result()
                            if page_results:
                                all_results.extend(page_results)
                                self.logger.info(f"Progress: {len(all_results)} total listings scraped (page {page_num}/{end_page})")
                            else:
                                failed_pages.append(page_num)
                        except Exception as e:
                            self.logger.error(f"Error processing page {page_num}: {str(e)}")
                            failed_pages.append(page_num)
                
                # Save results after each batch
                if all_results:
                    self.results = all_results
                    self.save_results()
                
                # Delay between batches
                time.sleep(random.uniform(3, 5))
            
            # Quick retry for failed pages
            if failed_pages:
                self.logger.info(f"Retrying {len(failed_pages)} failed pages...")
                for page_num in failed_pages:
                    try:
                        time.sleep(random.uniform(3, 5))
                        scraper = PageScraper(self.base_url, self.logger)
                        page_results = scraper.scrape_page(page_num)
                        if page_results:
                            all_results.extend(page_results)
                    except Exception as e:
                        self.logger.error(f"Failed to retry page {page_num}: {str(e)}")
            
            # Save final results
            if all_results:
                self.results = all_results
                self.save_results()
                self.logger.info(f"Final results count: {len(self.results)} listings")
                
        except Exception as e:
            self.logger.error(f"Error during parallel scraping: {str(e)}")
            raise

    def _scrape_single_page(self, page_num: int) -> List[Dict[str, Any]]:
        """Scrape a single page using a dedicated PageScraper instance"""
        scraper = PageScraper(self.base_url, self.logger)
        return scraper.scrape_page(page_num)

    def save_results(self):
        """Save results to JSON and CSV files with proper error handling"""
        if not self.results:
            self.logger.info("No results to save")
            return
        
        with self.results_lock:
            try:
                # Save to JSON
                json_file = "auction_results.json"
                with open(json_file, "w", encoding="utf-8") as f:
                    json.dump(self.results, f, indent=2)
                
                # Save to CSV
                csv_file = "auction_results.csv"
                if self.results:
                    with open(csv_file, "w", newline="", encoding="utf-8") as f:
                        fieldnames = ["brand", "model", "price", "sold_date", "hours", "condition", "specs", "location"]
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(self.results)
                
                self.logger.info(f"Successfully saved {len(self.results)} results to {json_file} and {csv_file}")
            except PermissionError as e:
                self.logger.error(f"Permission error saving results: {str(e)}")
            except IOError as e:
                self.logger.error(f"IO error saving results: {str(e)}")
            except Exception as e:
                self.logger.error(f"Unexpected error saving results: {str(e)}")

def main():
    scraper = MachineryPeteScraper(max_workers=2)
    scraper.scrape_pages(start_page=795, end_page=2622)  # Continue from where we actually left off

if __name__ == "__main__":
    main()
