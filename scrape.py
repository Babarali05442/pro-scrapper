#!/usr/bin/env python3
"""
ProScrape Backend API Server
A secure web scraping service with user authentication and job management
"""

import os
import json
import time
import hashlib
import secrets
import sqlite3
import threading
import queue
import re
from datetime import datetime, timedelta
from urllib.parse import urlparse
from flask import Flask, request, jsonify
from flask_cors import CORS
import jwt
import requests
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
import socket
import ipaddress

# Initialize Flask app
app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
CORS(app, origins=['http://localhost:*', 'http://127.0.0.1:*'])

# Database setup
DB_PATH = 'proscrape.db'

def init_db():
    """Initialize database with required tables"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Users table
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        email TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )''')
    
    # Jobs table
    c.execute('''CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        url TEXT NOT NULL,
        description TEXT,
        mode TEXT DEFAULT 'auto',
        crawl_depth INTEGER DEFAULT 0,
        max_pages INTEGER DEFAULT 1,
        selectors TEXT,
        status TEXT DEFAULT 'queued',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        completed_at TIMESTAMP,
        error_message TEXT,
        FOREIGN KEY (user_id) REFERENCES users (id)
    )''')
    
    # Results table
    c.execute('''CREATE TABLE IF NOT EXISTS results (
        id TEXT PRIMARY KEY,
        job_id TEXT NOT NULL,
        data TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (job_id) REFERENCES jobs (id)
    )''')
    
    conn.commit()
    conn.close()

# Initialize database on startup
init_db()

# Job queue for async processing
job_queue = queue.Queue()

class ScrapingWorker(threading.Thread):
    """Background worker for processing scraping jobs"""
    
    def __init__(self):
        super().__init__(daemon=True)
        self.running = True
    
    def run(self):
        while self.running:
            try:
                if not job_queue.empty():
                    job_id = job_queue.get(timeout=1)
                    self.process_job(job_id)
                else:
                    time.sleep(1)
            except queue.Empty:
                continue
            except Exception as e:
                print(f"Worker error: {e}")
    
    def process_job(self, job_id):
        """Process a single scraping job"""
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        
        try:
            # Get job details
            c.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
            job = c.fetchone()
            
            if not job:
                return
            
            # Update status to running
            c.execute("UPDATE jobs SET status = 'running' WHERE id = ?", (job_id,))
            conn.commit()
            
            # Extract job parameters
            url = job[2]
            mode = job[4]
            crawl_depth = job[5] or 0
            max_pages = job[6] or 1
            selectors = json.loads(job[7]) if job[7] else None
            
            # Perform the scraping
            results = self.scrape_url(url, mode, selectors, crawl_depth, max_pages)
            
            if results:
                # Store results
                result_id = secrets.token_hex(16)
                c.execute("INSERT INTO results (id, job_id, data) VALUES (?, ?, ?)",
                         (result_id, job_id, json.dumps(results)))
                
                # Update job status
                c.execute("UPDATE jobs SET status = 'completed', completed_at = CURRENT_TIMESTAMP WHERE id = ?",
                         (job_id,))
            else:
                c.execute("UPDATE jobs SET status = 'failed', error_message = 'No data extracted' WHERE id = ?",
                         (job_id,))
            
            conn.commit()
            
        except Exception as e:
            c.execute("UPDATE jobs SET status = 'failed', error_message = ? WHERE id = ?",
                     (str(e), job_id))
            conn.commit()
        finally:
            conn.close()
    
    def scrape_url(self, url, mode, selectors, crawl_depth, max_pages):
        """Perform advanced web scraping with multiple records extraction"""
        try:
            # Check robots.txt
            if not self.check_robots(url):
                raise Exception("Robots.txt disallows scraping this URL")
            
            # Check for local/private IPs
            if not self.is_safe_url(url):
                raise Exception("URL points to a private or local IP address")
            
            # Headers to appear more like a real browser
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
            
            all_results = []
            visited_urls = set()
            urls_to_visit = [url]
            pages_scraped = 0
            
            while urls_to_visit and pages_scraped < max_pages:
                current_url = urls_to_visit.pop(0)
                
                if current_url in visited_urls:
                    continue
                    
                visited_urls.add(current_url)
                
                try:
                    response = requests.get(current_url, headers=headers, timeout=10)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    if mode == 'css' and selectors:
                        # Advanced CSS selector mode with multiple records
                        results = self.extract_with_selectors(soup, selectors)
                    else:
                        # Enhanced auto-extract mode
                        results = self.enhanced_auto_extract(soup, current_url)
                    
                    if results:
                        all_results.extend(results)
                    
                    pages_scraped += 1
                    
                    # Find pagination links if we need more pages
                    if pages_scraped < max_pages:
                        next_links = self.find_pagination_links(soup, current_url)
                        for link in next_links:
                            if link not in visited_urls and link not in urls_to_visit:
                                urls_to_visit.append(link)
                    
                    # Small delay between requests to be polite
                    time.sleep(0.5)
                    
                except Exception as e:
                    print(f"Error scraping {current_url}: {e}")
                    continue
            
            return all_results
                
        except Exception as e:
            raise Exception(f"Scraping failed: {str(e)}")
    
    def enhanced_auto_extract(self, soup, current_url):
        """Enhanced automatic extraction that finds multiple records and structured data"""
        results = []
        
        # Try to detect and extract tables
        tables = soup.find_all('table')
        for table in tables:
            table_data = self.extract_table_data(table)
            if table_data:
                results.extend(table_data)
        
        # Try to detect product/item listings
        product_results = self.extract_product_listings(soup)
        if product_results:
            results.extend(product_results)
        
        # Try to detect article/blog listings
        article_results = self.extract_article_listings(soup)
        if article_results:
            results.extend(article_results)
        
        # Try to detect data cards/grids
        card_results = self.extract_card_data(soup)
        if card_results:
            results.extend(card_results)
        
        # If no structured data found, fall back to general extraction
        if not results:
            general_result = self.extract_general_data(soup)
            if general_result:
                results.append(general_result)
        
        return results
    
    def extract_table_data(self, table):
        """Extract data from HTML tables"""
        results = []
        
        # Get headers
        headers = []
        header_row = table.find('thead')
        if header_row:
            headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
        else:
            # Try first row as headers
            first_row = table.find('tr')
            if first_row:
                headers = [cell.get_text(strip=True) for cell in first_row.find_all(['th', 'td'])]
        
        if not headers:
            return results
        
        # Get data rows
        tbody = table.find('tbody') or table
        rows = tbody.find_all('tr')
        
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) == len(headers):
                row_data = {}
                for i, header in enumerate(headers):
                    if i < len(cells):
                        row_data[header] = cells[i].get_text(strip=True)
                if row_data and any(v for v in row_data.values()):
                    results.append(row_data)
        
        return results
    
    def extract_product_listings(self, soup):
        """Extract product/item listings from e-commerce style pages"""
        results = []
        
        # First try specific known patterns for books.toscrape.com
        if 'books.toscrape' in str(soup):
            products = soup.select('article.product_pod')
            for product in products:
                data = {}
                
                # Title - get the actual title text, not the link
                title_elem = product.select_one('h3 a')
                if title_elem:
                    # Get title attribute or text
                    data['title'] = title_elem.get('title') or title_elem.get_text(strip=True)
                
                # Price - extract the actual price
                price_elem = product.select_one('p.price_color')
                if price_elem:
                    data['price'] = price_elem.get_text(strip=True)
                
                # Rating - extract star rating
                rating_elem = product.select_one('p.star-rating')
                if rating_elem:
                    # Get the class that contains the rating
                    classes = rating_elem.get('class', [])
                    for cls in classes:
                        if cls in ['One', 'Two', 'Three', 'Four', 'Five']:
                            data['rating'] = cls
                            break
                
                # Stock availability
                stock_elem = product.select_one('p.instock.availability')
                if stock_elem:
                    data['availability'] = stock_elem.get_text(strip=True).replace('\n', ' ').strip()
                
                # Image
                img = product.select_one('div.image_container img')
                if img:
                    data['image'] = img.get('src', '')
                
                # Link to product page
                link = product.select_one('h3 a')
                if link:
                    data['link'] = link.get('href', '')
                
                if data:
                    results.append(data)
            
            if results:
                return results
        
        # Common product container patterns for other sites
        product_selectors = [
            'article.product_pod',  # books.toscrape.com
            'div.product', 'article.product', 'li.product',
            'div.product-item', 'div.product-card',
            'div.item', 'article.item', 'li.item',
            'div.col-product', 'div.grid-item',
            'article[data-product]', 'div[data-product-id]',
            'li[class*="product"]', 'div[class*="product"]',
            'article[class*="product"]', 'div.card.product'
        ]
        
        products = []
        for selector in product_selectors:
            found = soup.select(selector)
            if found and len(found) > 1:  # We want multiple products
                products = found
                break
        
        # If no specific product containers, try to find repeated structures
        if not products:
            # Try to find articles or list items that appear multiple times
            articles = soup.find_all('article')
            if len(articles) > 2:
                products = articles
            else:
                list_items = soup.find_all('li', class_=True)
                # Filter for items with substantial content
                products = [li for li in list_items if len(li.get_text(strip=True)) > 30][:50]
        
        for product in products[:100]:  # Limit to 100 products
            data = {}
            
            # Extract title - try multiple strategies
            title = None
            # Strategy 1: Look for heading tags
            for tag in ['h1', 'h2', 'h3', 'h4', 'h5']:
                title_elem = product.find(tag)
                if title_elem:
                    title = title_elem.get_text(strip=True)
                    break
            
            # Strategy 2: Look for links with title
            if not title:
                link = product.find('a', title=True)
                if link:
                    title = link.get('title')
            
            # Strategy 3: Look for any prominent link
            if not title:
                link = product.find('a')
                if link:
                    title = link.get_text(strip=True)
            
            if title and len(title) > 3:
                data['title'] = title
            
            # Extract price with multiple patterns
            price_selectors = ['.price', '.price_color', '.cost', '[class*="price"]', '[data-price]']
            for selector in price_selectors:
                price_elem = product.select_one(selector)
                if price_elem:
                    price_text = price_elem.get_text(strip=True)
                    if price_text:
                        data['price'] = price_text
                        break
            
            # If no price found with selectors, try regex
            if 'price' not in data:
                product_text = product.get_text()
                price_patterns = [
                    r'[\$£€]\s*[\d,]+\.?\d*',
                    r'USD\s*[\d,]+\.?\d*',
                    r'Rs\.?\s*[\d,]+\.?\d*',
                    r'Price:\s*([\d,]+\.?\d*)',
                    r'\b\d+\.\d{2}\b'  # Any decimal number that could be a price
                ]
                for pattern in price_patterns:
                    prices = re.findall(pattern, product_text)
                    if prices:
                        data['price'] = prices[0]
                        break
            
            # Extract description
            desc_selectors = ['.description', '.summary', '.excerpt', 'p']
            for selector in desc_selectors:
                desc_elem = product.select_one(selector)
                if desc_elem:
                    desc_text = desc_elem.get_text(strip=True)
                    if desc_text and len(desc_text) > 20:
                        data['description'] = desc_text[:200]
                        break
            
            # Extract image
            img = product.find('img')
            if img:
                data['image'] = img.get('src') or img.get('data-src') or img.get('data-lazy-src')
            
            # Extract link
            link = product.find('a', href=True)
            if link:
                data['link'] = link['href']
            
            # Extract rating
            rating_elem = product.select_one('[class*="rating"], [class*="star"]')
            if rating_elem:
                # Try to extract rating from class names
                classes = rating_elem.get('class', [])
                for cls in classes:
                    if 'five' in cls.lower() or '5' in cls:
                        data['rating'] = '5 stars'
                    elif 'four' in cls.lower() or '4' in cls:
                        data['rating'] = '4 stars'
                    elif 'three' in cls.lower() or '3' in cls:
                        data['rating'] = '3 stars'
                    elif 'two' in cls.lower() or '2' in cls:
                        data['rating'] = '2 stars'
                    elif 'one' in cls.lower() or '1' in cls:
                        data['rating'] = '1 star'
                
                # Also check text content
                if 'rating' not in data:
                    rating_text = rating_elem.get_text(strip=True)
                    if rating_text:
                        data['rating'] = rating_text
            
            # Extract stock/availability
            stock_selectors = ['.availability', '.stock', '.instock', '[class*="stock"]']
            for selector in stock_selectors:
                stock_elem = product.select_one(selector)
                if stock_elem:
                    data['availability'] = stock_elem.get_text(strip=True)
                    break
            
            # If no availability found, check for keywords
            if 'availability' not in data:
                product_text = product.get_text().lower()
                if 'in stock' in product_text or 'available' in product_text:
                    data['availability'] = 'In Stock'
                elif 'out of stock' in product_text or 'unavailable' in product_text:
                    data['availability'] = 'Out of Stock'
            
            # Only add if we have at least a title or price
            if data and ('title' in data or 'price' in data):
                results.append(data)
        
        return results
    
    def extract_article_listings(self, soup):
        """Extract article/blog post listings"""
        results = []
        
        # Common article container patterns
        article_selectors = [
            'article', 'div.post', 'div.article',
            'div.blog-post', 'div.entry',
            'section.post', 'div.content-item'
        ]
        
        articles = []
        for selector in article_selectors:
            found = soup.select(selector)
            if found:
                articles = found
                break
        
        if not articles:
            return results
        
        for article in articles[:50]:  # Limit to 50 articles
            data = {}
            
            # Extract title
            title = article.find(['h1', 'h2', 'h3', 'h4'])
            if title:
                data['title'] = title.get_text(strip=True)
            
            # Extract date
            date_patterns = [
                r'\d{4}-\d{2}-\d{2}',
                r'\d{1,2}/\d{1,2}/\d{4}',
                r'\w+ \d{1,2}, \d{4}',
                r'\d{1,2} \w+ \d{4}'
            ]
            article_text = article.get_text()
            for pattern in date_patterns:
                dates = re.findall(pattern, article_text)
                if dates:
                    data['date'] = dates[0]
                    break
            
            # Extract author
            author_tags = article.find_all(['span', 'div', 'p'], class_=re.compile(r'author|by-line|writer'))
            for tag in author_tags:
                text = tag.get_text(strip=True)
                if text and len(text) < 100:
                    data['author'] = text.replace('By ', '').replace('by ', '')
                    break
            
            # Extract summary/excerpt
            summary_tags = article.find_all(['p', 'div'], class_=re.compile(r'summary|excerpt|description'))
            if summary_tags:
                data['summary'] = summary_tags[0].get_text(strip=True)[:300]
            elif article.find('p'):
                data['summary'] = article.find('p').get_text(strip=True)[:300]
            
            # Extract link
            link = article.find('a', href=True)
            if link:
                data['link'] = link['href']
            
            # Extract category/tags
            category_tags = article.find_all(['span', 'a'], class_=re.compile(r'category|tag|label'))
            if category_tags:
                data['categories'] = [tag.get_text(strip=True) for tag in category_tags[:5]]
            
            if data and 'title' in data:
                results.append(data)
        
        return results
    
    def extract_card_data(self, soup):
        """Extract data from card-based layouts (Bootstrap, Material Design, etc.)"""
        results = []
        
        # Common card patterns
        card_selectors = [
            'div.card', 'div.panel', 'div.tile',
            'div.box', 'div.module', 'div.widget',
            'div[class*="card"]', 'div[class*="item"]',
            'div[class*="col-"]'  # Bootstrap columns
        ]
        
        cards = []
        for selector in card_selectors:
            found = soup.select(selector)
            if found and len(found) > 2:  # Need multiple cards
                cards = found
                break
        
        if not cards:
            return results
        
        for card in cards[:100]:  # Limit to 100 cards
            data = {}
            
            # Extract all text content
            card_text = card.get_text(separator=' ', strip=True)
            
            # Skip if too short
            if len(card_text) < 10:
                continue
            
            # Extract title (usually in heading or bold)
            title = card.find(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'strong', 'b'])
            if title:
                data['title'] = title.get_text(strip=True)
            
            # Extract various text fields
            text_elements = card.find_all(['p', 'span', 'div'], string=True)
            fields = []
            for elem in text_elements[:10]:
                text = elem.get_text(strip=True)
                if text and len(text) > 3 and text not in fields:
                    fields.append(text)
            
            if fields:
                # Try to identify common fields
                for i, field in enumerate(fields):
                    if re.search(r'\$[\d,]+\.?\d*|£[\d,]+\.?\d*|€[\d,]+\.?\d*', field):
                        data['price'] = field
                    elif re.search(r'\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{4}', field):
                        data['date'] = field
                    elif re.search(r'@|email', field, re.IGNORECASE):
                        data['email'] = field
                    elif re.search(r'\+?\d{10,}|\(\d{3}\)', field):
                        data['phone'] = field
                    elif i == 0 and 'title' not in data:
                        data['title'] = field
                    else:
                        data[f'field_{i}'] = field[:200]
            
            # Extract image
            img = card.find('img')
            if img and img.get('src'):
                data['image'] = img['src']
            
            # Extract link
            link = card.find('a', href=True)
            if link:
                data['link'] = link['href']
            
            if data:
                results.append(data)
        
        return results
    
    def extract_general_data(self, soup):
        """Fallback general data extraction"""
        result = {}
        
        # Title
        title = soup.find('title')
        if title:
            result['title'] = title.get_text(strip=True)
        
        # Main heading
        h1 = soup.find('h1')
        if h1:
            result['heading'] = h1.get_text(strip=True)
        
        # Meta description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc:
            result['description'] = meta_desc.get('content', '')
        
        # All text content
        all_text = soup.get_text(separator=' ', strip=True)
        result['full_text'] = all_text[:5000]  # First 5000 chars
        
        # Extract all structured data
        # JSON-LD
        json_ld = soup.find_all('script', type='application/ld+json')
        if json_ld:
            structured_data = []
            for script in json_ld:
                try:
                    data = json.loads(script.string)
                    structured_data.append(data)
                except:
                    pass
            if structured_data:
                result['structured_data'] = structured_data
        
        return result
    
    def extract_with_selectors(self, soup, selectors):
        """Extract data using CSS selectors with support for multiple records"""
        results = []
        
        # Check if selectors define a container for repeating elements
        if 'container' in selectors:
            containers = soup.select(selectors['container'])
            for container in containers:
                record = {}
                for key, selector in selectors.items():
                    if key != 'container':
                        element = container.select_one(selector)
                        if element:
                            record[key] = element.get_text(strip=True)
                if record:
                    results.append(record)
        else:
            # Try to match selectors as arrays
            first_key = list(selectors.keys())[0]
            first_elements = soup.select(selectors[first_key])
            
            if len(first_elements) > 1:
                # Multiple matches - create one record per match
                for i in range(len(first_elements)):
                    record = {}
                    for key, selector in selectors.items():
                        elements = soup.select(selector)
                        if i < len(elements):
                            record[key] = elements[i].get_text(strip=True)
                    if record:
                        results.append(record)
            else:
                # Single match - create one record with all data
                record = {}
                for key, selector in selectors.items():
                    elements = soup.select(selector)
                    if elements:
                        if len(elements) == 1:
                            record[key] = elements[0].get_text(strip=True)
                        else:
                            record[key] = [elem.get_text(strip=True) for elem in elements]
                if record:
                    results.append(record)
        
        return results
    
    def find_pagination_links(self, soup, current_url):
        """Find pagination links to scrape multiple pages"""
        from urllib.parse import urljoin, urlparse
        
        pagination_links = []
        base_url = f"{urlparse(current_url).scheme}://{urlparse(current_url).netloc}"
        
        # Look for pagination containers
        pagination_selectors = [
            'nav.pagination', 'div.pagination', 'ul.pagination',
            'div.pager', 'div.page-numbers', 'div.pages',
            'a.next', 'a.page-link', 'a[rel="next"]'
        ]
        
        for selector in pagination_selectors:
            elements = soup.select(selector)
            if elements:
                # Find links within pagination
                links = []
                for elem in elements:
                    if elem.name == 'a':
                        links.append(elem)
                    else:
                        links.extend(elem.find_all('a', href=True))
                
                for link in links:
                    href = link.get('href')
                    if href:
                        # Convert relative URLs to absolute
                        full_url = urljoin(current_url, href)
                        # Only add if it's from the same domain
                        if urlparse(full_url).netloc == urlparse(current_url).netloc:
                            pagination_links.append(full_url)
                
                if pagination_links:
                    break
        
        # Also look for "Next" or numbered page links
        if not pagination_links:
            next_patterns = ['next', 'siguiente', 'suivant', '»', '›', 'more']
            for pattern in next_patterns:
                next_link = soup.find('a', string=re.compile(pattern, re.IGNORECASE))
                if next_link and next_link.get('href'):
                    full_url = urljoin(current_url, next_link['href'])
                    if urlparse(full_url).netloc == urlparse(current_url).netloc:
                        pagination_links.append(full_url)
                        break
        
        return pagination_links[:5]  # Limit to 5 pagination links
    
    def check_robots(self, url):
        """Check if robots.txt allows scraping"""
        try:
            parsed = urlparse(url)
            robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
            
            rp = RobotFileParser()
            rp.set_url(robots_url)
            rp.read()
            
            # Check if our user agent is allowed
            return rp.can_fetch("ProScrape Bot", url)
        except:
            # If we can't check robots.txt, allow by default
            return True
    
    def is_safe_url(self, url):
        """Check if URL points to a safe (non-local) IP address"""
        try:
            parsed = urlparse(url)
            hostname = parsed.hostname
            
            if not hostname:
                return False
            
            # Resolve hostname to IP
            ip = socket.gethostbyname(hostname)
            ip_obj = ipaddress.ip_address(ip)
            
            # Block private and local IPs
            if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local:
                return False
            
            return True
        except:
            # If we can't resolve, block by default
            return False

# Start the worker thread
worker = ScrapingWorker()
worker.start()

# Utility functions
def hash_password(password):
    """Hash a password using SHA256"""
    return hashlib.sha256(password.encode()).hexdigest()

def generate_token(user_id):
    """Generate a JWT token for a user"""
    payload = {
        'user_id': user_id,
        'exp': datetime.utcnow() + timedelta(hours=24)
    }
    return jwt.encode(payload, app.config['SECRET_KEY'], algorithm='HS256')

def verify_token(token):
    """Verify and decode a JWT token"""
    try:
        payload = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
        return payload['user_id']
    except:
        return None

def require_auth(f):
    """Decorator to require authentication for an endpoint"""
    def wrapper(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({'error': 'Authentication required'}), 401
        
        token = auth_header.split(' ')[1]
        user_id = verify_token(token)
        
        if not user_id:
            return jsonify({'error': 'Invalid or expired token'}), 401
        
        request.user_id = user_id
        return f(*args, **kwargs)
    
    wrapper.__name__ = f.__name__
    return wrapper

# API Routes

@app.route('/api/auth/register', methods=['POST'])
def register():
    """Register a new user"""
    data = request.json
    email = data.get('email')
    password = data.get('password')
    
    if not email or not password:
        return jsonify({'error': 'Email and password required'}), 400
    
    # Validate email format
    if not re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', email):
        return jsonify({'error': 'Invalid email format'}), 400
    
    # Check password length
    if len(password) < 6:
        return jsonify({'error': 'Password must be at least 6 characters'}), 400
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    try:
        # Check if user exists
        c.execute("SELECT id FROM users WHERE email = ?", (email,))
        if c.fetchone():
            return jsonify({'error': 'Email already registered'}), 400
        
        # Create user
        user_id = secrets.token_hex(16)
        password_hash = hash_password(password)
        c.execute("INSERT INTO users (id, email, password_hash) VALUES (?, ?, ?)",
                 (user_id, email, password_hash))
        conn.commit()
        
        return jsonify({'message': 'Registration successful'}), 201
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/auth/login', methods=['POST'])
def login():
    """Login a user"""
    data = request.json
    email = data.get('email')
    password = data.get('password')
    
    if not email or not password:
        return jsonify({'error': 'Email and password required'}), 400
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    try:
        # Find user
        c.execute("SELECT id, password_hash FROM users WHERE email = ?", (email,))
        user = c.fetchone()
        
        if not user:
            return jsonify({'error': 'Invalid credentials'}), 401
        
        # Check password
        if hash_password(password) != user[1]:
            return jsonify({'error': 'Invalid credentials'}), 401
        
        # Generate token
        token = generate_token(user[0])
        
        return jsonify({
            'token': token,
            'user': {
                'id': user[0],
                'email': email
            }
        }), 200
        
    finally:
        conn.close()

@app.route('/api/auth/verify', methods=['GET'])
@require_auth
def verify():
    """Verify authentication status"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    try:
        c.execute("SELECT email FROM users WHERE id = ?", (request.user_id,))
        user = c.fetchone()
        
        if user:
            return jsonify({
                'user': {
                    'id': request.user_id,
                    'email': user[0]
                }
            }), 200
        else:
            return jsonify({'error': 'User not found'}), 404
            
    finally:
        conn.close()

@app.route('/api/scrape/create', methods=['POST'])
@require_auth
def create_scrape_job():
    """Create a new scraping job"""
    data = request.json
    url = data.get('url')
    
    if not url:
        return jsonify({'error': 'URL is required'}), 400
    
    # Validate URL format
    try:
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            return jsonify({'error': 'Invalid URL format'}), 400
    except:
        return jsonify({'error': 'Invalid URL format'}), 400
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    try:
        # Create job
        job_id = secrets.token_hex(16)
        c.execute("""
            INSERT INTO jobs (id, user_id, url, description, mode, crawl_depth, max_pages, selectors)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            job_id,
            request.user_id,
            url,
            data.get('description', ''),
            data.get('mode', 'auto'),
            data.get('crawl_depth', 0),
            data.get('max_pages', 1),
            json.dumps(data.get('selectors')) if data.get('selectors') else None
        ))
        conn.commit()
        
        # Add to job queue
        job_queue.put(job_id)
        
        return jsonify({'job_id': job_id}), 201
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@app.route('/api/scrape/jobs', methods=['GET'])
@require_auth
def get_jobs():
    """Get all jobs for the authenticated user"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    try:
        c.execute("""
            SELECT id, url, description, mode, status, created_at, completed_at
            FROM jobs
            WHERE user_id = ?
            ORDER BY created_at DESC
            LIMIT 50
        """, (request.user_id,))
        
        jobs = [dict(row) for row in c.fetchall()]
        return jsonify(jobs), 200
        
    finally:
        conn.close()

@app.route('/api/scrape/job/<job_id>', methods=['GET'])
@require_auth
def get_job(job_id):
    """Get details of a specific job"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    try:
        c.execute("""
            SELECT * FROM jobs
            WHERE id = ? AND user_id = ?
        """, (job_id, request.user_id))
        
        job = c.fetchone()
        if job:
            return jsonify(dict(job)), 200
        else:
            return jsonify({'error': 'Job not found'}), 404
            
    finally:
        conn.close()

@app.route('/api/scrape/results/<job_id>', methods=['GET'])
@require_auth
def get_results(job_id):
    """Get results for a specific job"""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    try:
        # Verify job belongs to user
        c.execute("SELECT id FROM jobs WHERE id = ? AND user_id = ?", (job_id, request.user_id))
        if not c.fetchone():
            return jsonify({'error': 'Job not found'}), 404
        
        # Get results
        c.execute("SELECT data FROM results WHERE job_id = ?", (job_id,))
        result = c.fetchone()
        
        if result:
            data = json.loads(result[0])
            return jsonify(data), 200
        else:
            return jsonify([]), 200
            
    finally:
        conn.close()

@app.route('/api/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({'status': 'healthy'}), 200

# Run the server
if __name__ == '__main__':
    print("""
    ========================================
    ProScrape Backend Server
    ========================================
    
    The server is starting on http://localhost:5000
    
    Make sure to run the frontend (index.html) in a web browser.
    
    Default test credentials:
    - Register with any email and password (min 6 chars)
    
    API Endpoints:
    - POST /api/auth/register
    - POST /api/auth/login
    - GET  /api/auth/verify
    - POST /api/scrape/create
    - GET  /api/scrape/jobs
    - GET  /api/scrape/job/<job_id>
    - GET  /api/scrape/results/<job_id>
    
    Press Ctrl+C to stop the server.
    ========================================
    """)
    
    app.run(host='0.0.0.0', port=5000, debug=True)