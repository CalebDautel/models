import requests
from bs4 import BeautifulSoup
import re
import time

# Alpaca API credentials (replace with your actual keys)
API_KEY = 'xxxxx'
API_SECRET = 'xxxxxxxxxx'
alpaca_base_url = 'https://paper-api.alpaca.markets'

# Headers for Alpaca API authentication
alpaca_headers = {
    'APCA-API-KEY-ID': API_KEY,
    'APCA-API-SECRET-KEY': API_SECRET
}

# Manually input EPS value range for comparison (unadjusted EPS)
manual_eps_low = 4.56
manual_eps_high = 4.88

# Ticker to short if conditions are met
ticker_to_short = 'APOG'  # Apogee Enterprises ticker

# URL of the Apogee Enterprises news releases page
base_url = 'https://www.apog.com/news-releases'

# Headers to mimic a browser for the scraping requests
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
}

# Function to fetch news titles and links
def fetch_article_links():
    # Send GET request with headers
    response = requests.get(base_url, headers=headers)
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        # Find all articles on the page
        articles = soup.find_all('a', href=True)

        # Filter articles containing the keyword "Apogee Enterprises Reports Fiscal 2025 Second Quarter Results"
        for article in articles:
            title = article.text.strip()
            if "apogee enterprises reports fiscal 2025 second quarter results" in title.lower():
                article_link = article['href']
                # Handle relative URLs
                full_article_link = article_link if article_link.startswith("http") else f"https://www.apog.com{article_link}"
                return full_article_link
    return None

# Function to fetch details from an article and extract the EPS outlook
def fetch_article_details(article_url):
    response = requests.get(article_url, headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        content = soup.get_text(separator='\n')

        # Search for EPS outlook information (we expect it in the format "$x.xx to $x.xx")
        eps_match = re.search(r'outlook for full-year diluted eps.*?(\$\d+\.\d+ to \$\d+\.\d+)', content, re.IGNORECASE)
        
        if eps_match:
            eps_str = eps_match.group(1)
            
            # Extract the EPS number range using regex and convert it to floats
            eps_number_match = re.search(r'\$(\d+\.\d+) to \$(\d+\.\d+)', eps_str)
            if eps_number_match:
                eps_low = float(eps_number_match.group(1))
                eps_high = float(eps_number_match.group(2))
                
                # Compare the extracted EPS to the manually input range
                if eps_low < manual_eps_low and eps_high < manual_eps_high:
                    # Place a short order on APOG
                    place_short_order(ticker_to_short, 10000)  # Shorting 10,000 shares of APOG
                    return True, eps_low, eps_high
                else:
                    print(f"EPS Outlook is within or above expected range: {eps_low} to {eps_high}")
                    return False, eps_low, eps_high
    return False, None, None

# Function to place a short order on Alpaca
def place_short_order(symbol, qty):
    order_url = f'{alpaca_base_url}/v2/orders'
    order_data = {
        "symbol": symbol,
        "qty": qty,
        "side": "sell",  # This is "sell" for a short order in Alpaca
        "type": "market",
        "time_in_force": "gtc"  # Good till canceled
    }
    
    # Send POST request to place the order
    response = requests.post(order_url, json=order_data, headers=alpaca_headers)
    
    if response.status_code == 200:
        print(f"Short order placed for {symbol}.")
    else:
        print(f"Failed to place short order. Status code: {response.status_code}")

# Function to loop and check for the article and EPS outlook
def check_for_earnings_release():
    while True:
        article_url = fetch_article_links()
        if article_url:
            print(f"Earnings release found: {article_url}")
            short_placed, eps_low, eps_high = fetch_article_details(article_url)
            
            if short_placed:
                print(f"Short order placed due to EPS outlook of {eps_low} to {eps_high}.")
                break  # Break the loop if short order is placed
            else:
                print(f"No short order placed. EPS was {eps_low} to {eps_high}.")
                break  # Break the loop if earnings release was found but no short order needed
        time.sleep(1)  # Check every second

# Start checking for the earnings release
check_for_earnings_release()
