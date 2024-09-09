import requests
from bs4 import BeautifulSoup
import pandas as pd

# URL of the webpage
url = 'https://archive.org/details/stackexchange_20240630'

# Send a GET request to fetch the raw HTML content
response = requests.get(url)

# Parse the content with BeautifulSoup
soup = BeautifulSoup(response.text, 'html.parser')

# Find all <a> tags with class 'stealth download-pill'
links = soup.find_all('a', class_='stealth download-pill')

# Extract the href attributes and clean up the text
download_links = []
for link in links:
    href = link['href']
    download_links.append(f'https://archive.org{href}')
    print(link)

# Convert to DataFrame
df = pd.DataFrame(download_links, columns=['Download Links'])


def extract_platform_name(url):
    # Split the URL by '/' and get the last part before '.7z'
    platform = url.split('/')[-1].replace('.com.7z', '')
    print(platform)
    return platform


df['Platform Name'] = df['Download Links'].apply(extract_platform_name)

# Save to a CSV file
df.to_csv('stackexchange_download_links.csv', index=False)

print(f"Extracted {len(download_links)} download links and saved them to 'stackexchange_download_links.csv'")
