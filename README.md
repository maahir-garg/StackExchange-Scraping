# StackExchange XML to CSV Converter (main.py)

This project automates the process of downloading `.7z` archives from StackExchange, extracting XML files, and converting the extracted XML data into CSV files. The goal is to gather various data (posts, users, votes, etc.) from different platforms and format it into a single, structured CSV file for analysis.

## Features

- **Download**: Automatically download `.7z` files containing XML data from a specified URL.
- **Extraction**: Extract the contents of the `.7z` files into a target directory.
- **XML Parsing**: Convert XML data (posts, users, votes, comments, badges) into pandas DataFrames.
- **Aggregation**: Aggregate votes, comments, and badges data.
- **CSV Export**: Export the final aggregated DataFrame to a CSV file.

## Prerequisites

- Python 3.x
- Required Python libraries:
  - `os`
  - `py7zr`
  - `requests`
  - `pandas`
  - `xml.etree.ElementTree`

To install the required libraries, run:
```bash
pip install py7zr requests pandas 
```

## Project Structure

**Posts.xml**: Contains data related to posts.
**Users.xml**: Contains data related to users.
**Votes.xml**: Contains data related to voting activities on posts.
**Comments.xml**: Contains data related to comments.
**Badges.xml**: Contains data related to badges users have received.

## Usage

1. Download and Unzip Files
To download the .7z files from StackExchange and extract their contents:

```python
download_and_unzip(platform_name)
```
#### This function:
1. Downloads the .7z archive file from a predefined URL.
Extracts the contents into the target directory.
2. Convert XML Files to CSV
To convert the extracted XML files into a CSV:

```python
xml_to_csv(platform_name)
```

#### This function:

1. Reads the XML files (posts, users, votes, comments, badges).
2. Aggregates the data by PostId and UserId.
3. Merges the data from different files into a single DataFrame.
4. Exports the final DataFrame to a CSV file.

## Run the Full Pipeline
To run the full pipeline of downloading, extracting, and converting to CSV:

```bash
python main.py
```

Loop through a list of platforms from stackexchange_download_links.csv. 

For each platform:
  - Download and extract XML files. 
  - Convert the XML files to CSV and save the results in the Output directory.

### Error Handling

**Parsing Errors**: If an XML parsing error occurs, the script attempts to skip the problematic line and continue processing the rest of the file.

**File Existence**: The script checks if XML files exist before attempting to process them.

# StackExchange Download Links Scraper (extract_links.py)

This script extracts download links for `.7z` files from the StackExchange archive webpage, specifically from the [StackExchange 20240630 dataset](https://archive.org/details/stackexchange_20240630). \
The links point to compressed data from different platforms in `.7z` format. \
The script also extracts platform names from the links and saves the data in a CSV file.

## Features

- **Web Scraping**: Uses BeautifulSoup to extract all download links from the target webpage.
- **Platform Name Extraction**: Automatically extracts the platform names from the URLs.
- **CSV Export**: The extracted download links and platform names are saved in a CSV file.

## Prerequisites

- Python 3.x
- Required Python libraries:
  - `requests`
  - `beautifulsoup4`
  - `pandas`

To install the required libraries, run:
```bash
pip install requests beautifulsoup4 pandas
```

## Usage
Run the script:

```bash
python stackexchange_scraper.py
```

The script will:
- Fetch the webpage content 
- Extract download links 
- Create a DataFrame with the links and platform names 
- Save the data to 'stackexchange_download_links.csv'

## Output
The script generates a CSV file named 'stackexchange_download_links.csv' with two columns:

**Download Links**: The full URLs for downloading StackExchange data dumps
Platform Name: The name of the StackExchange platform extracted from the URL
