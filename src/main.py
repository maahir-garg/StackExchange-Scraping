import os
import py7zr
import requests
import pandas as pd
import xml.etree.ElementTree as ET

# Define file paths
FILES = {
    'posts': 'Posts.xml',
    'users': 'Users.xml',
    'votes': 'Votes.xml',
    'comments': 'Comments.xml',
    'badges': 'Badges.xml'
}


# Function to download a file from a given URL
def download_file(url, local_filename):
    """Download a file from a given URL and save it locally."""
    print(f"Started download: {local_filename}")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(local_filename, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
    print(f"Downloaded file: {local_filename}")


# Function to extract a .7z file
def extract_7z_file(archive_path, extract_to='.'):
    """Extract a .7z file to the specified directory."""
    with py7zr.SevenZipFile(archive_path, mode='r') as archive:
        archive.extractall(path=extract_to)
    print(f"Extracted contents to: {extract_to}")


# Function to convert xml file into dataframe
# Handles parsing error
def xml_to_df(xml_file):
    records = []
    try:
        # Open the file with UTF-8 encoding to avoid encoding issues
        with open(xml_file, "r", encoding="utf-8") as file:
            tree = ET.parse(file)
            print(f"Extracting {xml_file}")
            root = tree.getroot()
            records = [elem.attrib for elem in root]
    except ET.ParseError as e:
        print(f"ParseError: {e} in file {xml_file}. Attempting to skip problematic lines.")
        with open(xml_file, "r", encoding="utf-8", errors="ignore") as file:
            lines = file.readlines()
        for i, line in enumerate(lines):
            try:
                # Try parsing each line individually
                ET.fromstring(line)
                records.append(ET.fromstring(line).attrib)
            except ET.ParseError:
                print(f"Skipping line {i + 1} due to ParseError.")

    return pd.DataFrame(records)


# Function to aggregate votes
def count_votes(group):
    """Count votes of different types."""
    return pd.Series({
        'Acceptance': (group['VoteTypeId'] == '1').sum(),
        'Upvotes': (group['VoteTypeId'] == '2').sum(),
        'Downvotes': (group['VoteTypeId'] == '3').sum()
    })


# Function to:
# 1. Download zipped files from stackexchange into folder ZippedFiles
# 2. Unzip the zipped files into folder UnzippedFiles
def download_and_unzip(platform_name):
    url = f'https://archive.org/download/stackexchange_20240630/stackexchange_20240630/{platform_name}.com.7z'
    zipped_dir = '../ZippedFiles'
    local_filename = os.path.join(zipped_dir, f'{platform_name}.7z')
    extract_directory = f'../UnzippedFiles/{platform_name}'

    # Ensure the ZippedFiles and extraction directories exist
    os.makedirs(zipped_dir, exist_ok=True)
    os.makedirs(extract_directory, exist_ok=True)

    # Download and extract the .7z file
    download_file(url, local_filename)

    # Extract the downloaded .7z file
    extract_7z_file(local_filename, extract_directory)


# Main function to convert XML files to CSV
# The csv files can be found in folder called csv_files
def xml_to_csv(data_directory, platform_name):
    extract_directory = os.path.join(data_directory, platform_name)
    download_directory = f'../csv_files'

    # Ensure the download directory exists
    os.makedirs(download_directory, exist_ok=True)

    # Read and process each XML file into DataFrames
    dfs = {
        key: xml_to_df(os.path.join(extract_directory, value))
        for key, value in FILES.items()
        if os.path.exists(os.path.join(extract_directory, value))
    }

    # Ensure Posts.xml is available
    if 'posts' not in dfs:
        print(f"Error: 'Posts.xml' not found in {extract_directory}")
        return

    posts_df = dfs['posts']
    users_df = dfs.get('users', pd.DataFrame())  # Handle missing files gracefully
    votes_df = dfs.get('votes', pd.DataFrame())
    comments_df = dfs.get('comments', pd.DataFrame())
    badges_df = dfs.get('badges', pd.DataFrame())

    # Create the base DataFrame from posts_df
    final_df = posts_df[['Id', 'PostTypeId', 'ParentId', 'CreationDate', 'ViewCount', 'OwnerUserId', 'Tags']].copy()
    final_df.rename(columns={'Id': 'PostId', 'OwnerUserId': 'UserId'}, inplace=True)

    # Merge with users_df
    if not users_df.empty:
        users_df = users_df[['Id', 'Reputation', 'CreationDate', 'LastAccessDate']]
        users_df.rename(columns={'CreationDate': 'UserDate', 'LastAccessDate': 'LastAccess'}, inplace=True)
        final_df = final_df.merge(users_df, left_on='UserId', right_on='Id', how='left').drop(columns=['Id'])

    # Aggregate votes by PostId
    if not votes_df.empty:
        votes_agg = votes_df.groupby('PostId').apply(count_votes).reset_index()
        final_df = final_df.merge(votes_agg, on='PostId', how='left')

    # Aggregate comments by PostId
    if not comments_df.empty:
        comments_agg = comments_df.groupby('PostId').size().reset_index(name='Comments')
        final_df = final_df.merge(comments_agg, on='PostId', how='left')

    # Aggregate badges by UserId
    if not badges_df.empty:
        badges_agg = badges_df.groupby('UserId')['Id'].count().reset_index(name='Badges')
        final_df = final_df.merge(badges_agg, on='UserId', how='left')

    # Add platform source and fill missing values
    final_df['SourceId'] = platform_name

    desired_order = [
        'PostId', 'PostTypeId', 'CreationDate', 'UserId', 'Tags', 'Comments',
        'ParentId', 'Acceptance', 'Upvotes', 'Downvotes', 'UserDate', 'LastAccess',
        'Reputation', 'Badges', 'ViewCount', 'SourceId'
    ]

    final_df = final_df[desired_order]

    # Save the CSV file in the download_directory
    output_file = os.path.join(download_directory, f'{platform_name}.csv')
    final_df.to_csv(output_file, index=False)
    print(f"CSV file saved: {output_file}")


# MAIN FUNCTION TO:
## 1. Download zipped files from stackexchange into folder ZippedFiles
## 2. Unzip the zipped files into folder UnzippedFiles
## 3. Converts the xml files in UnzippedFiles into csv files in folder csv_files
### Steps 1-2 are done using function download_and_unzip(platform_name)
### Step 3 is done using function xml_to_csv(data_directory, platform_name)
def main():
    data_directory = '../UnzippedFiles' # Specify location of the xml files are
    url_csv = pd.read_csv("stackexchange_download_links.csv")

    for i in range(241, len(url_csv["Platform Name"])):
        platform_name = url_csv.iloc[i, 1]
        print(platform_name + " start")
        if i == 319: # StackOverflow.com.7z was skipped as per instruction
            continue

        # Download and extract the .7z files into xml files
        download_and_unzip(platform_name)

        # Convert extracted files from XML to CSV
        xml_to_csv(data_directory, platform_name)
        print(platform_name + " finish")


if __name__ == "__main__":
    main()
