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


def xml_to_df(xml_file):
    records = []
    try:
        tree = ET.parse(xml_file)
        print(f"Extracting {xml_file}")
        root = tree.getroot()
        records = [elem.attrib for elem in root]
    except ET.ParseError as e:
        print(
            f"ParseError: {e} in file {xml_file}. Attempting to skip problematic lines."
        )
        # Attempt to skip the problematic line
        with open(xml_file, "r") as file:
            lines = file.readlines()
        for i, line in enumerate(lines):
            try:
                # Try parsing the line as XML
                ET.fromstring(line)
                records.append(ET.fromstring(line).attrib)
            except ET.ParseError:
                print(f"Skipping line {i + 1} due to ParseError.")

    return pd.DataFrame(records)


def count_votes(group):
    """Count votes of different types."""
    return pd.Series({
        'Acceptance': (group['VoteTypeId'] == '1').sum(),
        'Upvotes': (group['VoteTypeId'] == '2').sum(),
        'Downvotes': (group['VoteTypeId'] == '3').sum()
    })


def download_and_unzip(PLATFORM_NAME):
    url = f'https://archive.org/download/stackexchange_20240630/stackexchange_20240630/{PLATFORM_NAME}.com.7z'
    local_filename = f'../ZippedFiles/{PLATFORM_NAME}.7z'
    extract_directory = f'../UnzippedFiles/{PLATFORM_NAME}'

    # Download and extract the .7z file
    download_file(url, local_filename)

    # Ensure the extraction directory exists
    os.makedirs(extract_directory, exist_ok=True)

    # Extract the downloaded .7z file
    extract_7z_file(local_filename, extract_directory)


def xml_to_csv(platform_name):
    extract_directory = f'../UnzippedFiles/{platform_name}'

    dfs = {
        key: xml_to_df(os.path.join(extract_directory, value))
        for key, value in FILES.items()
        if os.path.exists(os.path.join(extract_directory, value))
    }

    posts_df = dfs['posts']
    users_df = dfs['users']
    votes_df = dfs['votes']
    comments_df = dfs['comments']
    badges_df = dfs['badges']

    # Create the base DataFrame from posts_df
    final_df = posts_df[['Id', 'PostTypeId', 'ParentId', 'CreationDate', 'ViewCount', 'OwnerUserId', 'Tags']].copy()
    final_df.rename(columns={'Id': 'PostId', 'OwnerUserId': 'UserId'}, inplace=True)

    # Merge with users_df
    users_df = users_df[['Id', 'Reputation', 'CreationDate', 'LastAccessDate']]
    users_df.rename(columns={'CreationDate': 'UserDate', 'LastAccessDate': 'LastAccess'}, inplace=True)
    final_df = final_df.merge(users_df, left_on='UserId', right_on='Id', how='left').drop(columns=['Id'])

    # Aggregate votes by PostId
    votes_agg = votes_df.groupby('PostId').apply(count_votes).reset_index()
    final_df = final_df.merge(votes_agg, on='PostId', how='left')

    # Aggregate comments by PostId
    comments_agg = comments_df.groupby('PostId').size().reset_index(name='Comments')
    final_df = final_df.merge(comments_agg, on='PostId', how='left')

    # Aggregate badges by UserId
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

    final_df.to_csv(f'../Output/{platform_name}.csv', index=False)


def main():
    url_csv = pd.read_csv("stackexchange_download_links.csv")

    # platform_name_to_csv("android.meta.stackexchange")
    for i in range(len(url_csv["Platform Name"])):
        platform_name = url_csv.iloc[i, 1]
        if platform_name[:1] == "h":
            break
        print(f"{i}: {platform_name}")

        # to convert extracted files from xml to csv
        xml_to_csv(platform_name)

        # to download and extract the .7z files into xml files
        # download_and_unzip(platform_name)

        print("---"*20)
    return None


main()
