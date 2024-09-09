import os
import py7zr
import requests
import pandas as pd
import xml.etree.ElementTree as ET

# Define file paths (update these paths accordingly)
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


# Function to parse XML and convert it into a DataFrame
def xml_to_df(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()

    data = []

    for row in root.findall('row'):  # Assuming the rows are <row> elements
        data.append(row.attrib)  # Extract attributes of each row

    # Convert to DataFrame
    df = pd.DataFrame(data)
    return df


def count_votes(group):
    """Count votes of different types."""
    return pd.Series({
        'Acceptance': (group['VoteTypeId'] == '1').sum(),
        'Upvotes': (group['VoteTypeId'] == '2').sum(),
        'Downvotes': (group['VoteTypeId'] == '3').sum()
    })


def download_and_unzip(PLATFORM_NAME):
    URL = f'https://archive.org/download/stackexchange_20240630/stackexchange_20240630/{PLATFORM_NAME}.com.7z'
    LOCAL_FILENAME = f'../ZippedFiles/{PLATFORM_NAME}.7z'
    EXTRACT_DIRECTORY = f'../UnzippedFiles/{PLATFORM_NAME}'

    # Download and extract the .7z file
    download_file(URL, LOCAL_FILENAME)

    # Ensure the extraction directory exists
    os.makedirs(EXTRACT_DIRECTORY, exist_ok=True)

    # Extract the downloaded .7z file
    extract_7z_file(LOCAL_FILENAME, EXTRACT_DIRECTORY)


def df_to_csv(PLATFORM_NAME):
    EXTRACT_DIRECTORY = f'../UnzippedFiles/{PLATFORM_NAME}'
    # Load XML files into DataFrames
    dfs = {key: xml_to_df(os.path.join(EXTRACT_DIRECTORY, value)) for key, value in FILES.items()}

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
    final_df['SourceId'] = PLATFORM_NAME

    desired_order = [
        'PostId', 'PostTypeId', 'CreationDate', 'UserId', 'Tags', 'Comments',
        'ParentId', 'Acceptance', 'Upvotes', 'Downvotes', 'UserDate', 'LastAccess',
        'Reputation', 'Badges', 'ViewCount', 'SourceId'
    ]

    final_df = final_df[desired_order]

    final_df.to_csv(f'../Output/{PLATFORM_NAME}.csv', index=False)


def main():
    url_csv = pd.read_csv("stackexchange_download_links.csv")

    # platform_name_to_csv("android.meta.stackexchange")
    for i in range(len(url_csv["Platform Name"])):
        platform_name = url_csv.iloc[i, 1]
        print(f"{i}: {platform_name}")
        df_to_csv(platform_name)
        print("---"*20)
    return


main()