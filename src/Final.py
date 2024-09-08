import os
import py7zr
import requests
import pandas as pd
import xml.etree.ElementTree as ET


# Function to download a file from a given URL
def download_file(url, local_filename):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    print(f"Downloaded file: {local_filename}")
    return local_filename

# Function to extract a .7z file
def extract_7z_file(archive_path, extract_to='.'):
    with py7zr.SevenZipFile(archive_path, mode='r') as archive:
        archive.extractall(path=extract_to)
    print(f"Extracted contents to: {extract_to}")

platform_name = "aviation.meta.stackexchange"
# Define the URL of the .7z file and local paths
url = 'https://archive.org/download/stackexchange_20240630/stackexchange_20240630/aviation.meta.stackexchange.com.7z'  # Replace with your URL
local_filename = f'{platform_name}.7z'
extract_directory = f'./{platform_name}'  # Folder to extract the files

# Download the file
download_file(url, local_filename)

# Ensure the extraction directory exists
if not os.path.exists(extract_directory):
    os.makedirs(extract_directory)

# Extract the downloaded .7z file
extract_7z_file(local_filename, extract_directory)

# Define the file paths (update these paths accordingly)
posts_file = './extracted_files/Posts.xml'
users_file = './extracted_files/Users.xml'
votes_file = './extracted_files/Votes.xml'
comments_file = './extracted_files/Comments.xml'
badges_file = './extracted_files/Badges.xml'


# Helper function to parse XML and convert it into a DataFrame
def xml_to_df(xml_file, element_name):
    print(f"Extracting {xml_file}")
    tree = ET.parse(xml_file)
    root = tree.getroot()
    all_records = []
    for elem in root:
        record = {}
        for child in elem.attrib:
            record[child] = elem.attrib[child]
        all_records.append(record)
    return pd.DataFrame(all_records)


# Load XML files into DataFrames
posts_df = xml_to_df(posts_file, 'row')
users_df = xml_to_df(users_file, 'row')
votes_df = xml_to_df(votes_file, 'row')
comments_df = xml_to_df(comments_file, 'row')
badges_df = xml_to_df(badges_file, 'row')

# 1. Base dataframe from posts_df
final_df = posts_df[['Id', 'PostTypeId', 'ParentId', 'CreationDate', 'ViewCount', 'OwnerUserId', 'Tags']].copy()
final_df.rename(columns={'Id': 'PostId', 'OwnerUserId': 'UserId'}, inplace=True)
# 2. Merge posts_df with users_df
final_df = final_df.merge(users_df[['Id', 'Reputation', 'CreationDate', 'LastAccessDate']], left_on='UserId', right_on='Id', how='left')
final_df.rename(columns={'CreationDate_y': 'UserDate', 'LastAccessDate': 'LastAccess'}, inplace=True)
final_df.drop(columns=['Id'], inplace=True)

# 3. Aggregate votes by PostId
def count_votes(group):
    return pd.Series({
        'Acceptance': (group['VoteTypeId'] == '1').sum(),
        'Upvotes': (group['VoteTypeId'] == '2').sum(),
        'Downvotes': (group['VoteTypeId'] == '3').sum()
    })

# Group by PostId and count votes
result = votes_df.groupby('PostId').apply(count_votes).reset_index()

# Merge the aggregated vote counts into the final dataframe
final_df = final_df.merge(result, left_on='PostId', right_on='PostId', how='left')
print(final_df)
print(final_df.columns)

# 4. Aggregate comments by PostId
comments_agg = comments_df.groupby('PostId').size().reset_index(name='Comments')
# Merge comments data
final_df = final_df.merge(comments_agg, left_on='PostId', right_on="PostId", how='left')

# 5. Aggregate badges by UserId
badges_agg = badges_df.groupby('UserId')['Id'].count().reset_index().rename(columns={'Id': 'Badges'})

# Merge badge data
final_df = final_df.merge(badges_agg, on='UserId', how='left')

# Add platform source and fill missing values
final_df['SourceId'] = platform_name  # Add your platform name here
final_df.fillna(0, inplace=True)

# Final DataFrame is ready
final_df.head()

print(final_df.head())
print(final_df.columns)
# Optionally, save to CSV
final_df.to_csv(f'{platform_name}.csv', index=False)
