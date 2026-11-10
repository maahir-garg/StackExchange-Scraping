# StackExchange scraping

Python scripts that download the public StackExchange data dumps from the Internet Archive, extract the `.7z` archives, and convert the XML files into one CSV per site. They were built to prepare data for a study of Stack Overflow answer quality.

## Context and status

The scripts were written in September 2024 as research-support tooling. They are kept for reference and are not maintained. They target the 2024-06-30 dump (`stackexchange_20240630` on archive.org).

## What is in the repo

- `src/extract_links.py`: fetches the archive.org page for the dump, collects the download links for the `.7z` files, derives a platform name from each link, and writes `stackexchange_download_links.csv`.
- `src/stackexchange_download_links.csv`: the output of that script, with the columns `Download Links` and `Platform Name` and 361 rows. It is committed so that `main.py` can run without calling `extract_links.py` first.
- `src/main.py`: downloads and extracts the archives, and converts the extracted XML to CSV.
- `requirements.txt`: the third-party libraries the scripts import.
- `.idea/`: PyCharm project settings.

## Requirements and setup

Python 3 and the packages in `requirements.txt` (`beautifulsoup4`, `pandas`, `py7zr`, `requests`).

```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

No environment variables or credentials are used.

## Usage

Run both scripts from inside `src/`, because `main.py` reads the links file from the current directory and uses paths relative to the parent directory.

Refresh the links file (optional, since a copy is committed):

```
python extract_links.py
```

Download, extract and convert with `main.py`:

```
python main.py
```

The script runs `main()` as soon as it is executed. For each platform in the links file, in order, it does the following.

- Download: `download_and_unzip(platform_name)` fetches `https://archive.org/download/stackexchange_20240630/stackexchange_20240630/<platform>.com.7z` into `../ZippedFiles/` and extracts it into `../UnzippedFiles/<platform>/`. The call is commented out in `main()`, so uncomment it for a first run. The directories `../ZippedFiles` and `../Output` are not created by the script and must exist beforehand.
- Convert: `xml_to_csv(platform_name)` reads `Posts.xml`, `Users.xml`, `Votes.xml`, `Comments.xml` and `Badges.xml` from the extracted folder, and writes `../Output/<platform>.csv`.
- The output has one row per post, with the columns `PostId`, `PostTypeId`, `CreationDate`, `UserId`, `Tags`, `Comments`, `ParentId`, `Acceptance`, `Upvotes`, `Downvotes`, `UserDate`, `LastAccess`, `Reputation`, `Badges`, `ViewCount` and `SourceId`. `Comments` is the number of comments on the post, `Acceptance`, `Upvotes` and `Downvotes` are the counts of vote types 1, 2 and 3 on the post, `Badges` is the number of badges held by the post owner, and `Reputation`, `UserDate` and `LastAccess` come from the owner's user record.
- If an XML file fails to parse, the script retries it line by line and skips lines that do not parse.

## Notes and limits

- As committed, the loop in `main()` stops at the first platform whose name starts with the letter "h". With the committed links file that is the 146th entry (`ham.meta.stackexchange`), so only the platforms listed before it are converted. The break condition has to be changed to process the rest, including `stackoverflow`.
- `xml_to_csv` expects all five XML files to be present and raises an error if one is missing.
- The data dumps are published by StackExchange through the Internet Archive and come with terms stated on that page, which you should check before reuse. The dumps are large, and the extracted XML and generated CSVs are not part of this repository (`.gitignore` lists `/ZippedFiles` and `/UnzippedFiles`).
- Keep the request rate low. `extract_links.py` depends on the CSS classes used on the archive.org page at the time it was written.
