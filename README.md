# CQF Downloader

Downloads materials from the CQF portal.

## Requirements

  * Python 3.6 or greater
  * requests
  * BeautifulSoup

## Usage

Example usage:
```
with CqfDownloader() as cqf:
    cqf.login(username, password)
    cqf.download_study_materials(num_workers=8)
```

Alternatively, you can run `CqfDownloader` as a script. To do this, set the parameters in `config.py` and run
```
python CqfDownloader.py
```
