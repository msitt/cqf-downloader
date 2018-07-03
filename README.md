# CQF Downloader

Downloads materials from the CQF portal.

## Requirements

This script was written for Python 3.x. This script depends on the requests and BeautifulSoup libraries.

## Usage

Example usage:
```
with CqfDownloader(outdir) as cqf:
    cqf.login(username, password)
    cqf.download_study_materials(num_workers=8)
```

Alternatively, you can run `CqfDownloader` as a script. To do this, set the parameters in `config.py` and run
```
python CqfDownloader.py
```
