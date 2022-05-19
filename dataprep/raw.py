# module to download raw files into a data/ folder (not uploaded to repo)
# importing necessary modules
import requests, zipfile, json
from io import BytesIO

with open('./dataprep/files.json','r') as f:
    well_file_urls = json.load(f)

for k,v in well_file_urls.items():
    try:
        # download the file via URL (SSL certificate isn't valid)
        req = requests.get(v, verify=False)

        # extract the zip file contents
        zfile = zipfile.ZipFile(BytesIO(req.content))
        zfile.extractall('./data/')
    except Exception:
        continue