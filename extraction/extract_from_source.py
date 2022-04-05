import logging
import requests
from io import StringIO

def download_file(ip_url: str, ip_encoding: str):
    # Function to download different types of files
    logging.info("Download file start")
    data = requests.get(ip_url).content
    data = str(data, ip_encoding)
    f = StringIO()
    f = data
    logging.info("Download file end")
    return f
