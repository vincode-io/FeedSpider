import requests
from bs4 import BeautifulSoup
import os
import shutil

# Scrape the file names from the index page
base_url = 'https://dumps.wikimedia.org/enwiki/20200501/'
latest_html = requests.get(base_url).text
soup_dump = BeautifulSoup(latest_html, 'html.parser')

files = []
for file in soup_dump.find_all('li', {'class': 'file'}):
    text = file.text
    if 'pages-articles' in text:
        files.append((text.split()[0], text.split()[1:]))

files_to_download = [file[0] for file in files if '.xml-p' in file[0]]

# Create the download directory
wikipedia_data_dir = 'wikipedia_data'
os.makedirs(wikipedia_data_dir, exist_ok=True)

# Download the all the files
for file in files_to_download:
    save_file = wikipedia_data_dir + '/' + file

    if not os.path.exists(save_file):
        print('Downloading ' + file)
        download_url = base_url + file

        r = requests.get(download_url, verify=True, stream=True)
        r.raw.decode_content = True

        with open(save_file, 'wb') as out_file:
            shutil.copyfileobj(r.raw, out_file)