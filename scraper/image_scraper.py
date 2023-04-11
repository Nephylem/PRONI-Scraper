import requests
import pandas as pd
from os import makedirs, listdir
from os.path import basename, join
import re
from concurrent.futures import ThreadPoolExecutor

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

def download_url(url): 
    headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'If-Modified-Since': 'Wed, 06 Mar 2013 18:08:36 GMT',
    'If-None-Match': '"23aafe9e951ace1:0"',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="107", "Chromium";v="107", "Not=A?Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    }
    try:
        with requests.session() as session:
            request = session.get(url)
            return request

    except:
        return None

def save_file(url, data, path):
  
    filename = basename(url)
    outpath = join(path, filename)
    with open(outpath, 'wb') as file:
        for content in data.iter_content(1000):
            file.write(content)
    return outpath

def download_and_save(url, path):

    data = download_url(url)
    outpath = save_file(url, data, path)
    if data is None:
        print(f'>Error downloading {url}')
        with open('image_logs.txt', 'a') as file:
            file.write(f'>Error downloading {url} from file {outpath}' + '\n')
            file.close()
        return

    print(f'>Saved {url} to {outpath}')
    # with open('image_logs.txt', 'w') as file:
    #     file.write(f'>Saved {url} to {outpath}' + '\n')
    #     file.close()

def download_docs(urls, path):
    # create the local directory, if needed
    makedirs(path, exist_ok=True)
    # create the thread pool
    n_threads = len(urls)
    with ThreadPoolExecutor(n_threads) as executor:
        # download each url and save as a local file
        _ = [executor.submit(download_and_save, url, path) for url in urls]


def download_dataframe(dataframe, path, start=0, stop=5):
    pattern = re.compile("http[s]*\S+\s\S+g")
    img_links = dataframe['IMG URLS']
    
    for index in range(start, stop):
        img_list = pattern.findall(img_links[index])
        img_folder = index + 1
        download_docs(urls=img_list, path=f'{path}/{img_folder}')
    
def image_counter(path): 
    images_path = [join(path, img) for img in listdir(path)]
    image_count = [count for img in images_path for count in listdir(img)] 
    
    print(f"> Total scraped images {len(image_count)}..")
    

def imgcount_column(dataframe):
    pattern = re.compile("http[s]*\S+\s\S+g")
    img_links = dataframe['IMG URLS']
    dataframe['IMAGES COUNT'] = [len(pattern.findall(link)) for link in img_links]
    return dataframe

# for merging all the terms collected
# dataframe_list = [pd.read_csv(join('A_L/', data)) for data in listdir('A_L')]
# merged_dataframe = pd.concat(dataframe_list, ignore_index=True)
# merged_dataframe.sort_values(by='PARISH').reset_index().drop(columns='index').to_csv('A-L.csv', index=False)


PATH = 'A-L_img_by_index'
drive_d = 'D:\scraped_data\A-L_img_by_index'
merged_dataframe = pd.read_csv('A-L.csv') 
# download_dataframe(merged_dataframe, PATH, start=24365, stop=58088)

# a function to count all scraped images
image_counter(PATH)

# a function to add images count column in merged dataframe

print(len(merged_dataframe))



# records = imgcount_column(merged_dataframe)

# records.to_csv('records.csv', index=False)

# sliced_dataframe = merged_dataframe[:5000]
# sliced_dataframe.to_csv('records.csv', index=False)