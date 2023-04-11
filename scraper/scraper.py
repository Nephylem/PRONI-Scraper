import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd


headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    # Requests sorts cookies= alphabetically
    # 'Cookie': 'ASP.NET_SessionId=mjtngd1z3syqkrrlo0er3okn; NIGOV_.proni.gov.uk_%2F_wat=AAAAAAUh2Mp9PF4OER2E7KIt8xAfLWJRmBdfDpFZwB53BZgn5UPXMmCGIcuNykhxYFh58H40w5u7Fi_sjSWihE5RghfNl4VSGWVZPVVLkn0r0N1Itg==&; _ga=GA1.3.741574887.1666847210; _gid=GA1.3.375659784.1666847210; NIGOV=M4n50jQ9pontK7ws5RukoEjBgyg0002; _gat_gtag_UA_106409400_5=1; WT_FPC=id=227047ed03c74a03bfd1666818432232:lv=1666835719576:ss=1666827705806',
    'Origin': 'https://apps.proni.gov.uk',
    'Referer': 'https://apps.proni.gov.uk/Val12B/Search.aspx',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="107", "Chromium";v="107", "Not=A?Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    }

def crawl_site(country, parish):
    
    print(f"Crawling \"{country} - {parish}\" term.")
    payload = {}
    with requests.Session() as session:
        req = session.get(url='https://apps.proni.gov.uk/Val12B/Search.aspx', headers=headers)

        soup = BeautifulSoup(req.content, 'html.parser')
        payload['__LASTFOCUS'] = soup.select_one('#__LASTFOCUS')['value']
        payload['__VIEWSTATE'] = soup.select_one('#__VIEWSTATE')['value']
        payload['__VIEWSTATEGENERATOR'] = soup.select_one('#__VIEWSTATEGENERATOR')['value']
        payload['__EVENTARGUMENT'] = soup.select_one('#__EVENTARGUMENT')['value']
        payload['__EVENTTARGET'] = soup.select_one('#__EVENTTARGET')['value']
        payload['__EVENTVALIDATION'] = soup.select_one('#__EVENTVALIDATION')['value']
        payload['__SCROLLPOSITIONX'] = soup.select_one('#__SCROLLPOSITIONX')['value']
        payload['__SCROLLPOSITIONY'] = soup.select_one('#__SCROLLPOSITIONY')['value']
        payload['_ctl0:content:txtSearch'] = ""
        payload['_ctl0:content:grpMatch'] = 'rbMatchAll'
        payload['_ctl0:content:ddlCounty'] = country
        payload['_ctl0:content:ddlParish'] = parish

        post = session.post(url='https://apps.proni.gov.uk/Val12B/Search.aspx', data=payload, headers=headers)

        soup = BeautifulSoup(post.text, 'html.parser')
        payload.update({
            "__LASTFOCUS" : soup.select_one('#__LASTFOCUS')['value'],
            "__VIEWSTATE" : soup.select_one('#__VIEWSTATE')['value'],
            "__VIEWSTATEGENERATOR" : soup.select_one('#__VIEWSTATEGENERATOR')['value'],
            "__EVENTARGUMENT" : soup.select_one('#__EVENTARGUMENT')['value'],
            "__EVENTTARGET" : soup.select_one('#__EVENTTARGET')['value'],
            "__EVENTVALIDATION" : soup.select_one('#__EVENTVALIDATION')['value'],
            "__SCROLLPOSITIONX" : soup.select_one('#__SCROLLPOSITIONX')['value'],
            "__SCROLLPOSITIONY" : soup.select_one('#__SCROLLPOSITIONY')['value'],
            '_ctl0:content:btnSearch': 'Search',})
        post = session.post(url='https://apps.proni.gov.uk/Val12B/Search.aspx', data=payload, headers=headers)

        crawl_table(post, session, payload)
    

def extract_imglink(post, session):

    row = 0
    base_url = 'https://apps.proni.gov.uk'

    soup = BeautifulSoup(post.content, 'html.parser')
    
    element_name_list = [value['name'] for value in soup.select('td input')]
    element_value_list = [value['value'] for value in soup.select('td input')]
    
    def update_payload(soup, name=None, value=None, img_page=False):
        payload = {}     
        if img_page: 
            payload.update({
                "__VIEWSTATE" : soup.select_one('#__VIEWSTATE')['value'],
                "__VIEWSTATEGENERATOR" : soup.select_one('#__VIEWSTATEGENERATOR')['value'],
                "__VIEWSTATEENCRYPTED": '',
                "__EVENTARGUMENT" : soup.select_one('#__EVENTARGUMENT')['value'],
                "__EVENTTARGET" : soup.select_one('#__EVENTTARGET')['value'],
                "__EVENTVALIDATION" : soup.select_one('#__EVENTVALIDATION')['value'],
                "__SCROLLPOSITIONX" : soup.select_one('#__SCROLLPOSITIONX')['value'],
                "__SCROLLPOSITIONY" : soup.select_one('#__SCROLLPOSITIONY')['value'],
                '_ctl0:content:NextBtn': 'Next'
                })
        else:
            payload.update({
            "__VIEWSTATE" : soup.select_one('#__VIEWSTATE')['value'],
            "__VIEWSTATEGENERATOR" : soup.select_one('#__VIEWSTATEGENERATOR')['value'],
            "__VIEWSTATEENCRYPTED": '',
            "__EVENTARGUMENT" : soup.select_one('#__EVENTARGUMENT')['value'],
            "__EVENTTARGET" : soup.select_one('#__EVENTTARGET')['value'],
            "__EVENTVALIDATION" : soup.select_one('#__EVENTVALIDATION')['value'],
            "__SCROLLPOSITIONX" : soup.select_one('#__SCROLLPOSITIONX')['value'],
            "__SCROLLPOSITIONY" : soup.select_one('#__SCROLLPOSITIONY')['value'],
            name : value,
            })
        return payload
    def scrape_img(payload):
        img_list = []
        post = session.post(url='https://apps.proni.gov.uk/Val12B/SearchResults.aspx', headers=headers, data=payload)
        soup = BeautifulSoup(post.content, 'html.parser')
        img_element = soup.select_one('#divPrint img')['src']
        img_link = urljoin(base_url, img_element)
        img_list.append(img_link)
        img_count = 0
        nextpage_visible = True
        while nextpage_visible:
            payload = update_payload(soup, img_page=True)

            # uncomment below line if you want to scrape all the images
            # if soup.select_one('input[name="_ctl0:content:NextBtn"]'):
            if img_count != 3:
                post = session.post(url='https://apps.proni.gov.uk/Val12B/ImageResult.aspx', headers=headers, data=payload)
                soup = BeautifulSoup(post.content, 'html.parser')
                
                img_element = soup.select_one('#divPrint img')['src']
                img_link = urljoin(base_url, img_element)
                img_list.append(img_link)
                img_count +=1
            else:
                print(f"scraped img in row...")
                nextpage_visible = False
        return img_list

    payload_list = []
    for x in range(len(element_name_list)):
        name = element_name_list[x]
        value = element_value_list[x]
        payload = update_payload(soup, name, value)
        
        payload_list.append(payload)
    
    img_array = []
    for payload in payload_list:
        # Img first page
        row+=1
        img_list = []
        post = session.post(url='https://apps.proni.gov.uk/Val12B/SearchResults.aspx', headers=headers, data=payload)
        soup = BeautifulSoup(post.content, 'html.parser')
        try:
            img_element = soup.select_one('#divPrint img')['src']
            img_link = urljoin(base_url, img_element)
            img_list.append(img_link)
        except:
            post = session.post(url='https://apps.proni.gov.uk/Val12B/SearchResults.aspx', headers=headers, data=payload)
            soup = BeautifulSoup(post.content, 'html.parser')
            img_element = soup.select_one('#divPrint img')['src']
            img_link = urljoin(base_url, img_element)
            img_list.append(img_link)
            print('retry request.. img url found..')

        # Img next page
        img_count = 0
        nextpage_visible = True
        while nextpage_visible:
            payload = update_payload(soup, img_page=True)

            # uncomment below line if you want to scrape all the images
            # if soup.select_one('input[name="_ctl0:content:NextBtn"]'):
            if img_count != 2:
                post = session.post(url='https://apps.proni.gov.uk/Val12B/ImageResult.aspx', headers=headers, data=payload)
                soup = BeautifulSoup(post.content, 'html.parser')
                # try:
                img_element = soup.select_one('#divPrint img')['src']
                img_link = urljoin(base_url, img_element)
                img_list.append(img_link)
                img_count +=1
                    
                # except:
                #     post = session.post(url='https://apps.proni.gov.uk/Val12B/ImageResult.aspx', headers=headers, data=payload)
                #     soup = BeautifulSoup(post.content, 'html.parser')
                #     img_element = soup.select_one('#divPrint img')['src']
                #     img_link = urljoin(base_url, img_element)
                #     img_list.append(img_link)
                #     print('retry request.. img url found..')
                #     img_count +=1
                   
            else:
                print(f"scraped img in row: {row}...")
                nextpage_visible = False
        img_array.append(img_list)
    return img_array
           

def crawl_table(post, session, payload):
    list_dataframe = []
    page = 0
    soup = BeautifulSoup(post.content, 'html.parser')
    dataframe = pd.read_html(post.content)
    dataframe[0]['PRONI REFERENCE'] = [value['value'] for value in soup.select('input#linkImage.ButtonAsLink')]
    dataframe[0]['IMG URLS'] = extract_imglink(post, session)
    list_dataframe.append(dataframe)
    page += 1
    print(f'scraped tables in page {page}...')

    payload.pop('_ctl0:content:txtSearch')
    payload.pop('_ctl0:content:grpMatch')
    payload.pop('_ctl0:content:ddlCounty')
    payload.pop('_ctl0:content:ddlParish')
    payload.pop('_ctl0:content:btnSearch')
    def update_payload(payload):
        payload.update({
            "__VIEWSTATE" : soup.select_one('#__VIEWSTATE')['value'],
            "__VIEWSTATEGENERATOR" : soup.select_one('#__VIEWSTATEGENERATOR')['value'],
            "__VIEWSTATEENCRYPTED": '',
            "__EVENTARGUMENT" : soup.select_one('#__EVENTARGUMENT')['value'],
            "__EVENTTARGET" : soup.select_one('#__EVENTTARGET')['value'],
            "__EVENTVALIDATION" : soup.select_one('#__EVENTVALIDATION')['value'],
            "__SCROLLPOSITIONX" : soup.select_one('#__SCROLLPOSITIONX')['value'],
            "__SCROLLPOSITIONY" : soup.select_one('#__SCROLLPOSITIONY')['value'], 
            '_ctl0:content:NextBtn': 'Next',
            })
        return payload

    nextpage_visible = True
    while nextpage_visible: 
       
        payload = update_payload(payload)
        if soup.select_one('input[title="Next"]'):
            page += 1

            post = session.post(url='https://apps.proni.gov.uk/Val12B/SearchResults.aspx', data=payload, headers=headers)
            soup = BeautifulSoup(post.content, 'html.parser')
            # try:
            next_dataframe = pd.read_html(post.content)
            next_dataframe[0]['PRONI REFERENCE'] = [value['value'] for value in soup.select('input#linkImage.ButtonAsLink')]
            next_dataframe[0]['IMG URLS'] = extract_imglink(post, session)
            list_dataframe.append(next_dataframe)
            print(f'scraped tables in page {page}...')
               
            # except:
            #     post = session.post(url='https://apps.proni.gov.uk/Val12B/SearchResults.aspx', data=payload, headers=headers)
            #     soup = BeautifulSoup(post.content, 'html.parser')
            #     next_dataframe = pd.read_html(post.content)
            #     next_dataframe[0]['PRONI REFERENCE'] = [value['value'] for value in soup.select('input#linkImage.ButtonAsLink')]
            #     next_dataframe[0]['IMG URLS'] = extract_imglink(post, session)
            #     list_dataframe.append(next_dataframe)
            #     print('PRONI REFERENCE column recaptured.. Done!')
            #         
        else:
            nextpage_visible = False
            print('Finished!')

    merge_dataframe = pd.concat([pd.DataFrame(data[0]) for data in list_dataframe], ignore_index=True)
    parish_name = merge_dataframe['PARISH'][0]
    county_name = merge_dataframe['COUNTY'][0]
    merge_dataframe.to_csv(f'{parish_name}_{county_name}.csv', index=False)
