from scraper import crawl_site
import pandas as pd

from concurrent.futures import ThreadPoolExecutor


if __name__ == '__main__':

    df = pd.read_csv('COUNTY_PARISH.csv')
    remove_list = []
    data = df[~df.PARISH.isin(remove_list)]

    # update the slicing depending on how many terms you want to scrape
    reverse_data = data.sort_values(by='PARISH').reset_index().drop(columns='index').iloc[100:234,:]
    
    parish = []
    country = []
    for country_name in reverse_data.COUNTY.unique():
        df_country = reverse_data[reverse_data.COUNTY == str(country_name)]
        for parish_name in df_country.PARISH.unique():
            country.append(country_name)
            parish.append(parish_name)
    
    # # write a checkpoint for current terms
    with open('scraper/terms_checkpoint.txt', 'w') as write:
        for terms in map(lambda x,y: f"{x}_{y}\n", parish, country):
            write.write(terms)
        write.close()

    # prints the current terms to scrape
    print(reverse_data)

    # comment the code below if you want to check the terms before scraping.
    MAX_WORKERS = len(parish)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for result in executor.map(crawl_site, country, parish):
            print(result)

            