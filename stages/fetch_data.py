'''
# This is fetching using the `datatables.js` server-side at 
# ........ https://datatables.net/
# ........ https://datatables.net/manual/server-side

# This script scrapes the table of datasets
# ........ at the following site: https://cebs-ext.niehs.nih.gov/datasets/
# ........ As of 01-08-2025, this contains 47 datasets. 
# The outer dataframe will contain the name, group, description, and link, of each of # these 47 datasets.

# This outer dataframe is then looped through to retrieve each individual dataset, 
# which are all stored in brick/datasets and backed up using dvc to an amazon s3 bucket.
'''


import numpy as np
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from pathlib import Path
import backoff

# Initial table
def get_html_table(url, get_name_links=False):
    '''
    Given a url, returns dataframe of first table on webpage.

    Arguments
    -----------------
        url : CEBS https://cebs.niehs.nih.gov/cebs/
        get_name_links : Get links of datasets in the name column
        
    Returns
    -----------------
        df : Pandas dataframe listing collection of datasets name, group, description, link, index
        
    Note
    -----------------
    This function does not retrieve full tables on webpages with dynamically rendered content. 
    API calls are used in another function get_html_table_api(...).
    '''
    
    # Send a GET request to the website
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")

        # Find the table on the page
        tables = soup.find_all("table")
        
        if tables:
            # Selects the first table 
            table = tables[0] 
            
            # Extract headers
            headers = [th.text.strip() for th in table.find_all("th")] 
            if get_name_links: 
                headers.append('Link')  # add link column for "Name" links
            
            # Extract rows
            rows = []
            for row in table.find_all("tr")[1:]:  # Skip the header row
                cells = row.find_all("td")
                row_data = [cell.text.strip() for cell in cells]
    
                # Add link column if table has links in the first column to extract
                if get_name_links: 
                    # Extract the link from the 'Name' column
                    name_cell = row.find("td")  # Assuming the 'Name' is in the first column

                    if name_cell and name_cell.find("a"):
                        link = name_cell.find("a")["href"]
                        # print(link)
                        row_data.append(link)  # Append the link
                    else:
                        row_data.append("")  # Append empty if no link
                        
                rows.append(row_data)
            
            # Convert to pandas DataFrame
            df = pd.DataFrame(rows, columns=headers)
            return df
        else:
            print("No table found on the page.")
    else:
        print(f"Failed to fetch the webpage. Status code: {response.status_code}")

    return


# Adding slug for api search later - this is the unique identifier for each dataset
def get_slug(link):
    '''
    Each dataset listed in the initial CEBS table has a unique identifier
    that is passed into the API.
    '''
    i = link.find('search/') + len('search/')
    slug = link[i:]
    return slug


# Individual datasets
# ... Gets list of columns in each dataset
def get_columnList(url):
    '''
    Finds list of columns to pass into the API while fetching a particular dataset
    Note that the columnList passed into the API does not directly match the table headers in HTML, 
    as these are formatted. The columnList is hidden as an attribute "data-header" of the 'th'.
    This function thus searches for the 'data-header' attribute instead of the outward-facing columns.

    Arguments
    ----------------
    url : CEBS dataset link
    
    Returns 
    ----------------
    columnList: list of column names to pass into the API
    '''
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        tables = soup.find_all("table")
        if tables:
            table = tables[0] 
            # headers = [th.text.strip() for th in table.find_all("th")]  # <-- This is the outward-facing column name
            columnList = [th.get('data-header') for th in table.find_all("th")]
            return columnList
        else:
            print("No table found on the page.")
    else:
        print(f"Failed to fetch the webpage. Status code: {response.status_code}")
    return



# Function with backoff to deal with mis-behaving slugs. Called during pagination.
@backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=10)
def fetch_api_data(api_url, payload, headers):
    """ Fetch a single batch from API. """
    response = requests.post(api_url, data=payload, headers=headers, timeout=10)
    
    if response.status_code == 200:
        return response
    
    print(f"Failed API call. Status code: {response.status_code}")
    return None


# ... Gets dataset given individual dataset's slug identifier
def get_html_table_api(
    api_url, 
    slug, 
    columnList=[],
    _token="",
):
    '''
    Arguments
    ----------------------
    api_url : CEB's api - https://cebs-ext.niehs.nih.gov/datasets/api/dataset/data/fetch
    slug : dataset identifier
    draw : used by API to identify which
    columnList : list of columns to request from API
    
    Returns
    ----------------------
    Pandas dataframe
    '''
    # Headers (adjust as needed based on the actual request)
    headers = {
        ""
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US, en;q=0.9",
        "Connection":"keep-alive",
        "Content-length":"3005",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Cookie": "ncbi_sid=90C64A6173B90DC1_0000SID; _ga=GA1.3.13396594.1731956957; _ga_CNVCG2ERJ0=GS1.1.1732028775.1.1.1732028818.17.0.0; _ga_7SNXGJ5KCC=GS1.2.1732045042.1.1.1732045128.0.0.0; _ga_1EBN0LJRZK=GS1.2.1732045042.1.1.1732045128.0.0.0; _ga_DP2X732JSX=GS1.1.1736367127.5.0.1736367127.0.0.0; _gid=GA1.2.2013118334.1736367218; _ga_Z8SS5QJCLH=GS1.1.1736370157.1.0.1736370157.0.0.0; XSRF-TOKEN=eyJpdiI6IjFkd0EvZkorei9oRlRnZHM2QVgyZWc9PSIsInZhbHVlIjoiYTdzSnhrcjg0KzE4aGJUNjZRZE9sRHp4WHJJUkhlQlQ3TGowRnZHY2pxTGtUZE5ncXJCMHNKTFFCMXhtK2xHd1ZIck9odjJETnlVMjkxRUF6N1ZPakVIZTMzaTBmMDV4eElnUGg2dHdQUDlpT0RxRkxoM2ZidUhGRkxweExZN2ciLCJtYWMiOiI2NWU2Y2EwNDUwZWRhNWJmODdhOTNkOWI0MTBlMjljMjEwMjE3YTY1MWEwYjVkN2NmYjIyMjVkNDRjMTQ5MGNiIiwidGFnIjoiIn0%3D; dtt_data_collections_guided_search_session=eyJpdiI6Inc5d0FaT3RmeDNmQ1ZzS3JBYlBNUVE9PSIsInZhbHVlIjoiRGdRcmNDUktxRFAwMnFMRlg5R1kwZ1ZGMlR6UU50SzI2UWEwakYrWWxzT29ZZEx6ZXUreEhhTkJ2dERraGlCVmdUVndIMUF3d1pHTDJDV1hDYlZTMTlFYzFzb3packZTb0dtWlRjbWtkWUpwd1lTeHVCdEtCRGNJNFdDZ0NIZm0iLCJtYWMiOiIxNTc0ZDllYWM0MGY1ZTg0NDQwODgwNzhlMmY0Y2Q0ZWY0ZmMzY2JlZjI1ZWRmMGQyNDliM2E3M2Y0OTExYTBlIiwidGFnIjoiIn0%3D; _ga=GA1.2.13396594.1731956957; _ga_38SLQ135G0=GS1.2.1736440261.10.1.1736442742.0.0.0; _ga_CSLL4ZEK4L=GS1.1.1736440261.15.1.1736442748.0.0.0",
        "Host": "cebs-ext.niehs.nih.gov",
        "Origin": "https://cebs-ext.niehs.nih.gov",
        "Referer": "https://cebs-ext.niehs.nih.gov/datasets/search/tgx-ddi-tox21-pos",
        "Sec-CH-UA": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": '"macOS"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }
    # ******************************** Initialize parameters *************************** 
    draw = 1
    all_data = []

    start = 0
    batch_size = 1_000

    # ************************ First get N_iterations to loop through ******************* 
    payload = {
            "draw": draw,                        # Increment with each request if needed
            "start": start,                      # Starting index (pagination)
            "length": batch_size,                # Number of rows to fetch (adjust as needed)
            "search[value]": "",                 # Global search query (empty for no search)
            "search[regex]": "false",            # Regex for global search (false)
            "slug": slug,                        # Dataset identifier
            "_token": _token,                    # CSRF token
            "searchFilters": f"_token={_token}",
            "columnList[]": columnList,          # Columns to fetch
            "previewMode": "false",              # Preview mode
        }

    response = requests.post(api_url, data=payload, headers=headers)
    if response.status_code == 200:
        data = response.json()
        records_total = data.get("recordsTotal", 0)

        N_iterations = int(records_total / batch_size) + 1
        print('# records total: ', records_total)
    else: 
        print("Failed to fetch data. Status code: {response.status_code}")
        # breakpoint()  # This will invoke `ipdb`

    # ****************************** Now start iterating ********************************
    # for i in tqdm(range(N_iterations), 
    #     ascii=True, dynamic_ncols=True, file=sys.stdout, disable=None, ncols=80
    # ): 
    for i in range(N_iterations):

        # Payload for the POST request (this should match the request payload from the network tab)
        payload = {
            "draw": draw,                        # Increment with each request if needed
            "start": start,                      # Starting index (pagination)
            "length": batch_size,                # Number of rows to fetch (adjust as needed)
            "search[value]": "",                 # Global search query (empty for no search)
            "search[regex]": "false",            # Regex for global search (false)
            "slug": slug,                        # Dataset identifier
            "_token": _token,                    # CSRF token
            "searchFilters": f"_token={_token}",
            "columnList[]": columnList,          # Columns to fetch
            "previewMode": "false",              # Preview mode
        }
        
        response = fetch_api_data(api_url, payload, headers)
        
        if response.status_code == 200:
            data = response.json()
            batch_data = data.get("data", [])
            all_data.extend(batch_data)
            start += batch_size
            
        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")
            # breakpoint()  # This will invoke `ipdb`
            break
    
    # ****************************** Return dataframe ********************************
    df = pd.DataFrame(all_data)
    N_fetched = len(df)
    print('# records fetched: ', N_fetched)

    status = 'Failed'
    if len(df) < records_total:
        status = f'Incomplete ({N_fetched}/{records_total})'
    if len(df) == records_total: 
        status = 'Complete'
        
    return df, status




if __name__ == '__main__':
 
    # *********************************************************************
    # Fetch initial table on site (indexes all datasets and their links)
    # *********************************************************************
    url = "https://cebs-ext.niehs.nih.gov/datasets/"
    df = get_html_table(url, get_name_links=True)

    # ... Slug is the identifier of each dataset
    df['slug'] = df['Link'].apply(get_slug)

    # ... Saving initial table.

    output_dir = Path("brick/") # Create directory
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f'cebs_DDT_datasets.parquet'
    
    # df.to_csv(output_path, index=False)
    df.to_parquet(output_path)
    print(f"Initial table scraped and saved to '{output_path}'", '\n')
    print(f"There are {len(df)} datasets to retrieve.", '\n')



    # *********************************************************************
    # Fetch each dataset in the initial table.
    # *********************************************************************
    
    api_url = "https://cebs-ext.niehs.nih.gov/datasets/api/dataset/data/fetch"
    _token = "Yk9ru1CcmxkmgOhbtYh2yXxQ3ZA0W8b67Gsv2wRT"  # Payload parameters - CSRF token (cross-site request forgery)

    # Create output directory
    output_dir = Path("brick/datasets/")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Create file to track status of API requests 
    file_status_queries = 'status_queries.txt'
    open(file_status_queries, "w").close()


    slugs_requested = [
        'clin-chem-iad-2024',
        'hematol-iad-2024',
        'organ-weight-iad-2024'
    ]

    for index, row in df.iterrows():
        try: 
            # Get parameters to pass into API
            name = row['Name']
            group = row['Group']
            desc = row['Description']
            link = row['Link']
            slug = row['slug']  # Dataset identifier for API
            
            columnList = get_columnList(link)
            
            print(f'Index: {index}.')
            print('Slug: ', slug)
            print('Columns: ', columnList)

            # Fetching dataset
            df_data, status = get_html_table_api(
                api_url, 
                slug, 
                columnList=columnList,
                _token=_token
            )

            # Save data
            output_path = output_dir / f'{slug}.parquet'
            df_data.to_parquet(output_path)
            print(f'Dataframe saved to {output_path}' , '\n')

            #  Write slug and status to file
            with open(file_status_queries, 'a') as f:
                f.write(f"{status} - {slug} \n")

        except: 
            status = 'Failed to retrieve.'
            slug = row['slug']
            print(f'Dataset with identifier {slug} was not successfully retrieved.\n\n')
            print("********************************************************************")

            # Write to failed queries file
            with open(file_status_queries, 'a') as f:
                f.write(f"{status} - {slug}\n")





    
    
# write problematic slugs to a separate file