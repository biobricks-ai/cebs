'''
# This is fetching using the `datatables.js` server-side at 
# ........ https://datatables.net/
# ........ https://datatables.net/manual/server-side

# This script scrapes the table of datasets
# ........ at the following site: https://cebs-ext.niehs.nih.gov/datasets/
# ........ As of 01-08-2025, this contains 47 datasets. 
# The outer dataframe will contain the name, group, description, and link, of each of # these 47 datasets.

# This outer dataframe is then looped through to retrieve each individual dataset, 
# which are all stored in ____
'''


import numpy as np
import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path

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

# ... Gets dataset given individual dataset's slug identifier
def get_html_table_api(
    api_url, 
    slug, 
    draw=1,
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
    
    # Payload for the POST request (this should match the request payload from the network tab)
    payload = {
        "draw": draw,                        # Increment with each request if needed
        "start": 0,                          # Starting index (pagination)
        "length": 1000,                      # Number of rows to fetch (adjust as needed)
        "search[value]": "",                 # Global search query (empty for no search)
        "search[regex]": "false",            # Regex for global search (false)
        "slug": slug,                        # Dataset identifier
        "_token": _token,                    # CSRF token
        "searchFilters": f"_token={_token}",
        "columnList[]": columnList,          # Columns to fetch
        "previewMode": "false",              # Preview mode
    }
    
    # Send the POST request
    response = requests.post(api_url, data=payload, headers=headers)
    
    # Check the response
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
    
        keys = list(data.keys())
        print('Data keys: ', keys)
        
        # Extract the table data (adjust keys based on the JSON structure)
        d_data = data.get("data", [])
        
        N_total = data.get("recordsTotal", [])
        N_filtered = data.get("recordsFiltered", [])
        print('recordsTotal / recordsFiltered: ', N_total, ' / ', N_filtered)
        
        # Create a pandas DataFrame
        df = pd.DataFrame(d_data)
        
        return df
    
    else:
        print(f"Failed to fetch data. Status code: {response.status_code}")

    return


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
    print(f"Initial table scraped and saved to '{output_path}'")


    # *********************************************************************
    # Fetch each dataset in the initial table.
    # *********************************************************************
    
    api_url = "https://cebs-ext.niehs.nih.gov/datasets/api/dataset/data/fetch"
    _token = "Yk9ru1CcmxkmgOhbtYh2yXxQ3ZA0W8b67Gsv2wRT"  # Payload parameters - CSRF token (cross-site request forgery)

    
    # Saving dataset to ouptut
    output_dir = Path("brick/datasets/") # Create directory
    output_dir.mkdir(parents=True, exist_ok=True)

    d_cebs_data = {}
    for index, row in df.iterrows():
        # Get parameters to pass into API
        name = row['Name']
        group = row['Group']
        desc = row['Description']
        link = row['Link']
        slug = row['slug']  # Dataset identifier for API
        
        columnList = get_columnList(link)
        
        print('Slug: ', slug)
        print('Column list: ', columnList)

        # Fetching dataset
        df_data = get_html_table_api(
            api_url, 
            slug, 
            columnList=columnList,
            _token=_token,
            draw=1
        )
        
        print(df_data.head(), df_data.columns)
        
        d_cebs_data[slug] = df_data

        output_path = output_dir / f'{slug}.parquet'
        df_data.to_parquet(output_path)
        print(f'Dataframe saved to {output_path}' , '\n')
    
    
    
