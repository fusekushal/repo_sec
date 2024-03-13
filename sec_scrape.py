import os
import yaml
import re
import time
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from scrapingbee import ScrapingBeeClient
from concurrent.futures import ThreadPoolExecutor
from requests.exceptions import (
    HTTPError,
    ConnectionError,
    RequestException,
    Timeout
)

import pandas as pd
import pyarrow
from pandas import DataFrame
from logger_file import logger

import pyspark
from pyspark.sql import SparkSession, DataFrame, functions as F, types as T, Row
from pyspark.sql.window import Window
from pyspark import SparkContext

logger.info(f"Logger initialized successfully in file")


def save_dataframepqt_pd(df: DataFrame, path: str):
    file_exists = os.path.exists(f"{path}")
    df.to_parquet(f"{path}.parquet", index = False)

def scrape_document(cik_name: str, date: str, cik_num: str, accsNum: str, document: str, client, max_retries=3):
    retries = 0
    page_content = []
    default_soup = 'No Soup! Got Value other than 200'
    while retries < max_retries:
        try:
            url = f"https://www.sec.gov/Archives/edgar/data/{cik_num}/{accsNum}/{document}"
            logger.info(f"Currently beginning scraping for cik name {cik_name} and document {document}")
            print(url)
            res = client.get(url, params = {'render_js': 'False',})
            if res.status_code == 200:
                data = res.content
                soup = BeautifulSoup(data, "html.parser")
                soupstr = str(soup.body)
                logger.info(f"Successfully extracted the soup from {url}")
                page_content = {
                            "cik_name": cik_name,
                            "reporting_date": date,
                            "url":url,
                            "contents": soupstr
                        }
                return page_content
            else:
                page_content = {
                            "cik_name": cik_name,
                            "reporting_date": date,
                            "url":url,
                            "contents": default_soup
                        }
                logger.info(f"Couldnt extract soup, so sending default data")
                return page_content
        except requests.exceptions.HTTPError as errh:
            logger.info("Http Error:", str(errh))
            time.sleep(2)
        except requests.exceptions.ChunkedEncodingError as errh:
            logger.info("Chunked Encoding Error:", str(errh))
            time.sleep(2)
        except requests.exceptions.ConnectionError as errc:
            logger.info("Error Connecting:", str(errc))
            time.sleep(2)
        except requests.exceptions.Timeout as errt:
            logger.info("Timeout Error:", str(errt))
            time.sleep(2)
        except requests.exceptions.RequestException as err:
            logger.info("OOps: Something Else", str(err))
        retries += 1
        time.sleep(5)
    page_content = {
                "cik_name": cik_name,
                "reporting_date": date,
                "url":url,
                "contents": default_soup
            }
    return page_content

def scrap_table(dict_records: dict):
    cik_num = dict_records["cik_number"]
    date = dict_records["reportDate"]
    accsNum = dict_records["accessionNumber"]
    document = dict_records["primaryDocument"]
    cik_name = dict_records["cik_name"]
    tables = scrape_document(cik_name, date, cik_num, accsNum, document, client)
    logger.info(f"scrapped the document from {document} inside the CIK name {cik_name}!!")
    return tables

def process_chunked_df(processed_rows, output_path):
    out = f"data/output/table_contents/{output_path}"
    if len(processed_rows) > 0:
        logger.info(len(processed_rows))
        table_df = pd.DataFrame(processed_rows, columns=["cik_name", "reporting_date", "url", "contents"])
        print(table_df.head())
        table_df = table_df[~table_df['contents'].isnull()]
        
        save_dataframepqt_pd(table_df, out)
        logger.info(
            f"Saved the document to {out}"
        )


if __name__ == "__main__":
    api_key = ''
    global client
    client = ScrapingBeeClient(api_key=api_key)
    logger.info(f"Scraping Table content Started")
    companies_df = pd.read_parquet("data/input/companies_detail1.parquet")
    year = 2022
    companies_df = companies_df[companies_df['year'] == year]
    companies_df['row_id'] = companies_df.sort_values(by='cik_number') \
                                      .reset_index(drop=True) \
                                      .index + 1
    total_items = companies_df.shape[0]
    batch_size = 55
    num_batches = (total_items + batch_size - 1) // batch_size
    logger.info(f"Total Number of Batches: {num_batches}")
    for i in range(0,num_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, total_items)
        logger.info(f"Batche of index: {start_idx}-{end_idx} and {i}/{num_batches} and started.")
        chunked_df = companies_df[(companies_df['row_id'] >= start_idx) & (companies_df['row_id'] < end_idx)]

        with ThreadPoolExecutor(max_workers=11) as executor:
            chunked_df = chunked_df.drop("row_id", axis=1)
            processed_rows = list(executor.map(scrap_table, chunked_df.to_dict(orient="records"))
            )
        logger.info(len(processed_rows))
        process_chunked_df(processed_rows, output_path = f"{year}/Batch_{i}")
    logger.info(f"Scraping Table content Completed")