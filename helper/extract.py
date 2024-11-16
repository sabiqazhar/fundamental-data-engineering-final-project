import pandas as pd
import requests
import time

from bs4 import BeautifulSoup
# from tqdm import tqdm


class Extract:
    @staticmethod
    def scrapper_engine(url: str):
        resp = requests.get(url)

        soup = BeautifulSoup(resp.text, "html.parser")

        return soup

    @staticmethod
    def take_scrapper_rows(soup, section):
        data = []
        if section == "table":
            section_data = soup.find("table")
            rows = section_data.find_all("tr")[1:]

            for row_data in rows:
                # get data for cryptodata
                columns = row_data.find_all("td")
                number = columns[1].text.strip()
                name = columns[2].text.strip().split(number)[0]
                price = columns[3].text.strip()
                last_price = columns[4].text.strip()
                mcap = columns[7].text.strip()
                data.append({
                    "number": number,
                    "name": name,
                    "price": price,
                    "market_cap": mcap,
                    "percentage_change_24h": last_price
                })

        return data

    @staticmethod
    def fetch_engine(url):
        pass
