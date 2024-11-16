import pandas as pd
import luigi
import requests
import time

from helper.connection import Connection
from helper.extract import Extract

"""
link csv: https://drive.google.com/file/d/1J0Mv0TVPWv2L-So0g59GUiQJBhExPYl6/view
link scrapping crypto: https://coinmarketcap.com/trending-cryptocurrencies/
"""


class ExtractSalesData(luigi.Task):
    def requires(self):
        pass

    def run(self):
        engine = Connection.postgres_engine("extract")
        get_query = "SELECT * FROM "

        sales_data = pd.read_sql(get_query, engine)

        sales_data.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget(
            f"data/extract/extract_sales_data_{time.time_ns().csv}"
        )


class ExtractMarketingData(luigi.Task):
    def requires(self):
        pass

    def run(self):
        marketing_data = pd.read_csv("https://drive.google.com/file/d/1J0Mv0TVPWv2L-So0g59GUiQJBhExPYl6/view?usp=sharing")
        marketing_data.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget(
            f"data/extract/extract_marketing_data_{time.time_ns().csv}"
        )


class ExtractCryptoData(luigi.Task):
    def requires(self):
        pass

    def run(self):
        url = "https://coinmarketcap.com/trending-cryptocurrencies/"

        soup = Extract.scrapper_engine(url=url)

        data_scrapper = Extract.take_scrapper_rows(soup=soup, section="table")

        data_scrapper.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget(
            f"data/extract/extract_scrapping_data_{time.time_ns()}.csv"
        )


class TransformSalesData(luigi.Task):
    def requires(self):
        pass

    def run(self):
        pass

    def output(self):
        pass


class TransformMarketingData(luigi.Task):
    def requires(self):
        pass

    def run(self):
        pass

    def output(self):
        pass


class TransformCryptoData(luigi.Task):
    def requires(self):
        pass

    def run(self):
        pass

    def output(self):
        pass


class ValidationData(luigi.Task):
    def requires(self):
        return [ExtractSalesData(),
                ExtractMarketingData(),
                ExtractCryptoData()]

    def run(self):
        len_requires = len(self.requires())
        for idx in range(0, len_requires-1):
            data = pd.read_csv(self.input()[idx].path)

            # start data quality pipeline
            print("===== Data Quality Pipeline Start =====")
            print("")

            # check data shape
            print("===== Check Data Shape =====")
            print("")
            print(f"Data Shape for this Data {data.shape}")

            # check data type
            get_cols = data.columns

            print("")
            print("===== Check Data Types =====")
            print("")

            # iterate to each column
            for col in get_cols:
                print(f"Column {col} has data type {data[col].dtypes}")

            # check missing values
            print("")
            print("===== Check Missing Values =====")
            print("")

            # iterate to each column
            for col in get_cols:
                # calculate missing values in percentage
                get_missing_values = (data[col].isnull().sum() * 100) / len(data)
                print(
                    f"Columns {col} has percentages missing values {get_missing_values} %"
                )

            print("===== Data Quality Pipeline End =====")
            print("")

            data.to_csv(self.output()[idx].path, index=False)

    def output(self):
        return [
            luigi.LocalTarget("data/validate/validate_sales.csv"),
            luigi.LocalTarget("data/validate/validate_marketing_data.csv"),
            luigi.LocalTarget("data/validate/validate_crypto_data.csv"),
        ]


class LoadData(luigi.Task):
    def requires(self):
        pass

    def run(self):
        pass

    def output(self):
        pass
