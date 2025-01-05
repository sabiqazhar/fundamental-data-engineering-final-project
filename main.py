import pandas as pd
import luigi

from datetime import  datetime
from helper.connection import Connection
from helper.extract import Extract
from helper.validate import Validate

"""
for testing 1 class
luigi.build([ExtractHotelData()], local_scheduler = True)

run this first after u run run_etl.sh
luigid --port 8082 > /dev/null 2> /dev/null &
"""

today_date = datetime.now().strftime("%Y_%m_%d")

class ExtractSalesData(luigi.Task):
    def requires(self):
        pass

    def run(self):
        engine = Connection.postgres_engine("extract")

        # note: using limit offset and batch mechanism if had a lot data
        get_query = "SELECT * FROM amazon_sales_data"
        sales_data = pd.read_sql(get_query, engine)

        sales_data.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget(
            f"data/extract/extract_sales_data_{today_date}.csv"
        )


class ExtractMarketingData(luigi.Task):
    def requires(self):
        pass

    def run(self):
        marketing_data = pd.read_csv(
            "raw_data/ElectronicsProductsPricingData.csv"
        )

        marketing_data.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget(
            f"data/extract/extract_marketing_data_{today_date}.csv"
        )


class ExtractCryptoData(luigi.Task):
    def requires(self):
        pass

    def run(self):
        url = "https://coinmarketcap.com/trending-cryptocurrencies/"

        soup = Extract.scrapper_engine(url=url)

        data_scrapper = Extract.take_scrapper_rows(soup=soup, section="table")

        data = pd.DataFrame(data_scrapper)
        data.to_csv(self.output().path, index=False)

        print("Data extract complete!!!")

    def output(self):
        return luigi.LocalTarget(
            f"data/extract/extract_scrapping_data_{today_date}.csv"
        )

class ValidationData(luigi.Task):
    def requires(self):
        return [ExtractSalesData(),ExtractMarketingData(),ExtractCryptoData()]

    def run(self):
        validate_sales_data = pd.read_csv(self.input()[0].path)
        validate_marketing_data = pd.read_csv(self.input()[1].path)
        validate_crypto_data = pd.read_csv(self.input()[2].path)

        Validate.validatation_process(validate_sales_data, "Sales")
        Validate.validatation_process(validate_marketing_data, "Marketing")
        Validate.validatation_process(validate_crypto_data, "Crypto")

    def output(self):
        pass

# class TransformSalesData(luigi.Task):
#     def requires(self):
#         pass
#
#     def run(self):
#         pass
#
#     def output(self):
#         pass


class TransformMarketingData(luigi.Task):
    def requires(self):
        return ExtractMarketingData()

    def run(self):
        marketing_data = pd.read_csv(self.input().path)

        # deleted unused column
        unused_cols = ["ean", "Unnamed: 26", "Unnamed: 27", "Unnamed: 28", "Unnamed: 29", "Unnamed: 30"]
        marketing_data = marketing_data.drop(unused_cols, axis=1)

        rename_cols = {
            "prices.amountMax": "amount_max",
            "prices.amountMin": "amount_min",
            "prices.availability": "availability",
            "prices.condition": "condition",
            "prices.currency": "currency",
            "prices.dateSeen": "date_seen",
            "prices.isSale": "sale",
            "prices.merchant": "merchant",
            "prices.shipping": "shipping",
            "imageURLs": "image_urls",
            "manufacturerNumber": "manufacturer_number",
            "primaryCategories": "primary_categories",
            "sourceURLs": "source_urls",
            "dateAdded": "date_added",
            "dateUpdated": "date_updated"
        }

        # rename coloumn
        marketing_data = marketing_data.rename(columns=rename_cols)

        # transform data
        marketing_data['shipping'] = marketing_data['shipping'].fillna(marketing_data['shipping'].mode()[0])
        marketing_data['manufacturer'] = marketing_data['manufacturer'].fillna(marketing_data['manufacturer'].mode()[0])
        marketing_data["date_seen"] = pd.to_datetime(marketing_data["date_seen"].apply(lambda x: x.split(",")[0]))


    def output(self):
        return luigi.LocalTarget(
            f"data/load/transform_marketing_data_{today_date}.csv"
        )


# class TransformCryptoData(luigi.Task):
#     def requires(self):
#         pass
#
#     def run(self):
#         pass
#
#     def output(self):
#         pass

# class LoadData(luigi.Task):
#     def requires(self):
#         pass
#
#     def run(self):
#         pass
#
#     def output(self):
#         pass

if __name__ == '__main__':
    luigi.build([ExtractSalesData(),
                 ExtractMarketingData(),
                 ExtractCryptoData(),
                 ValidationData()])
