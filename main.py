import pandas as pd
import luigi
import math

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

class TransformSalesData(luigi.Task):
    def requires(self):
        return ExtractSalesData()

    def run(self):
        sales_data = pd.read_csv(self.input().path)

        # deleted unused column
        unused_cols = "Unnamed: 0"
        sales_data = sales_data.drop(unused_cols, axis=1)

        # transform data
        sales_data['ratings'] = pd.to_numeric(sales_data['ratings'], errors='coerce')
        sales_data["ratings"] = sales_data["ratings"].fillna(sales_data["ratings"].mean())
        sales_data['ratings'] = sales_data['ratings'].apply(lambda x: math.ceil(x) if pd.notnull(x) else x)

        sales_data['no_of_ratings'] = pd.to_numeric(sales_data['no_of_ratings'], errors='coerce')
        sales_data["no_of_ratings"] = sales_data["no_of_ratings"].fillna(sales_data["no_of_ratings"].mean())
        sales_data['no_of_ratings'] = sales_data['no_of_ratings'].apply(lambda x: math.ceil(x) if pd.notnull(x) else x)

        sales_data['discount_price'] = sales_data['discount_price'].str.replace('₹', '').str.strip()
        sales_data['discount_price'] = pd.to_numeric(sales_data['discount_price'], errors='coerce')
        sales_data["discount_price"] = sales_data["discount_price"].fillna(sales_data["discount_price"].mean())
        sales_data['discount_price'] = sales_data['discount_price'].apply(lambda x: math.ceil(x) if pd.notnull(x) else x)

        sales_data['actual_price'] = sales_data['actual_price'].str.replace('₹', '').str.replace(',', '').str.strip()
        sales_data['actual_price'] = pd.to_numeric(sales_data['actual_price'], errors='coerce')
        sales_data["actual_price"] = sales_data["actual_price"].fillna(sales_data["actual_price"].median())
        sales_data['actual_price'] = sales_data['actual_price'].apply(lambda x: math.ceil(x) if pd.notnull(x) else x)


    def output(self):
        return luigi.LocalTarget(
            f"data/transform/transform_sales_data_{today_date}.csv"
        )


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
            f"data/transform/transform_marketing_data_{today_date}.csv"
        )


class TransformCryptoData(luigi.Task):
    def requires(self):
        return ExtractCryptoData()

    def run(self):
        pass

    def output(self):
        return luigi.LocalTarget(
            f"data/transform/transform_crypto_data_{today_date}.csv"
        )

class LoadData(luigi.Task):
    def requires(self):
        return [TransformSalesData(),
                TransformMarketingData(),
                TransformCryptoData()]

    def run(self):
        engine = Connection.postgres_engine()

        load_sales_data = pd.read_csv(self.input()[0].path)
        load_marketing_data = pd.read_csv(self.input()[1].path)
        load_crypto_data = pd.read_csv(self.input()[2].path)

        sales_data_name = "sales_data"
        marketing_data_name = "marketing_data"
        crypto_data_name = "crypto_data"

        load_sales_data.to_sql(name=sales_data_name, con=engine, if_exists="append", index=False)
        load_marketing_data.to_sql(name=marketing_data_name, con=engine, if_exists="append", index=False)
        load_crypto_data.to_sql(name=crypto_data_name, con=engine, if_exists="append", index=False)

        load_sales_data.to_csv(self.output()[0].path, index=False)
        load_marketing_data.to_csv(self.output()[1].path, index=False)
        load_crypto_data.to_csv(self.output()[2].path, index=False)


    def output(self):
        return [luigi.LocalTarget(
            f"data/transform/transform_sales_data_{today_date}.csv"
        ), luigi.LocalTarget(
            f"data/transform/transform_marketing_data_{today_date}.csv"
        ), luigi.LocalTarget(
            f"data/transform/transform_crypto_data_{today_date}.csv"
        )]

if __name__ == '__main__':
    luigi.build([ExtractSalesData(),
                 ExtractMarketingData(),
                 ExtractCryptoData(),
                 ValidationData(),
                 TransformSalesData(),
                 TransformMarketingData(),
                 TransformCryptoData()])
