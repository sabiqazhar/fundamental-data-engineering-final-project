import pandas as pd

class Validate():
    @staticmethod
    def validatation_process(data: pd.DataFrame, data_name: str):
        try:
            print("========== Start Pipeline Validation ==========")
            print("")

            # check data shape
            n_rows = data.shape[0]
            n_cols = data.shape[1]

            print(f"In the data {data_name} there are {n_rows} rows and {n_cols} columns")
            print("")

            GET_COLS = data.columns

            # check data types for each column
            for col in GET_COLS:
                print(f"Column {col} has data type {data[col].dtypes}")

            print("")

            # check missing values for each column
            for col in GET_COLS:
                # calculate missing values in percentage
                get_missing_values = (data[col].isnull().sum() * 100) / len(data)
                print(f"Columns {col} has percentages missing values {get_missing_values} %")

                print("")
            print("========== End Pipeline Validation ==========")
        except Exception as e:
            print(f"Error processing data {data_name}: {e}")
