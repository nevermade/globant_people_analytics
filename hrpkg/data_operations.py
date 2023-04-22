import pandas as pd
from unicodedata import name
from unittest import result
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import logging
import os
import datetime
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
# from fastavro import writer, reader, parse_schema, schemaless_writer

logging.basicConfig(format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S', level=os.environ.get("LOGLEVEL", "INFO"))


class SnowflakeOperations:

    def __init__(self, credentials):
        self.__CHUNK_SIZE = 1000
        self.__conn = snowflake.connector.connect(
            user=credentials["user"],
            password=credentials["password"],
            account=credentials["account"]
        )

    def load_csv_to_snowflake_table(self, filename,  destination, datatypes):
        cs = self.__conn.cursor()

        try:
            cs.execute(f"USE DATABASE GLOBANT_PEOPLE_ANALYTICS")
            cs.execute(f"USE SCHEMA PUBLIC")
            df_invalid_rows = pd.DataFrame(columns=datatypes.keys())
            for chunk in pd.read_csv(
                filename,
                names=datatypes.keys(),
                chunksize=self.__CHUNK_SIZE
            ):
                df_valid_rows = pd.DataFrame(columns=datatypes.keys())
                for idx, row in chunk.iterrows():
                    if self.__is_valid_row(idx, row, datatypes):
                        df_valid_rows = df_valid_rows.append(row)
                    else:
                        df_invalid_rows = df_invalid_rows.append(row)
                df_valid_rows.to_csv('valid_rows.csv')
                write_pandas(self.__conn, df_valid_rows, destination, auto_create_table  = True)
            logging.info(f"File {filename} was uploaded")

            return df_invalid_rows
        except Exception as e:
            logging.error(f"File wasn't uploaded: {e}")
            return df_invalid_rows
        finally:
            cs.close()

    def __is_valid_row(self, idx, row, datatypes):
        if row.isnull().any():
            row["message"] = f"Row #{idx}: Row has empty values"
            return False
        for column, value in row.iteritems():
            if datatypes[column] == "int":
                if not self.__is_integer(value):
                    logging.info(value)
                    row["message"] = f"Row #{idx}: Column {column} has a non-integer value -{value}-"
                    return False
            elif datatypes[column] == "str":
                if not isinstance(value, str):
                    row["message"] = f"Row #{idx}: Column {column} has a non-str value -{value}-"
                    return False
            elif datatypes[column] == "date":
                if not self.__is_iso_date(value):
                    row["message"] = f"Row #{idx}: Column {column} has a non-str value -{value}-"
                    return False
        return True

    def __is_iso_date(self, date_string):
        try:
            datetime.datetime.fromisoformat(date_string[:-1]+'+00:00')
            return True
        except ValueError:
            return False

    def __is_integer(self, int_string):
        try:
            int(int_string)
            return True
        except ValueError:
            return False
