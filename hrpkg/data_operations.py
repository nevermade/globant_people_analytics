from fastavro import writer, reader, parse_schema, schemaless_writer
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
                        df_valid_rows = pd.concat(
                            [df_valid_rows, row.to_frame().T], ignore_index=True)
                    else:
                        df_invalid_rows = pd.concat(
                            [df_invalid_rows, row.to_frame().T], ignore_index=True)                
                write_pandas(self.__conn, df_valid_rows,
                             destination, auto_create_table=True)
            logging.info(f"File {filename} was uploaded")

            return df_invalid_rows
        except Exception as e:
            logging.error(f"File wasn't uploaded: {e}")
            return df_invalid_rows
        finally:
            cs.close()

    def __is_valid_row(self, idx, row, datatypes):
        if row.isnull().any():
            row["message"] = f"Row #{idx+1}: Row has empty values"
            return False
        for column, value in row.items():
            if datatypes[column] == "int":
                if not self.__is_integer(value):
                    logging.info(value)
                    row["message"] = f"Row #{idx+1}: Column {column} has a non-integer value -{value}-"
                    return False
            elif datatypes[column] == "str":
                if not isinstance(value, str):
                    row["message"] = f"Row #{idx+1}: Column {column} has a non-str value -{value}-"
                    return False
            elif datatypes[column] == "date":
                if not self.__is_iso_date(value):
                    row["message"] = f"Row #{idx+1}: Column {column} has a non-str value -{value}-"
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

    def export_to_avro(self, tablename, schema):
        try:
            cs = self.__conn.cursor()
            cs.execute(f"USE DATABASE GLOBANT_PEOPLE_ANALYTICS")
            cs.execute(f"USE SCHEMA PUBLIC")
            query = f'SELECT * FROM "{tablename}"'
            cs.execute(query)
            results = cs.fetchall()
            rows = []

            # Parse to dict
            
            for row in results:
                new_row = {}
                for i in range(len(schema["fields"])):
                    new_row[schema["fields"][i]["name"]] = row[i]
                rows.append(new_row)
            parsed_schema = parse_schema(schema)            
            with open(f"output_files/{tablename}.avro", "wb") as out:
                writer(out, parsed_schema, rows)
            return 1
        except Exception as e:
            logging.error(e)
            return 0
        finally:
            cs.close()

    def restore_table_from_avro(self, tablename):
        try:
            cs = self.__conn.cursor()
            cs.execute(f"USE DATABASE GLOBANT_PEOPLE_ANALYTICS")
            cs.execute(f"USE SCHEMA PUBLIC")
            cs.execute(f'DROP TABLE IF EXISTS "{tablename}"')
            with open(f"uploads/{tablename}.avro", "rb") as fo:
                avro_reader = reader(fo)
                list = []
                for record in avro_reader:
                    list.append(record)
            df = pd.DataFrame(list)
            write_pandas(self.__conn, df, tablename, auto_create_table=True)

            return 1
        except Exception as e:
            logging.error(e)
            return 0
        finally:
            cs.close()
