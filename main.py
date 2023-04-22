import logging
import os
from hrpkg import data_operations
import config
import constants


logging.basicConfig(format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S', level=os.environ.get("LOGLEVEL", "INFO"))

if __name__ == "__main__":
    
    snow_op = data_operations.SnowflakeOperations(config.credentials)
    snow_op.load_csv_to_snowflake_table(
        'uploads/hired_employees.csv', 'hired_employees', constants.HIRED_EMPLOYEES_DT)
    

