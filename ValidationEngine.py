################################################################
# Purpose:  Data Validation library. Currently supports CSV File and SQL Server
# Author: Deepak Gupta
# Created: Sep 2, 2024
################################################################

import duckdb
import pandas as pd
import pyodbc
import logging
import os
import pyodbc
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import sqlalchemy as sa
import time

# variables
test_start_time = time.time()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log") #,
        #logging.StreamHandler()
    ]
)

# Validate the count of records in the CSV file
def validate_csv_record_count(csv_file_path, comparison):
    if not os.path.exists(csv_file_path):
        logging.error(f"CSV file not found: {csv_file_path}")
        return False
    
    try:
        df = pd.read_csv(csv_file_path)
        record_count = len(df)
        logging.info(f"CSV file {csv_file_path} has {record_count} records.")
 
        result = eval(str(record_count) + " " + comparison)
 
        return result
    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
        return None

# Validate the count of records in the CSV file
def get_records_csv(csv_file_path, tableName, strSQL):
    if not os.path.exists(csv_file_path):
        logging.error(f"CSV file not found: {csv_file_path}")
        return False
    
    try:
        df = pd.read_csv(csv_file_path)
        record_count = len(df)
        logging.info(f"CSV file {csv_file_path} has {record_count} records.")
 
        duckdb.sql("CREATE TABLE " + tableName + " AS SELECT * FROM df")
        df = duckdb.sql(strSQL).df()
        record_count = len(df)
        duckdb.sql("DROP TABLE " + tableName)

        logging.info(f"CVS SQL Query returned {record_count} records.")

        return df
    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
        return None

def get_records_sql_server(server, database, strSQL):

    try:
        conn_str = (
            f"DRIVER={{SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
        )
        
        # create connection
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": conn_str})
        engine = create_engine(connection_url)

        with engine.begin() as conn:
            df = pd.read_sql_query(sa.text(strSQL), conn)
            #duckdb.sql("CREATE TABLE mySQLTable AS SELECT * FROM df")


        logging.info("Successfully connected to SQL Server.")
        logging.info(f"Fetched {len(df)} records from SQL Server.")

    except Exception as e:
        logging.error(f"Failed to connect to SQL Server: {e}")
        return None

    return df

def compare_csv_with_db(csv_df, db_df, key_columns):
    mismatches = pd.merge(csv_df, db_df, on=key_columns, how='outer', indicator=True)
    only_in_csv = mismatches[mismatches['_merge'] == 'left_only']
    only_in_db = mismatches[mismatches['_merge'] == 'right_only']

    if len(only_in_csv) == 0 and len(only_in_db) == 0:
        logging.info(f"Records in CSV match DB")
        logging.info(f"Records in CSV but not in DB: 0")
        logging.info(f"Records in DB but not in CSV: 0")
        return True
    else:
        if len(only_in_csv) > 0:
            logging.info(f"Records in CSV but not in DB: {only_in_csv}")
        if len(only_in_db) > 0:
            logging.info(f"Records in DB but not in CSV: {only_in_db}")

        return False

def start_test_case(tcName):
    logging.info (f"Test Case:" + tcName)
    test_start_time = time.time()

def end_test_case(tcName, result, message):

    test_end_time = time.time()
    time_taken = str(round(test_end_time - test_start_time, 1)) + "s"
    time_taken = time_taken.ljust(10, " ")

    if result is None:
        logging.error("Result: Error")
    else:
        PassFail = ("Pass" if result == True else "Fail")
        logging.info("Result: " + PassFail)
        print (tcName.ljust(10, " ") + " | " +  PassFail.ljust(6, " ") + " | " + time_taken + " | ")
    #logging.info (f"Test Case:" + tcName)

def convert_df_string(df):

    logging.debug ("Started Function: convert_df_string")

    logging.debug ("Dataframe length: " + str(len(df)))


    # conver to array
    myArray = df.to_numpy()

    # create a list
    return_value = ""

    # join array value to a string
    for value in myArray:
        if len(return_value) == 0:
            return_value = value
        else:
            return_value = return_value + ", " + value

    return return_value