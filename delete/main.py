import duckdb
from src import ValidationEngine
#from src import TestCasesLib
import pandas as pd
import pyodbc
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
import sqlalchemy as sa
    
# Global Variables
connections = {}
#testCases = {}

def main():
    # boot up
    connections = ValidationEngine.readConnections()
    ValidationEngine.readTestCases()
    ValidationEngine.executeTestCases(connections)
    
def test():
    #data = pandas.read_csv("data\\Electric_Vehicle_Population_Data.csv") 
    cnxn = pyodbc.connect("Driver={SQL Server};"
                      "Server=Deepak-Laptop;"
                      "Database=MemberPortal;"
                      "Trusted_Connection=yes;")
    
    connection_string = "DRIVER={SQL Server};SERVER=Deepak-Laptop;DATABASE=MemberPortal;Trusted_Connection=yes"
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})

    engine = create_engine(connection_url)

    with engine.begin() as conn:
        df = pd.read_sql_query(sa.text("SELECT top 10000 * FROM dbo.Electric_Vehicle_Population_Data"), conn)

    duckdb.sql("CREATE TABLE my_table AS SELECT * FROM df")

    response = duckdb.sql("SELECT count(*) from my_table").fetchall()

    duckdb.sql("DROP TABLE my_table")

    print (response)

main()

#print ("Hello")

#result = duckdb.sql("CREATE TABLE ontime AS SELECT * FROM 'flights.csv';